"""
Equity Breakdown Report Service
Generates comprehensive equity sector analysis reports
"""

import logging
from datetime import datetime
from typing import Tuple
from django.db.models import Count
from portfolio.models import Client, PortfolioSnapshot
from portfolio.utils.report_utils import save_report_html
from .enhanced_report_service import EnhancedReportService
from .equity_analysis_service import EquityAnalysisService

logger = logging.getLogger(__name__)


class EquityBreakdownReportService(EnhancedReportService):
    """Service for generating Equity Breakdown reports."""

    def __init__(self):
        super().__init__()
        self.equity_analysis_service = EquityAnalysisService()

    def generate_equity_breakdown_report(self, client_code: str) -> Tuple[str, str]:
        """
        Generate Equity Breakdown Report using latest snapshot.

        Args:
            client_code: Client code to generate report for

        Returns:
            Tuple of (html_content, snapshot_date)
        """
        logger.info(f"Generating Equity Breakdown report for {client_code}")

        try:
            # 1. Get client and latest snapshot
            client = Client.objects.get(code=client_code)
            snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()

            if not snapshot:
                logger.warning(f"No portfolio snapshots found for client {client_code}")
                return self._generate_empty_report(client_code, "No portfolio data found"), None

            # 2. Filter equity positions (exclude ALTs)
            equity_positions = snapshot.positions.select_related('asset').filter(
                asset__asset_type='Equities'
            ).exclude(asset__bank='ALT')

            if not equity_positions.exists():
                logger.warning(f"No equity positions found for client {client_code}")
                return self._generate_empty_report(client_code, f"No equity positions found for snapshot {snapshot.snapshot_date}"), snapshot.snapshot_date

            # 3. Analyze equities
            logger.info(f"Analyzing {equity_positions.count()} equity positions")
            analysis_results = self.equity_analysis_service.analyze_equity_portfolio(
                equity_positions
            )

            # 4. Prepare template context
            context = {
                'client_name': client.name,
                'client_code': client_code,
                'report_date': snapshot.snapshot_date.strftime('%Y-%m-%d'),
                'total_equity_value': analysis_results['total_equity_value'],
                'aggregated_holdings': analysis_results['aggregated_holdings'],  # NEW: Look-through view
                'direct_holdings': analysis_results['direct_holdings'],  # Kept for backward compat
                'etf_holdings': analysis_results['etf_holdings'],
                'aggregated_holdings_count': len(analysis_results['aggregated_holdings']),
                'direct_holdings_count': len(analysis_results['direct_holdings']),
                'etf_holdings_count': len(analysis_results['etf_holdings']),
                'sector_breakdown': analysis_results['sector_comparison'],
                'spy_benchmark': analysis_results['spy_benchmark'],
                'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # 5. Render template
            template = self.jinja_env.get_template('equity_breakdown_template.html')
            html_content = template.render(context)

            # 6. Save report to file
            report_date = snapshot.snapshot_date.strftime('%Y-%m-%d')
            file_path, file_size = save_report_html(
                client_code,
                'equity_breakdown',
                report_date,
                html_content
            )

            logger.info(f"Successfully generated Equity Breakdown report for {client_code}: "
                       f"{len(analysis_results['direct_holdings'])} direct holdings, "
                       f"{len(analysis_results['etf_holdings'])} ETFs, "
                       f"${analysis_results['total_equity_value']:,.2f} total value")

            return html_content, snapshot.snapshot_date

        except Client.DoesNotExist:
            error_msg = f"Client {client_code} not found"
            logger.error(error_msg)
            return self._generate_empty_report(client_code, error_msg), None
        except Exception as e:
            error_msg = f"Error generating Equity Breakdown report for {client_code}: {str(e)}"
            logger.error(error_msg)
            import traceback
            logger.error(traceback.format_exc())
            return self._generate_empty_report(client_code, error_msg), None

    def _generate_empty_report(self, client_code: str, error_message: str) -> str:
        """Generate empty report with error message."""
        context = {
            'client_code': client_code,
            'client_name': 'Unknown',
            'report_date': None,
            'total_equity_value': 0.0,
            'aggregated_holdings': [],
            'direct_holdings': [],
            'etf_holdings': [],
            'aggregated_holdings_count': 0,
            'direct_holdings_count': 0,
            'etf_holdings_count': 0,
            'sector_breakdown': {},
            'spy_benchmark': {},
            'error': error_message,
            'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # Render template using inherited jinja_env
        template = self.jinja_env.get_template('equity_breakdown_template.html')
        return template.render(context)
