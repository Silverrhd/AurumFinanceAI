"""
Django Report Generation Service
Generates ProjectAurum-compatible reports using pure Django models.
"""

from jinja2 import Environment, FileSystemLoader
from django.conf import settings
from ..models import Client, PortfolioSnapshot, Position, Transaction
from .portfolio_calculation_service import PortfolioCalculationService
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class DjangoReportService:
    """Generate ProjectAurum-compatible reports from Django data."""
    
    def __init__(self):
        self.calculation_service = PortfolioCalculationService()
        self._setup_jinja2()
    
    def _setup_jinja2(self):
        """Setup Jinja2 environment with custom filters."""
        template_dir = os.path.join(settings.BASE_DIR, 'templates')
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir))
        
        # Register custom filters (reuse existing functions)
        from ..business_logic.generate_html_report import format_currency, format_percentage, format_number
        self.jinja_env.filters['format_currency'] = format_currency
        self.jinja_env.filters['format_percentage'] = format_percentage
        self.jinja_env.filters['format_number'] = format_number
    
    def generate_weekly_report(self, client_code: str, current_date: str, 
                             comparison_date: str = None) -> str:
        """
        Generate weekly report using Django models.
        
        Args:
            client_code: Client identifier
            current_date: Current snapshot date
            comparison_date: Comparison snapshot date (optional)
            
        Returns:
            HTML content for the report
        """
        logger.info(f"Generating report for {client_code}: {comparison_date} -> {current_date}")
        
        try:
            # Get client and snapshots
            client = Client.objects.get(code=client_code)
            current_snapshot = PortfolioSnapshot.objects.get(
                client=client, snapshot_date=current_date
            )
            
            # Get comparison snapshot (or use current if none specified)
            if comparison_date and comparison_date != current_date:
                try:
                    comparison_snapshot = PortfolioSnapshot.objects.get(
                        client=client, snapshot_date=comparison_date
                    )
                except PortfolioSnapshot.DoesNotExist:
                    logger.warning(f"Comparison snapshot not found for {comparison_date}, using current")
                    comparison_snapshot = current_snapshot
                    comparison_date = current_date
            else:
                comparison_snapshot = current_snapshot
                comparison_date = current_date
            
            # Get current metrics (should already be calculated)
            current_metrics = current_snapshot.portfolio_metrics
            comparison_metrics = comparison_snapshot.portfolio_metrics
            
            # Prepare template context in ProjectAurum format
            context = self._prepare_template_context(
                client, current_date, comparison_date,
                current_metrics, comparison_metrics
            )
            
            # Render template using Jinja2 (compatible with existing template)
            try:
                template = self.jinja_env.get_template('report_template.html')
                html_content = template.render(context)
            except Exception as e:
                logger.error(f"Template rendering error: {e}")
                raise ValueError(f"Failed to render report template: {str(e)}")
            
            logger.info(f"Report generated successfully for {client_code}")
            return html_content
            
        except Exception as e:
            logger.error(f"Error generating report for {client_code}: {e}")
            raise
    
    def _prepare_template_context(self, client: Client, current_date: str, 
                                comparison_date: str, current_metrics: dict, 
                                comparison_metrics: dict) -> dict:
        """Prepare template context in exact ProjectAurum format."""
        
        # Calculate period changes
        current_value = current_metrics.get('total_value', 0)
        comparison_value = comparison_metrics.get('total_value', 0)
        value_change = current_value - comparison_value
        
        # Get real gain/loss (Modified Dietz)
        real_gain_loss_dollar = current_metrics.get('real_gain_loss_dollar', 0)
        real_gain_loss_percent = current_metrics.get('real_gain_loss_percent', 0)
        
        # Get inception returns
        inception_gain_loss_dollar = current_metrics.get('inception_gain_loss_dollar', 0)
        inception_gain_loss_percent = current_metrics.get('inception_gain_loss_percent', 0)
        
        # Prepare context matching ProjectAurum template expectations
        context = {
            # Header information
            'client_name': client.name,
            'client_code': client.code,
            'date1': comparison_date,
            'date2': current_date,
            'report_title': f'Portfolio Report - {client.name}',
            'period_info': f'Period: {comparison_date} to {current_date}',
            
            # Main portfolio data (current period)
            'week2_data': {
                'total_value': current_value,
                'total_cost_basis': current_metrics.get('total_cost_basis', 0),
                'unrealized_gain_loss': current_metrics.get('unrealized_gain_loss', 0),
                'unrealized_gain_loss_pct': current_metrics.get('unrealized_gain_loss_pct', 0)
            },
            
            # Summary metrics
            'week2_summary': {
                'weekly_dollar_performance': real_gain_loss_dollar,
                'weekly_percent_performance': real_gain_loss_percent,
                'total_return_pct': real_gain_loss_percent,
                'inception_dollar_performance': inception_gain_loss_dollar,
                'inception_return_pct': inception_gain_loss_percent,
                'ytd_dollar_performance': inception_gain_loss_dollar,  # Placeholder
                'ytd_return_pct': inception_gain_loss_percent,  # Placeholder
                'estimated_annual_income': current_metrics.get('estimated_annual_income', 0),
                'annual_income_yield': self._calculate_yield_percentage(
                    current_metrics.get('estimated_annual_income', 0), current_value
                )
            },
            
            # Asset allocation
            'asset_allocation': current_metrics.get('asset_allocation', {}),
            
            # Custody allocation
            'custody_allocation': current_metrics.get('custody_allocation', {}),
            
            # Top movers
            'top_movers': current_metrics.get('top_movers', {'gainers': [], 'losers': []}),
            
            # Bond maturity timeline
            'bond_maturity': current_metrics.get('bond_maturity', {}),
            
            # Positions grouped by type
            'positions_by_type': current_metrics.get('positions_by_type', {}),
            
            # Recent transactions
            'recent_transactions': current_metrics.get('recent_transactions', []),
            
            # Weekly comparison data
            'weekly_comparison': {
                'previous_total_value': comparison_value,
                'current_total_value': current_value,
                'total_value_change': value_change,
                'real_gain_loss_dollar': real_gain_loss_dollar,
                'real_gain_loss_percent': real_gain_loss_percent,
                'cash_flow': current_metrics.get('net_cash_flow', 0),
                'income_change': (
                    current_metrics.get('estimated_annual_income', 0) - 
                    comparison_metrics.get('estimated_annual_income', 0)
                ),
                'position_count_change': (
                    current_metrics.get('position_count', 0) - 
                    comparison_metrics.get('position_count', 0)
                )
            },
            
            # Chart data
            'chart_data': current_metrics.get('chart_data', {}),
            
            # Additional template variables
            'is_first_report': comparison_date == current_date,
            'has_comparison': comparison_date != current_date,
            'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return context
    
    def _calculate_yield_percentage(self, annual_income: float, total_value: float) -> float:
        """Calculate yield percentage."""
        if total_value > 0:
            return (annual_income / total_value) * 100
        return 0.0
    
    def generate_report_for_client(self, client_code: str, snapshot_date: str = None) -> str:
        """
        Generate report for a client using the most recent snapshot.
        
        Args:
            client_code: Client identifier
            snapshot_date: Specific snapshot date (optional)
            
        Returns:
            HTML content for the report
        """
        if not snapshot_date:
            # Get most recent snapshot
            client = Client.objects.get(code=client_code)
            latest_snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
            
            if not latest_snapshot:
                raise ValueError(f"No snapshots found for client {client_code}")
            
            snapshot_date = latest_snapshot.snapshot_date
        
        # Get previous snapshot for comparison
        client = Client.objects.get(code=client_code)
        previous_snapshot = PortfolioSnapshot.objects.filter(
            client=client,
            snapshot_date__lt=snapshot_date
        ).order_by('-snapshot_date').first()
        
        comparison_date = previous_snapshot.snapshot_date if previous_snapshot else snapshot_date
        
        return self.generate_weekly_report(client_code, snapshot_date, comparison_date)
    
    def get_available_reports(self, client_code: str = None) -> list:
        """
        Get list of available reports.
        
        Args:
            client_code: Optional client filter
            
        Returns:
            List of available report dates and clients
        """
        queryset = PortfolioSnapshot.objects.all()
        
        if client_code:
            client = Client.objects.get(code=client_code)
            queryset = queryset.filter(client=client)
        
        snapshots = queryset.select_related('client').order_by('-snapshot_date')
        
        return [{
            'client_code': snapshot.client.code,
            'client_name': snapshot.client.name,
            'snapshot_date': snapshot.snapshot_date,
            'total_value': snapshot.portfolio_metrics.get('total_value', 0),
            'has_metrics': bool(snapshot.portfolio_metrics)
        } for snapshot in snapshots]