"""
Cash Position Report Service
Generates individual and consolidated cash position reports with 5% AUM concentration alerts
"""

import logging
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from django.db.models import Q, Sum
from portfolio.models import Client, PortfolioSnapshot, Position, Report
from .enhanced_report_service import EnhancedReportService
from ..utils.report_utils import save_report_html

logger = logging.getLogger(__name__)

class CashReportService(EnhancedReportService):
    """Service for generating Cash Position reports."""
    
    def __init__(self):
        super().__init__()
        self.concentration_threshold = 5.0  # 5% cash concentration alert threshold
    
    def generate_cash_position_report(self, client_code: str, report_type: str = 'individual') -> str:
        """
        Generate Cash Position Report (individual or consolidated).
        Uses latest available snapshot data.
        """
        logger.info(f"Generating {report_type} Cash Position report for {client_code}")
        
        try:
            if report_type == 'consolidated' or client_code == 'ALL':
                return self._generate_consolidated_report()
            else:
                return self._generate_individual_report(client_code)
                
        except Exception as e:
            logger.error(f"Error generating cash report: {str(e)}")
            raise
    
    def _generate_individual_report(self, client_code: str) -> str:
        """Generate individual client cash position report."""
        
        # Get client and latest snapshot
        client = Client.objects.get(code=client_code)
        snapshot = PortfolioSnapshot.objects.filter(
            client=client
        ).order_by('-snapshot_date').first()
        
        if not snapshot:
            raise ValueError(f"No portfolio data found for {client_code}")
        
        # Get cash positions - asset_type in ['Cash', 'Money Market']
        cash_positions = snapshot.positions.select_related('asset').filter(
            asset__asset_type__in=['Cash', 'Money Market'],
            market_value__gt=0
        ).order_by('asset__name')
        
        # Calculate custody format: f"{bank} {account}".strip()
        positions_data = []
        for position in cash_positions:
            custody = f"{position.bank} {position.account}".strip()
            positions_data.append({
                'custody': custody,
                'asset_name': position.asset.name,
                'market_value': float(position.market_value),
                'ticker': position.asset.ticker
            })
        
        # Calculate total cash and AUM for concentration check
        total_cash = sum(pos['market_value'] for pos in positions_data)
        total_aum = float(self._calculate_total_aum(snapshot))
        
        # Check concentration alert (5% threshold)
        concentration_alert = None
        if total_aum > 0:
            cash_percentage = (total_cash / total_aum) * 100
            if cash_percentage > self.concentration_threshold:
                concentration_alert = {
                    'percentage': round(cash_percentage, 2),
                    'threshold': self.concentration_threshold,
                    'total_cash': float(total_cash),
                    'total_aum': float(total_aum)
                }
        
        # Generate report HTML
        template_context = {
            'client_name': client.name,
            'client_code': client.code,
            'snapshot_date': snapshot.snapshot_date,
            'cash_positions': positions_data,
            'total_cash': total_cash,
            'concentration_alert': concentration_alert,
            'report_generated': datetime.now()
        }
        
        template = self.jinja_env.get_template('cash_position_individual.html')
        html_content = template.render(template_context)
        
        # Save report to file
        report_date = snapshot.snapshot_date.strftime('%Y-%m-%d') if snapshot.snapshot_date else datetime.now().strftime('%Y-%m-%d')
        file_path, file_size = save_report_html(
            client_code, 
            'cash_position_reports', 
            report_date, 
            html_content
        )
        
        return html_content
    
    def _generate_consolidated_report(self) -> str:
        """Generate consolidated cash position report for all clients."""
        
        # Get all clients with cash positions
        clients_with_cash = []
        all_clients = Client.objects.all().order_by('code')
        
        for client in all_clients:
            snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
            
            if not snapshot:
                continue
            
            # Get cash positions for this client
            cash_positions = snapshot.positions.select_related('asset').filter(
                asset__asset_type__in=['Cash', 'Money Market'],
                market_value__gt=0
            )
            
            if not cash_positions.exists():
                continue
            
            # Calculate client cash data (excluding ALT)
            total_cash = float(cash_positions.exclude(asset__bank='ALT').aggregate(
                total=Sum('market_value')
            )['total'] or Decimal('0'))
            
            total_aum = float(self._calculate_total_aum(snapshot))
            
            # Check concentration alert
            concentration_alert = None
            if total_aum > 0:
                cash_percentage = (total_cash / total_aum) * 100
                if cash_percentage > self.concentration_threshold:
                    concentration_alert = {
                        'percentage': round(cash_percentage, 2),
                        'threshold': self.concentration_threshold
                    }
            
            # Prepare detailed positions for expandable rows
            detailed_positions = []
            for position in cash_positions:
                custody = f"{position.bank} {position.account}".strip()
                detailed_positions.append({
                    'custody': custody,
                    'asset_name': position.asset.name,
                    'market_value': float(position.market_value),
                    'ticker': position.asset.ticker
                })
            
            clients_with_cash.append({
                'client_code': client.code,
                'client_name': client.name,
                'snapshot_date': snapshot.snapshot_date,
                'total_cash': total_cash,
                'total_aum': total_aum,
                'concentration_alert': concentration_alert,
                'detailed_positions': detailed_positions
            })
        
        # Generate consolidated report HTML
        template_context = {
            'clients_with_cash': clients_with_cash,
            'report_generated': datetime.now(),
            'total_clients': len(clients_with_cash)
        }
        
        template = self.jinja_env.get_template('cash_position_consolidated.html')
        html_content = template.render(template_context)
        
        # Save report to file - use 'ALL' as client code for consolidated reports
        file_path, file_size = save_report_html(
            'ALL', 
            'cash_position_reports', 
            datetime.now().strftime('%Y-%m-%d'), 
            html_content
        )
        
        return html_content
    
    def _calculate_total_aum(self, snapshot: PortfolioSnapshot) -> Decimal:
        """Calculate total NON-ALT AUM for a snapshot."""
        total = snapshot.positions.exclude(asset__bank='ALT').aggregate(
            total=Sum('market_value')
        )['total']
        return total or Decimal('0')
    
    def get_available_cash_report_dates(self, client_code: str) -> List[str]:
        """Get available dates for cash report generation."""
        client = Client.objects.get(code=client_code)
        dates = PortfolioSnapshot.objects.filter(
            client=client,
            positions__asset__asset_type__in=['Cash', 'Money Market'],
            positions__market_value__gt=0
        ).distinct().order_by('-snapshot_date').values_list('snapshot_date', flat=True)
        
        return [date.strftime('%Y-%m-%d') for date in dates]
    
    def get_generated_cash_reports(self, client_code: str = None) -> List[Dict]:
        """Get list of generated cash reports."""
        reports_query = Report.objects.filter(
            report_type='CASH_POSITION'
        ).select_related('client').order_by('client__code')
        
        if client_code and client_code != 'ALL':
            reports_query = reports_query.filter(client__code=client_code)
        
        reports_data = []
        for report in reports_query:
            reports_data.append({
                'id': report.id,
                'client_code': report.client.code,
                'client_name': report.client.name,
                'report_date': report.report_date.strftime('%Y-%m-%d'),
                'file_path': report.file_path,
                'file_size': report.file_size or 0,
                'generation_time': report.generation_time or 0,
                'created_at': report.created_at.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return reports_data