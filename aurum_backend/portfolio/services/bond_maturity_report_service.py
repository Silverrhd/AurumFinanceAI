"""
Bond Maturity Report Service
Generates individual and consolidated bond maturity reports sorted by maturity date
"""

import logging
from decimal import Decimal
from datetime import datetime, date
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from django.db.models import Q, Sum
from portfolio.models import Client, PortfolioSnapshot, Position, Report
from .enhanced_report_service import EnhancedReportService
from ..utils.report_utils import save_report_html

logger = logging.getLogger(__name__)

class BondMaturityReportService(EnhancedReportService):
    """Service for generating Bond Maturity reports."""
    
    def __init__(self):
        super().__init__()
        self.imminent_threshold = 30  # 30 days for imminent maturity alerts
    
    def generate_bond_maturity_report(self, client_code: str, report_type: str = 'individual') -> str:
        """
        Generate Bond Maturity Report (individual or consolidated).
        Uses latest available snapshot data.
        """
        logger.info(f"Generating {report_type} Bond Maturity report for {client_code}")
        
        try:
            if report_type == 'consolidated' or client_code == 'ALL':
                return self._generate_consolidated_report()
            else:
                return self._generate_individual_report(client_code)
                
        except Exception as e:
            logger.error(f"Error generating bond maturity report: {str(e)}")
            raise
    
    def _generate_individual_report(self, client_code: str) -> str:
        """Generate individual client bond maturity report."""
        
        # Get client and latest snapshot
        client = Client.objects.get(code=client_code)
        snapshot = PortfolioSnapshot.objects.filter(
            client=client
        ).order_by('-snapshot_date').first()
        
        if not snapshot:
            raise ValueError(f"No portfolio data found for {client_code}")
        
        # CRITICAL: Filter ONLY actual bonds (Fixed Income with maturity dates) 
        # This excludes ETFs and mutual funds which don't have maturity dates
        # Also exclude ALT positions for presentation (same logic as bond_issuer_weight)
        bond_positions = snapshot.positions.select_related('asset').filter(
            asset__asset_type='Fixed Income',
            asset__maturity_date__isnull=False
        ).exclude(asset__bank='ALT')
        
        if not bond_positions.exists():
            logger.warning(f"No bond positions found for {client_code} (Fixed Income with maturity dates)")
            return self._render_empty_individual_report(client, snapshot)
        
        # Get sorted bonds data
        bonds_data = self._get_sorted_bonds_data(bond_positions, include_client=False)
        
        # Calculate maturity risk analysis
        risk_analysis = self._calculate_maturity_risk_analysis(bonds_data)
        
        # Generate report HTML
        template_context = {
            'client_name': client.name,
            'client_code': client.code,
            'snapshot_date': snapshot.snapshot_date,
            'bonds': bonds_data,
            'total_bonds': len(bonds_data),
            'total_market_value': sum(bond['market_value'] for bond in bonds_data),
            'risk_analysis': risk_analysis,
            'report_generated': datetime.now(),
            'logo_base64': self._get_logo_base64()
        }
        
        template = self.jinja_env.get_template('bond_maturity_individual.html')
        html_content = template.render(template_context)
        
        logger.info(f"Individual Bond Maturity report generated for {client_code}: {len(bonds_data)} bonds")
        return html_content
    
    def _generate_consolidated_report(self) -> str:
        """Generate consolidated bond maturity report for all clients."""
        
        # Get all clients with bond positions
        all_bonds = []
        clients_processed = 0
        latest_snapshot_date = None
        
        all_clients = Client.objects.exclude(code='ALL').order_by('code')
        
        for client in all_clients:
            snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
            
            if not snapshot:
                continue
            
            # Track the latest snapshot date across all clients
            if latest_snapshot_date is None or snapshot.snapshot_date > latest_snapshot_date:
                latest_snapshot_date = snapshot.snapshot_date
            
            # Get bond positions for this client
            bond_positions = snapshot.positions.select_related('asset').filter(
                asset__asset_type='Fixed Income',
                asset__maturity_date__isnull=False
            ).exclude(asset__bank='ALT')
            
            if not bond_positions.exists():
                continue
            
            # Get bonds data for this client (include client info)
            client_bonds = self._get_sorted_bonds_data(bond_positions, include_client=True, client=client)
            all_bonds.extend(client_bonds)
            clients_processed += 1
        
        if not all_bonds:
            logger.warning("No bond positions found across all clients")
            return self._render_empty_consolidated_report()
        
        # Sort ALL bonds by maturity date globally (closest first)
        all_bonds.sort(key=lambda x: x['maturity_date_obj'])
        
        # Calculate consolidated risk analysis
        risk_analysis = self._calculate_maturity_risk_analysis(all_bonds)
        
        # Generate consolidated report HTML
        template_context = {
            'all_bonds': all_bonds,
            'clients_processed': clients_processed,
            'total_bonds': len(all_bonds),
            'total_market_value': sum(bond['market_value'] for bond in all_bonds),
            'risk_analysis': risk_analysis,
            'report_generated': datetime.now(),
            'snapshot_date': latest_snapshot_date,  # Use actual latest snapshot date
            'logo_base64': self._get_logo_base64()
        }
        
        template = self.jinja_env.get_template('bond_maturity_consolidated.html')
        html_content = template.render(template_context)
        
        logger.info(f"Consolidated Bond Maturity report generated: {len(all_bonds)} bonds from {clients_processed} clients")
        return html_content
    
    def _get_sorted_bonds_data(self, bond_positions, include_client=False, client=None) -> List[Dict]:
        """Extract and sort bonds data by maturity date."""
        bonds_data = []
        today = date.today()
        
        for position in bond_positions:
            asset = position.asset
            
            # Calculate days to maturity
            days_to_maturity = (asset.maturity_date - today).days if asset.maturity_date else 0
            
            bond_detail = {
                'custody': f"{asset.bank} {asset.account}".strip() or 'Unknown',
                'name': asset.name,
                'quantity': float(position.quantity),
                'market_value': float(position.market_value),
                'maturity_date': asset.maturity_date.strftime('%Y-%m-%d') if asset.maturity_date else 'N/A',
                'maturity_date_formatted': asset.maturity_date.strftime('%d/%m/%Y') if asset.maturity_date else 'N/A',
                'maturity_date_obj': asset.maturity_date,  # For sorting
                'days_to_maturity': days_to_maturity,
                'coupon_rate': f"{position.coupon_rate or asset.coupon_rate or 0}%" if (position.coupon_rate or asset.coupon_rate) else "0.00%",
                'cusip': asset.cusip or 'N/A',
                'is_imminent': days_to_maturity <= self.imminent_threshold
            }
            
            # Add client info for consolidated report
            if include_client and client:
                bond_detail['client'] = client.code
                bond_detail['client_code'] = client.code
            
            bonds_data.append(bond_detail)
        
        # Sort by maturity date (closest first)
        bonds_data.sort(key=lambda x: x['maturity_date_obj'] if x['maturity_date_obj'] else date.max)
        
        return bonds_data
    
    def _calculate_maturity_risk_analysis(self, bonds_data: List[Dict]) -> Dict:
        """Calculate maturity concentration risk analysis."""
        if not bonds_data:
            return {
                'imminent_bonds': [],
                'imminent_count': 0,
                'imminent_value': 0.0,
                'maturity_periods': {
                    '1_year': {'count': 0, 'value': 0.0},      # 0-1 year
                    '2_year': {'count': 0, 'value': 0.0},      # 1-2 years  
                    '3_year': {'count': 0, 'value': 0.0},      # 2-3 years
                    '4_year': {'count': 0, 'value': 0.0},      # 3-4 years
                    '5_year': {'count': 0, 'value': 0.0},      # 4-5 years
                    '6_plus_year': {'count': 0, 'value': 0.0}  # 5+ years
                },
                'total_value': 0.0
            }
        
        # Categorize bonds by year-based maturity periods
        maturity_periods = {
            '1_year': {'bonds': [], 'count': 0, 'value': 0.0, 'face_value': 0.0},
            '2_year': {'bonds': [], 'count': 0, 'value': 0.0, 'face_value': 0.0},
            '3_year': {'bonds': [], 'count': 0, 'value': 0.0, 'face_value': 0.0},
            '4_year': {'bonds': [], 'count': 0, 'value': 0.0, 'face_value': 0.0},
            '5_year': {'bonds': [], 'count': 0, 'value': 0.0, 'face_value': 0.0},
            '6_plus_year': {'bonds': [], 'count': 0, 'value': 0.0, 'face_value': 0.0}
        }
        
        imminent_bonds = []
        total_value = sum(bond['market_value'] for bond in bonds_data)
        
        for bond in bonds_data:
            days = bond['days_to_maturity']
            value = bond['market_value']
            
            if days <= 365:  # 1 year
                period = '1_year'
                if days <= 30:  # Keep imminent logic for highlighting
                    imminent_bonds.append(bond)
            elif days <= 730:  # 2 years
                period = '2_year'
            elif days <= 1095:  # 3 years
                period = '3_year'
            elif days <= 1460:  # 4 years
                period = '4_year'
            elif days <= 1825:  # 5 years
                period = '5_year'
            else:  # 6+ years
                period = '6_plus_year'
            
            maturity_periods[period]['bonds'].append(bond)
            maturity_periods[period]['count'] += 1
            maturity_periods[period]['value'] += value
            maturity_periods[period]['face_value'] += bond['quantity']
        
        # Remove bonds list to reduce template size, keep just counts/values
        for period in maturity_periods:
            del maturity_periods[period]['bonds']
        
        return {
            'imminent_bonds': imminent_bonds,
            'imminent_count': len(imminent_bonds),
            'imminent_value': sum(bond['market_value'] for bond in imminent_bonds),
            'maturity_periods': maturity_periods,
            'total_value': total_value
        }
    
    def _render_empty_individual_report(self, client: Client, snapshot: PortfolioSnapshot) -> str:
        """Render empty individual report when no bond positions found."""
        template_context = {
            'client_name': client.name,
            'client_code': client.code,
            'snapshot_date': snapshot.snapshot_date,
            'bonds': [],
            'total_bonds': 0,
            'total_market_value': 0.0,
            'risk_analysis': {
                'imminent_bonds': [],
                'imminent_count': 0,
                'imminent_value': 0.0,
                'maturity_periods': {
                    '1_year': {'count': 0, 'value': 0.0},
                    '2_year': {'count': 0, 'value': 0.0},
                    '3_year': {'count': 0, 'value': 0.0},
                    '4_year': {'count': 0, 'value': 0.0},
                    '5_year': {'count': 0, 'value': 0.0},
                    '6_plus_year': {'count': 0, 'value': 0.0}
                },
                'total_value': 0.0
            },
            'report_generated': datetime.now(),
            'logo_base64': self._get_logo_base64(),
            'empty_report': True
        }
        
        template = self.jinja_env.get_template('bond_maturity_individual.html')
        return template.render(template_context)
    
    def _render_empty_consolidated_report(self) -> str:
        """Render empty consolidated report when no bond positions found."""
        template_context = {
            'all_bonds': [],
            'clients_processed': 0,
            'total_bonds': 0,
            'total_market_value': 0.0,
            'risk_analysis': {
                'imminent_bonds': [],
                'imminent_count': 0,
                'imminent_value': 0.0,
                'maturity_periods': {
                    '1_year': {'count': 0, 'value': 0.0},
                    '2_year': {'count': 0, 'value': 0.0},
                    '3_year': {'count': 0, 'value': 0.0},
                    '4_year': {'count': 0, 'value': 0.0},
                    '5_year': {'count': 0, 'value': 0.0},
                    '6_plus_year': {'count': 0, 'value': 0.0}
                },
                'total_value': 0.0
            },
            'report_generated': datetime.now(),
            'snapshot_date': datetime.now().date(),
            'logo_base64': self._get_logo_base64(),
            'empty_report': True
        }
        
        template = self.jinja_env.get_template('bond_maturity_consolidated.html')
        return template.render(template_context)
    
    def _get_logo_base64(self) -> str:
        """Get base64 encoded logo for reports."""
        try:
            import base64
            import os
            
            # Try to find logo file
            logo_paths = [
                os.path.join(os.path.dirname(__file__), '..', '..', 'static', 'images', 'logo.png'),
                os.path.join(os.path.dirname(__file__), '..', '..', 'static', 'logo.png'),
                os.path.join(os.path.dirname(__file__), '..', '..', 'templates', 'logo.png'),
            ]
            
            for logo_path in logo_paths:
                if os.path.exists(logo_path):
                    with open(logo_path, 'rb') as logo_file:
                        return base64.b64encode(logo_file.read()).decode('utf-8')
            
            # No logo found
            return ''
            
        except Exception as e:
            logger.warning(f"Could not load logo: {e}")
            return ''