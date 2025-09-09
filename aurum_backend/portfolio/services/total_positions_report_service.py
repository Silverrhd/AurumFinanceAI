"""
Total Positions Report Service for AurumFinance.
Generates complete positions report including ALT investments.
Shows: custody, description, ticker, quantity, market_value only.
"""

import logging
from collections import defaultdict
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Any
from django.db.models import Sum
from ..models import Client, PortfolioSnapshot, Position
from ..utils.report_utils import save_report_html
from .enhanced_report_service import EnhancedReportService

logger = logging.getLogger(__name__)


class TotalPositionsReportService(EnhancedReportService):
    """Service for generating Total Positions reports including ALT positions."""
    
    def __init__(self):
        super().__init__()
    
    def generate_total_positions_report(self, client_code: str) -> str:
        """
        Generate Total Positions Report HTML including ALT positions.
        Shows: custody, description, ticker, quantity, market_value only.
        Excludes: coupon_rate, maturity_date, gain_loss, cost_basis
        
        Args:
            client_code: Client code to generate report for
            
        Returns:
            str: HTML content of the generated report
        """
        logger.info(f"Generating Total Positions report for {client_code}")
        
        try:
            # Get client and latest snapshot
            client = Client.objects.get(code=client_code)
            snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
            
            if not snapshot:
                logger.warning(f"No portfolio snapshots found for client {client_code}")
                return self._generate_empty_report(client_code, "No portfolio data found")
            
            # Get ALL positions (including ALTs) - this is the key difference
            positions = snapshot.positions.select_related('asset').all()
            
            if not positions.exists():
                logger.warning(f"No positions found for client {client_code} in snapshot {snapshot.snapshot_date}")
                return self._generate_empty_report(client_code, f"No positions found for snapshot {snapshot.snapshot_date}")
            
            # Calculate metrics WITH ALTs included
            total_positions = positions.count()
            total_market_value = float(positions.aggregate(
                total=Sum('market_value')
            )['total'] or Decimal('0'))
            
            # Count ALT positions for information
            alt_positions_count = positions.filter(asset__bank='ALT').count()
            
            # Generate simplified positions table (no cost basis, gain/loss, etc.)
            positions_table = self._generate_total_positions_table(positions)
            
            # Calculate asset allocation WITH ALTs
            asset_allocation = self._calculate_total_asset_allocation(positions)
            
            # Calculate custody allocation WITH ALTs  
            custody_allocation = self._calculate_total_custody_allocation(positions)
            
            # Prepare template context
            template_context = {
                'client_code': client_code,
                'client_name': client.name,
                'snapshot_date': snapshot.snapshot_date,
                'positions_table': positions_table,
                'asset_allocation': asset_allocation,
                'custody_allocation': custody_allocation,
                'total_positions': total_positions,
                'total_market_value': total_market_value,
                'alt_positions_count': alt_positions_count,
                'includes_alts': True,
                'report_type': 'total_positions',
                'report_generated': datetime.now()
            }
            
            # Render template using inherited jinja_env
            template = self.jinja_env.get_template('total_positions_template.html')
            html_content = template.render(template_context)
            
            # Save report to file
            report_date = snapshot.snapshot_date.strftime('%Y-%m-%d')
            file_path, file_size = save_report_html(
                client_code, 
                'total_positions', 
                html_content,
                report_date
            )
            
            logger.info(f"Successfully generated Total Positions report for {client_code}: "
                       f"{total_positions} positions (including {alt_positions_count} ALTs), "
                       f"${total_market_value:,.2f} total value")
            
            return html_content
            
        except Client.DoesNotExist:
            error_msg = f"Client {client_code} not found"
            logger.error(error_msg)
            return self._generate_empty_report(client_code, error_msg)
        except Exception as e:
            error_msg = f"Error generating Total Positions report for {client_code}: {str(e)}"
            logger.error(error_msg)
            return self._generate_empty_report(client_code, error_msg)
    
    def _generate_total_positions_table(self, positions) -> str:
        """
        Generate HTML table for total positions (simplified version).
        Only shows: custody, description, ticker, quantity, market_value
        """
        # Group positions by asset type for organization
        grouped_positions = defaultdict(list)
        
        for position in positions:
            asset_type = position.asset.asset_type or 'Other'
            grouped_positions[asset_type].append(position)
        
        # Start building HTML table
        html = '''
        <div class="positions-table-container">
            <table class="positions-table">
                <thead>
                    <tr>
                        <th>Custody</th>
                        <th>Description</th>
                        <th>Ticker</th>
                        <th>Quantity</th>
                        <th>Market Value</th>
                    </tr>
                </thead>
                <tbody>
        '''
        
        # Add positions grouped by asset type
        for asset_type, positions_list in sorted(grouped_positions.items()):
            # Add asset type header row
            html += f'''
                    <tr class="asset-type-header">
                        <td colspan="5"><strong>{asset_type}</strong></td>
                    </tr>
            '''
            
            # Add positions for this asset type
            for position in sorted(positions_list, key=lambda p: p.asset.name):
                custody = f"{position.bank}"
                if position.account and position.account != position.bank:
                    custody += f" - {position.account}"
                
                html += f'''
                    <tr>
                        <td>{custody}</td>
                        <td>{position.asset.name or 'N/A'}</td>
                        <td>{position.asset.ticker or 'N/A'}</td>
                        <td>{position.quantity:,.2f}</td>
                        <td>${position.market_value:,.2f}</td>
                    </tr>
                '''
        
        html += '''
                </tbody>
            </table>
        </div>
        '''
        
        return html
    
    def _calculate_total_asset_allocation(self, positions) -> Dict[str, Any]:
        """Calculate asset allocation INCLUDING ALTs."""
        allocation = defaultdict(float)
        
        for position in positions:
            asset_type = position.asset.asset_type or 'Other'
            allocation[asset_type] += float(position.market_value)
        
        total_value = sum(allocation.values())
        
        # Convert to percentage and format for charts
        result = {}
        for asset_type, value in allocation.items():
            percentage = (value / total_value * 100) if total_value > 0 else 0
            result[asset_type] = {
                'market_value': value,
                'percentage': percentage
            }
        
        return result
    
    def _calculate_total_custody_allocation(self, positions) -> Dict[str, Any]:
        """Calculate custody allocation INCLUDING ALTs."""
        allocation = defaultdict(float)
        
        for position in positions:
            custody_key = position.bank
            allocation[custody_key] += float(position.market_value)
        
        total_value = sum(allocation.values())
        
        # Convert to percentage and format for charts
        result = {}
        for custody, value in allocation.items():
            percentage = (value / total_value * 100) if total_value > 0 else 0
            result[custody] = {
                'market_value': value,
                'percentage': percentage
            }
        
        return result
    
    def _generate_empty_report(self, client_code: str, error_message: str) -> str:
        """Generate empty report with error message."""
        template_context = {
            'client_code': client_code,
            'client_name': 'Unknown',
            'snapshot_date': None,
            'positions_table': f'<div class="no-data-message"><p>{error_message}</p></div>',
            'asset_allocation': {},
            'custody_allocation': {},
            'total_positions': 0,
            'total_market_value': 0.0,
            'alt_positions_count': 0,
            'includes_alts': True,
            'report_type': 'total_positions',
            'error': error_message,
            'report_generated': datetime.now()
        }
        
        # Render template using inherited jinja_env
        template = self.jinja_env.get_template('total_positions_template.html')
        return template.render(template_context)