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
            tuple: (HTML content, snapshot_date) of the generated report
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
            
            # Generate positions table organized by asset class (like weekly report)
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
                report_date,
                html_content
            )
            
            logger.info(f"Successfully generated Total Positions report for {client_code}: "
                       f"{total_positions} positions (including {alt_positions_count} ALTs), "
                       f"${total_market_value:,.2f} total value")
            
            return html_content, snapshot.snapshot_date
            
        except Client.DoesNotExist:
            error_msg = f"Client {client_code} not found"
            logger.error(error_msg)
            return self._generate_empty_report(client_code, error_msg), None
        except Exception as e:
            error_msg = f"Error generating Total Positions report for {client_code}: {str(e)}"
            logger.error(error_msg)
            return self._generate_empty_report(client_code, error_msg), None
    
    def _generate_total_positions_table(self, positions) -> str:
        """
        Generate HTML tables for total positions organized by asset class.
        Follows weekly report structure with separate tables and subtotals.
        """
        # Consolidate positions by asset class
        consolidated_positions = self._consolidate_asset_classes(positions)
        
        html_sections = []
        
        # Define section order (same as weekly report)
        section_order = [
            'Cash/Money Market',
            'Fixed Income', 
            'Equities',
            'Alternatives'
        ]
        
        # Generate each asset class section
        for asset_class in section_order:
            positions_list = consolidated_positions.get(asset_class, [])
            if positions_list:  # Only include if there are positions
                section_html = self._generate_asset_class_section(asset_class, positions_list)
                html_sections.append(section_html)
        
        return ''.join(html_sections)
    
    def _consolidate_asset_classes(self, positions) -> dict:
        """
        Consolidate positions by asset class following weekly report structure.
        Maps to: Cash/Money Market, Fixed Income, Equities, Alternatives
        """
        consolidated = defaultdict(list)
        
        for position in positions:
            # Prepare simplified position data (only 5 columns for total positions report)
            custody = f"{position.account} {position.bank}" if position.account and position.bank else "Unknown"
            
            position_data = {
                'custody': custody,
                'name': position.asset.name or 'N/A',
                'ticker': position.asset.ticker or 'N/A',
                'quantity': float(position.quantity),
                'market_value': float(position.market_value)
            }
            
            # Map asset types to consolidated categories (same logic as weekly report)
            asset_type = position.asset.asset_type
            if asset_type in ['Fixed Income', 'Bond', 'Treasury', 'Corporate Bond']:
                consolidated['Fixed Income'].append(position_data)
            elif asset_type in ['Equities', 'Equity', 'Stock', 'Common Stock']:
                consolidated['Equities'].append(position_data)
            elif asset_type in ['Cash', 'Money Market']:
                consolidated['Cash/Money Market'].append(position_data)
            else:
                # This includes ALTs and any other types
                consolidated['Alternatives'].append(position_data)
        
        # Sort each group by market_value descending
        for asset_class in consolidated:
            consolidated[asset_class].sort(key=lambda x: x['market_value'], reverse=True)
            
        return consolidated
    
    def _generate_asset_class_section(self, asset_class: str, positions_list: list) -> str:
        """
        Generate HTML section for a single asset class with subtotal.
        """
        if not positions_list:
            return ''
            
        # Calculate subtotal
        subtotal_market_value = sum(p['market_value'] for p in positions_list)
        
        html_parts = []
        html_parts.append(f'<h3>{asset_class}</h3>')
        html_parts.append('<div class="position-table-container">')
        html_parts.append('<table class="position-table">')
        html_parts.append('''
            <thead>
                <tr>
                    <th class="col-custody">Custody</th>
                    <th class="col-name">Description</th>
                    <th class="col-ticker">Ticker</th>
                    <th class="col-quantity">Quantity</th>
                    <th class="col-market-value">Market Value</th>
                </tr>
            </thead>
            <tbody>
        ''')
        
        # Add individual positions
        for position in positions_list:
            html_parts.append(f'''
                <tr>
                    <td class="col-custody">{position['custody']}</td>
                    <td class="col-name">{position['name']}</td>
                    <td class="col-ticker">{position['ticker']}</td>
                    <td class="numeric">{position['quantity']:,.2f}</td>
                    <td class="numeric">${position['market_value']:,.2f}</td>
                </tr>
            ''')
        
        # Add subtotal row
        html_parts.append(f'''
            <tr class="subtotal-row">
                <td class="col-custody"></td>
                <td class="col-name">Subtotal</td>
                <td class="col-ticker"></td>
                <td class="numeric">-</td>
                <td class="numeric">${subtotal_market_value:,.2f}</td>
            </tr>
        ''')
        
        html_parts.append('</tbody></table></div>')
        return ''.join(html_parts)
    
    def _calculate_total_asset_allocation(self, positions) -> Dict[str, Any]:
        """Calculate asset allocation INCLUDING ALTs with consolidated categories."""
        allocation = defaultdict(float)
        
        for position in positions:
            # Map asset types to consolidated categories (same logic as table)
            asset_type = position.asset.asset_type
            if asset_type in ['Fixed Income', 'Bond', 'Treasury', 'Corporate Bond']:
                allocation['Fixed Income'] += float(position.market_value)
            elif asset_type in ['Equities', 'Equity', 'Stock', 'Common Stock']:
                allocation['Equities'] += float(position.market_value)
            elif asset_type in ['Cash', 'Money Market']:
                allocation['Cash/Money Market'] += float(position.market_value)
            else:
                # This includes ALTs and any other types
                allocation['Alternatives'] += float(position.market_value)
        
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