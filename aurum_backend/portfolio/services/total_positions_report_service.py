"""
Total Positions Report Service for AurumFinance.
Generates complete positions report including ALT investments.
Shows: custody, description, ticker, quantity, market_value only.
"""

import logging
from collections import defaultdict
from decimal import Decimal
from typing import Dict, List, Any
from django.db.models import Sum
from ..models import Client, PortfolioSnapshot, Position

logger = logging.getLogger(__name__)


class TotalPositionsReportService:
    """Service for generating total positions reports including ALT positions."""
    
    def __init__(self):
        self.logger = logger
    
    def generate_total_positions_report(self, client_code: str) -> Dict[str, Any]:
        """
        Generate total positions report including ALT positions.
        Shows: custody, description, ticker, quantity, market_value
        Excludes: coupon_rate, maturity_date, gain_loss, cost_basis
        
        Args:
            client_code: Client code to generate report for
            
        Returns:
            Dict with report data including ALT positions
        """
        try:
            # Get latest snapshot for client
            snapshot = self._get_latest_snapshot(client_code)
            if not snapshot:
                error_msg = f"No portfolio snapshots found for client {client_code}"
                self.logger.warning(error_msg)
                return self._get_empty_report(client_code, error_msg)
            
            # Get ALL positions (including ALTs) - this is the key difference
            positions = snapshot.positions.select_related('asset').all()
            
            if not positions.exists():
                error_msg = f"No positions found for client {client_code} in snapshot {snapshot.snapshot_date}"
                self.logger.warning(error_msg)
                return self._get_empty_report(client_code, error_msg)
            
            # Calculate metrics WITH ALTs included
            total_positions = positions.count()
            total_market_value = float(positions.aggregate(
                total=Sum('market_value')
            )['total'] or Decimal('0'))
            
            # Generate simplified positions table (no cost basis, gain/loss, etc.)
            positions_table = self._generate_total_positions_table(positions)
            
            # Calculate asset allocation WITH ALTs
            asset_allocation = self._calculate_total_asset_allocation(positions)
            
            # Calculate custody allocation WITH ALTs  
            custody_allocation = self._calculate_total_custody_allocation(positions)
            
            # Count ALT positions for information
            alt_positions_count = positions.filter(asset__bank='ALT').count()
            
            self.logger.info(f"Generated total positions report for {client_code}: "
                           f"{total_positions} positions (including {alt_positions_count} ALTs), "
                           f"${total_market_value:,.2f} total value")
            
            return {
                'client_code': client_code,
                'snapshot_date': str(snapshot.snapshot_date),
                'positions_table': positions_table,
                'asset_allocation': asset_allocation,
                'custody_allocation': custody_allocation,
                'total_positions': total_positions,
                'total_market_value': total_market_value,
                'alt_positions_count': alt_positions_count,
                'includes_alts': True,
                'report_type': 'total_positions'
            }
            
        except Exception as e:
            self.logger.error(f"Error generating total positions report for {client_code}: {str(e)}")
            return self._get_empty_report(client_code, error=str(e))
    
    def _get_latest_snapshot(self, client_code: str) -> PortfolioSnapshot:
        """Get the latest snapshot for a client."""
        try:
            client = Client.objects.get(code=client_code)
            return PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
        except Client.DoesNotExist:
            self.logger.error(f"Client {client_code} not found")
            return None
        except Exception as e:
            self.logger.error(f"Error getting latest snapshot for {client_code}: {str(e)}")
            return None
    
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
    
    def _get_empty_report(self, client_code: str, error: str = None) -> Dict[str, Any]:
        """Return empty report structure with proper error handling."""
        error_message = error or "No positions data available"
        
        return {
            'client_code': client_code,
            'snapshot_date': None,
            'positions_table': f'<div class="no-data-message"><p>{error_message}</p></div>',
            'asset_allocation': {},
            'custody_allocation': {},
            'total_positions': 0,
            'total_market_value': 0.0,
            'alt_positions_count': 0,
            'includes_alts': True,
            'report_type': 'total_positions',
            'error': error_message
        }