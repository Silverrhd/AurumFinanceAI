"""
Performance calculation service for AurumFinance.
Migrated from ProjectAurum calculations/performance.py with Django ORM integration.

Calculates comprehensive portfolio performance metrics including:
- Modified Dietz returns
- Asset allocation breakdowns  
- Performance comparisons
- Portfolio value analysis
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union
from decimal import Decimal
from django.db.models import Sum, Count, Avg, Max, Min, Q
from ..models import Asset, Position, Transaction, AssetSnapshot

logger = logging.getLogger(__name__)


class PerformanceService:
    """
    Performance metrics calculator for portfolios using Django ORM.
    Ported from ProjectAurum with identical calculation logic.
    """
    
    def __init__(self):
        """Initialize the performance service."""
        logger.debug("PerformanceService initialized")
        
    def calculate_performance_metrics(self, client: str, start_date: Union[str, date], 
                                    end_date: Union[str, date]) -> Dict[str, Any]:
        """
        Calculate comprehensive performance metrics for a client.
        
        Args:
            client: Client code (e.g., 'JN', 'AU')
            start_date: Start date for performance calculation
            end_date: End date for performance calculation
            
        Returns:
            Dict: Comprehensive performance metrics
        """
        try:
            # Convert dates
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            logger.info(f"Calculating performance metrics for {client}: {start_date} to {end_date}")
            
            # Get portfolio values
            start_value = self._get_portfolio_value_on_date(client, start_date)
            end_value = self._get_portfolio_value_on_date(client, end_date)
            
            # Calculate Modified Dietz return
            from .modified_dietz_service import ModifiedDietzService
            dietz_service = ModifiedDietzService()
            
            dietz_return = dietz_service.calculate_return(client, start_date, end_date)
            dietz_details = dietz_service.calculate_portfolio_return_detailed(client, start_date, end_date)
            
            # Calculate additional metrics
            absolute_gain_loss = end_value - start_value - dietz_details.get('net_external_flows', 0)
            value_change = end_value - start_value
            
            # Get asset allocation for the end date
            asset_allocation = self._calculate_asset_allocation(client, end_date)
            
            # Calculate income metrics
            income_metrics = self._calculate_income_metrics(client, start_date, end_date)
            
            # Build comprehensive metrics
            metrics = {
                'client': client,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'period_days': (end_date - start_date).days,
                
                # Portfolio Values
                'start_value': start_value,
                'end_value': end_value,
                'value_change': value_change,
                'absolute_gain_loss': absolute_gain_loss,
                
                # Modified Dietz Metrics
                'modified_dietz_return': dietz_return,
                'net_external_flows': dietz_details.get('net_external_flows', 0),
                'weighted_external_flows': dietz_details.get('weighted_external_flows', 0),
                'average_capital': dietz_details.get('average_capital', start_value),
                'external_flow_count': dietz_details.get('external_flow_count', 0),
                
                # Asset Allocation
                'asset_allocation': asset_allocation,
                
                # Income Metrics
                'income_metrics': income_metrics,
                
                # Performance Summary
                'performance_summary': {
                    'total_return_pct': dietz_return,
                    'total_gain_loss_dollar': absolute_gain_loss,
                    'annualized_return': self._annualize_return(dietz_return, (end_date - start_date).days),
                    'best_performing_asset': self._get_best_performing_asset(client, start_date, end_date),
                    'worst_performing_asset': self._get_worst_performing_asset(client, start_date, end_date)
                }
            }
            
            logger.info(f"Performance metrics calculated for {client}: {dietz_return:.2f}% return")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics for {client}: {str(e)}")
            return {
                'client': client,
                'error': str(e),
                'modified_dietz_return': 0.0,
                'start_value': 0.0,
                'end_value': 0.0
            }
    
    def calculate_portfolio_metrics_simple(self, client: str, date: Union[str, date]) -> Dict[str, Any]:
        """
        Calculate simple portfolio metrics for a single date.
        Used for dashboard displays.
        
        Args:
            client: Client code
            date: Date to calculate metrics for
            
        Returns:
            Dict: Simple portfolio metrics
        """
        try:
            if isinstance(date, str):
                date = datetime.strptime(date, '%Y-%m-%d').date()
            
            # Get positions for the date
            positions = Position.objects.filter(
                client=client, 
                date=date
            ).select_related('asset')
            
            if not positions.exists():
                return {
                    'client': client,
                    'date': date.strftime('%Y-%m-%d'),
                    'total_value': 0.0,
                    'position_count': 0,
                    'asset_allocation': {}
                }
            
            # Calculate total value
            total_value = positions.aggregate(total=Sum('market_value'))['total'] or 0.0
            
            # Calculate asset allocation
            allocation = {}
            for position in positions:
                asset_type = position.asset.asset_type
                if asset_type not in allocation:
                    allocation[asset_type] = {'value': 0.0, 'count': 0}
                allocation[asset_type]['value'] += position.market_value
                allocation[asset_type]['count'] += 1
            
            # Convert to percentages
            allocation_pct = {}
            for asset_type, data in allocation.items():
                allocation_pct[asset_type] = {
                    'percentage': (data['value'] / total_value * 100) if total_value > 0 else 0,
                    'value': data['value'],
                    'count': data['count']
                }
            
            return {
                'client': client,
                'date': date.strftime('%Y-%m-%d'),
                'total_value': total_value,
                'position_count': len(positions),
                'asset_allocation': allocation_pct,
                'largest_position': self._get_largest_position(positions),
                'asset_type_count': len(allocation)
            }
            
        except Exception as e:
            logger.error(f"Error calculating simple portfolio metrics for {client}: {str(e)}")
            return {
                'client': client,
                'error': str(e),
                'total_value': 0.0
            }
    
    def _get_portfolio_value_on_date(self, client: str, target_date: date) -> float:
        """Get total portfolio value for a client on a specific date."""
        try:
            total_value = Position.objects.filter(
                client=client,
                date=target_date
            ).aggregate(total=Sum('market_value'))['total']
            
            return float(total_value or 0.0)
            
        except Exception as e:
            logger.error(f"Error getting portfolio value for {client} on {target_date}: {str(e)}")
            return 0.0
    
    def _calculate_asset_allocation(self, client: str, date: date) -> Dict[str, Any]:
        """Calculate detailed asset allocation breakdown."""
        try:
            positions = Position.objects.filter(
                client=client,
                date=date
            ).select_related('asset')
            
            if not positions.exists():
                return {'breakdown': {}, 'total_value': 0.0}
            
            total_value = sum(pos.market_value for pos in positions)
            allocation = {}
            
            for position in positions:
                asset_type = position.asset.asset_type
                currency = position.asset.currency
                
                if asset_type not in allocation:
                    allocation[asset_type] = {
                        'value': 0.0,
                        'percentage': 0.0,
                        'count': 0,
                        'currencies': {}
                    }
                
                allocation[asset_type]['value'] += position.market_value
                allocation[asset_type]['count'] += 1
                
                if currency not in allocation[asset_type]['currencies']:
                    allocation[asset_type]['currencies'][currency] = 0.0
                allocation[asset_type]['currencies'][currency] += position.market_value
            
            # Calculate percentages
            for asset_type, data in allocation.items():
                data['percentage'] = (data['value'] / total_value * 100) if total_value > 0 else 0
            
            return {
                'breakdown': allocation,
                'total_value': total_value,
                'calculation_date': date.strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            logger.error(f"Error calculating asset allocation for {client}: {str(e)}")
            return {'breakdown': {}, 'total_value': 0.0}
    
    def _calculate_income_metrics(self, client: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """Calculate income-related metrics for the period."""
        try:
            # Get income-generating transactions
            income_transactions = Transaction.objects.filter(
                client=client,
                transaction_date__gte=start_date,
                transaction_date__lte=end_date,
                transaction_type__in=[
                    'Cash Dividend', 'Dividend', 'Dividends',
                    'Bond Interest', 'Interest', 'Interest Received',
                    'Interest Income', 'Interest Payment'
                ]
            ).select_related('asset')
            
            total_income = sum(tx.amount for tx in income_transactions if tx.amount > 0)
            dividend_income = sum(
                tx.amount for tx in income_transactions 
                if 'dividend' in tx.transaction_type.lower() and tx.amount > 0
            )
            interest_income = sum(
                tx.amount for tx in income_transactions 
                if 'interest' in tx.transaction_type.lower() and tx.amount > 0
            )
            
            return {
                'total_income': total_income,
                'dividend_income': dividend_income,
                'interest_income': interest_income,
                'income_transaction_count': len(income_transactions),
                'period_days': (end_date - start_date).days
            }
            
        except Exception as e:
            logger.error(f"Error calculating income metrics for {client}: {str(e)}")
            return {
                'total_income': 0.0,
                'dividend_income': 0.0,
                'interest_income': 0.0,
                'income_transaction_count': 0
            }
    
    def _annualize_return(self, period_return: float, period_days: int) -> float:
        """Convert period return to annualized return."""
        if period_days <= 0:
            return 0.0
        
        try:
            # Annualized return = (1 + period_return/100)^(365/period_days) - 1
            period_return_decimal = period_return / 100
            annualized = ((1 + period_return_decimal) ** (365 / period_days)) - 1
            return annualized * 100
            
        except Exception as e:
            logger.error(f"Error annualizing return: {str(e)}")
            return 0.0
    
    def _get_best_performing_asset(self, client: str, start_date: date, end_date: date) -> Optional[Dict[str, Any]]:
        """Find the best performing asset for the period."""
        # This is a simplified implementation - could be enhanced with actual performance tracking
        try:
            # Get positions with highest market value increase (simplified)
            end_positions = Position.objects.filter(
                client=client,
                date=end_date
            ).select_related('asset').order_by('-market_value')[:1]
            
            if end_positions:
                position = end_positions[0]
                return {
                    'ticker': position.asset.ticker,
                    'name': position.asset.name,
                    'market_value': position.market_value,
                    'asset_type': position.asset.asset_type
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding best performing asset: {str(e)}")
            return None
    
    def _get_worst_performing_asset(self, client: str, start_date: date, end_date: date) -> Optional[Dict[str, Any]]:
        """Find the worst performing asset for the period."""
        # This is a simplified implementation - could be enhanced with actual performance tracking
        try:
            # Get positions with lowest market value (simplified)
            end_positions = Position.objects.filter(
                client=client,
                date=end_date,
                market_value__lt=0  # Look for negative values or losses
            ).select_related('asset').order_by('market_value')[:1]
            
            if end_positions:
                position = end_positions[0]
                return {
                    'ticker': position.asset.ticker,
                    'name': position.asset.name,
                    'market_value': position.market_value,
                    'asset_type': position.asset.asset_type
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding worst performing asset: {str(e)}")
            return None
    
    def _get_largest_position(self, positions) -> Optional[Dict[str, Any]]:
        """Get the largest position by market value."""
        try:
            if not positions:
                return None
                
            largest = max(positions, key=lambda p: p.market_value)
            return {
                'ticker': largest.asset.ticker,
                'name': largest.asset.name,
                'market_value': largest.market_value,
                'quantity': largest.quantity,
                'asset_type': largest.asset.asset_type
            }
            
        except Exception as e:
            logger.error(f"Error finding largest position: {str(e)}")
            return None