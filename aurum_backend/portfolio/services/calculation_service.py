"""
Core calculation service orchestrator for Aurum Finance.
Coordinates between different calculation services to provide comprehensive portfolio analytics.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, date
from django.db.models import Sum, Count, Q
from ..models import Asset, Position, Transaction, AssetSnapshot

logger = logging.getLogger(__name__)


class CalculationService:
    """
    Main orchestrator for all portfolio calculations.
    Coordinates between ModifiedDietz, CashFlow, and Performance services.
    """
    
    def __init__(self):
        """Initialize calculation service with sub-services."""
        # Import here to avoid circular imports
        from .modified_dietz_service import ModifiedDietzService
        from .cash_flow_service import CashFlowService
        from .performance_service import PerformanceService
        
        self.dietz_service = ModifiedDietzService()
        self.cash_flow_service = CashFlowService()
        self.performance_service = PerformanceService()
        
        logger.info("CalculationService initialized with all sub-services")
    
    def calculate_portfolio_metrics(self, client: str, current_date: str, 
                                  comparison_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Calculate comprehensive portfolio metrics for a client.
        
        Args:
            client: Client code (e.g., 'JN', 'AU')
            current_date: Current date in YYYY-MM-DD format
            comparison_date: Optional comparison date for performance calculations
            
        Returns:
            Dict containing all portfolio metrics
        """
        try:
            logger.info(f"Calculating portfolio metrics for client {client} on {current_date}")
            
            # Get basic portfolio data
            current_positions = self._get_positions_for_date(client, current_date)
            current_transactions = self._get_transactions_for_period(client, comparison_date, current_date)
            
            # Calculate current portfolio value
            current_value = self._calculate_portfolio_value(current_positions)
            
            # Calculate performance metrics if comparison date provided
            performance_metrics = {}
            if comparison_date:
                performance_metrics = self.performance_service.calculate_performance_metrics(
                    client, comparison_date, current_date
                )
            
            # Calculate asset allocation
            asset_allocation = self._calculate_asset_allocation(current_positions)
            
            # Combine all metrics
            metrics = {
                'client': client,
                'calculation_date': current_date,
                'comparison_date': comparison_date,
                'portfolio_value': current_value,
                'asset_allocation': asset_allocation,
                'performance_metrics': performance_metrics,
                'position_count': len(current_positions),
                'transaction_count': len(current_transactions)
            }
            
            logger.info(f"Portfolio metrics calculated successfully for client {client}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating portfolio metrics for {client}: {str(e)}")
            raise
    
    def calculate_admin_metrics(self) -> Dict[str, Any]:
        """
        Calculate admin dashboard metrics across all clients.
        
        Returns:
            Dict containing admin-level metrics
        """
        try:
            logger.info("Calculating admin dashboard metrics")
            
            # Get all clients
            clients = self._get_active_clients()
            
            # Calculate totals
            total_market_value = Position.objects.aggregate(
                total=Sum('market_value')
            )['total'] or 0.0
            
            total_clients = len(clients)
            total_assets = Asset.objects.count()
            
            # Get recent performance data
            recent_date = self._get_most_recent_snapshot_date()
            previous_date = self._get_previous_snapshot_date(recent_date)
            
            overall_performance = {}
            if recent_date and previous_date:
                # Calculate overall performance across all clients
                overall_performance = self._calculate_overall_performance(previous_date, recent_date)
            
            metrics = {
                'total_clients': total_clients,
                'total_assets': total_assets,
                'total_market_value': total_market_value,
                'recent_date': recent_date,
                'overall_performance': overall_performance
            }
            
            logger.info("Admin metrics calculated successfully")
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating admin metrics: {str(e)}")
            raise
    
    def _get_positions_for_date(self, client: str, date: str) -> List[Position]:
        """Get positions for a specific client and date."""
        return list(Position.objects.filter(
            client=client,
            date=date
        ).select_related('asset'))
    
    def _get_transactions_for_period(self, client: str, start_date: str, end_date: str) -> List[Transaction]:
        """Get transactions for a specific client and date range."""
        if not start_date:
            return []
            
        return list(Transaction.objects.filter(
            client=client,
            transaction_date__gte=start_date,
            transaction_date__lte=end_date
        ).select_related('asset'))
    
    def _calculate_portfolio_value(self, positions: List[Position]) -> float:
        """Calculate total portfolio value from positions."""
        return sum(pos.market_value for pos in positions)
    
    def _calculate_asset_allocation(self, positions: List[Position]) -> Dict[str, Any]:
        """Calculate asset allocation breakdown."""
        if not positions:
            return {'breakdown': {}, 'total': 0.0}
            
        allocation = {}
        total_value = sum(pos.market_value for pos in positions)
        
        for position in positions:
            asset_type = position.asset.asset_type
            if asset_type not in allocation:
                allocation[asset_type] = 0.0
            allocation[asset_type] += position.market_value
        
        # Convert to percentages
        allocation_pct = {}
        for asset_type, value in allocation.items():
            allocation_pct[asset_type] = (value / total_value * 100) if total_value > 0 else 0
        
        return {
            'breakdown': allocation_pct,
            'total': total_value
        }
    
    def _get_active_clients(self) -> List[str]:
        """Get list of all active clients."""
        return list(Asset.objects.values_list('client', flat=True).distinct())
    
    def _get_most_recent_snapshot_date(self) -> Optional[str]:
        """Get the most recent snapshot date."""
        snapshot = AssetSnapshot.objects.order_by('-snapshot_date').first()
        return snapshot.snapshot_date.strftime('%Y-%m-%d') if snapshot else None
    
    def _get_previous_snapshot_date(self, current_date: str) -> Optional[str]:
        """Get the snapshot date before the given date."""
        if not current_date:
            return None
            
        snapshot = AssetSnapshot.objects.filter(
            snapshot_date__lt=current_date
        ).order_by('-snapshot_date').first()
        
        return snapshot.snapshot_date.strftime('%Y-%m-%d') if snapshot else None
    
    def _calculate_overall_performance(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Calculate overall performance across all clients."""
        # This will be implemented with the performance service
        return {
            'period_return': 0.0,
            'total_gain_loss': 0.0,
            'start_date': start_date,
            'end_date': end_date
        }