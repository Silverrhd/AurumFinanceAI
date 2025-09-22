"""
Pure Django Portfolio Calculation Service
Implements ProjectAurum calculations using only Django models.
"""

from django.db.models import Sum, F, Q
from ..models import Client, PortfolioSnapshot, Position, Transaction
from decimal import Decimal, ROUND_HALF_UP
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
from .modified_dietz_service import ModifiedDietzService

logger = logging.getLogger(__name__)

class PortfolioCalculationService:
    """Pure Django service for portfolio calculations."""
    
    def calculate_portfolio_metrics(self, client_code: str, snapshot_date: str) -> dict:
        """
        Calculate comprehensive portfolio metrics using Django ORM.
        
        Args:
            client_code: Client identifier
            snapshot_date: Snapshot date (YYYY-MM-DD)
            
        Returns:
            Dict with calculated metrics matching ProjectAurum format
        """
        logger.info(f"Calculating portfolio metrics for {client_code} on {snapshot_date}")
        
        client = Client.objects.get(code=client_code)
        snapshot = PortfolioSnapshot.objects.get(client=client, snapshot_date=snapshot_date)
        
        # Get positions and transactions (EXCLUDE ALT for presentation)
        positions = Position.objects.filter(snapshot=snapshot).exclude(asset__bank='ALT').select_related('asset')
        transactions = Transaction.objects.filter(client=client, date__lte=snapshot_date).exclude(bank='ALT')
        
        # Calculate core metrics
        metrics = {
            'snapshot_date': snapshot_date,
            'client_code': client_code,
            'calculation_timestamp': datetime.now().isoformat()
        }
        
        # Basic portfolio values
        metrics.update(self._calculate_basic_metrics(positions))
        
        # Asset allocation
        metrics['asset_allocation'] = self._calculate_asset_allocation(positions, metrics['total_value'])
        
        # Custody allocation
        metrics['custody_allocation'] = self._calculate_custody_allocation(positions, metrics['total_value'])
        
        # Bank allocation
        metrics['bank_allocation'] = self._calculate_bank_allocation(positions, metrics['total_value'])
        
        # Annual income calculations
        metrics['estimated_annual_income'] = self._calculate_annual_income(positions)
        
        # Modified Dietz calculations
        metrics.update(self._calculate_modified_dietz_returns(client, snapshot_date, positions, transactions))
        
        # Top movers (requires comparison with previous period)
        metrics['top_movers'] = self._calculate_top_movers(client, snapshot_date, positions)
        
        # Bond maturity timeline
        metrics['bond_maturity'] = self._calculate_bond_maturity(positions)
        
        # Position details for reports
        metrics['positions_by_type'] = self._group_positions_by_type(positions)
        
        # Recent transactions
        metrics['recent_transactions'] = self._get_recent_transactions(transactions)
        
        # Chart data for dashboard
        metrics['chart_data'] = self._prepare_chart_data(metrics)
        
        # Update snapshot with calculated metrics
        snapshot.portfolio_metrics = metrics
        snapshot.save()
        
        logger.info(f"Calculated metrics for {client_code}: ${metrics['total_value']:,.2f} total value")
        return metrics
    
    def _calculate_basic_metrics(self, positions) -> dict:
        """Calculate basic portfolio metrics."""
        total_value = positions.aggregate(total=Sum('market_value'))['total'] or Decimal('0')
        total_cost_basis = positions.aggregate(total=Sum('cost_basis'))['total'] or Decimal('0')
        
        unrealized_gain_loss = total_value - total_cost_basis
        unrealized_gain_loss_pct = 0
        if total_cost_basis > 0:
            unrealized_gain_loss_pct = float((unrealized_gain_loss / total_cost_basis) * 100)
        
        return {
            'total_value': float(total_value),
            'total_cost_basis': float(total_cost_basis),
            'unrealized_gain_loss': float(unrealized_gain_loss),
            'unrealized_gain_loss_pct': unrealized_gain_loss_pct,
            'position_count': positions.count()
        }
    
    def _calculate_asset_allocation(self, positions, total_value: float) -> dict:
        """Calculate asset allocation by asset type."""
        allocation = {}
        
        for position in positions:
            asset_type = position.asset.asset_type
            if asset_type not in allocation:
                allocation[asset_type] = {'value': Decimal('0'), 'percentage': 0}
            allocation[asset_type]['value'] += position.market_value
        
        # Calculate percentages
        for asset_type in allocation:
            if total_value > 0:
                allocation[asset_type]['percentage'] = float(
                    (allocation[asset_type]['value'] / Decimal(str(total_value))) * 100
                )
            allocation[asset_type]['value'] = float(allocation[asset_type]['value'])
        
        return allocation
    
    def _calculate_custody_allocation(self, positions, total_value: float) -> dict:
        """Calculate custody allocation by bank + account."""
        allocation = {}
        
        for position in positions:
            custody = f"{position.bank} {position.account}".strip()
            if custody not in allocation:
                allocation[custody] = {'value': Decimal('0'), 'percentage': 0}
            allocation[custody]['value'] += position.market_value
        
        # Calculate percentages
        for custody in allocation:
            if total_value > 0:
                allocation[custody]['percentage'] = float(
                    (allocation[custody]['value'] / Decimal(str(total_value))) * 100
                )
            allocation[custody]['value'] = float(allocation[custody]['value'])
        
        return allocation
    
    def _calculate_bank_allocation(self, positions, total_value: float) -> dict:
        """Calculate bank allocation by bank field."""
        allocation = {}
        
        for position in positions:
            bank_name = position.bank
            if bank_name not in allocation:
                allocation[bank_name] = {'value': Decimal('0'), 'percentage': 0}
            allocation[bank_name]['value'] += position.market_value
        
        # Calculate percentages
        for bank_name in allocation:
            if total_value > 0:
                allocation[bank_name]['percentage'] = float(
                    (allocation[bank_name]['value'] / Decimal(str(total_value))) * 100
                )
            allocation[bank_name]['value'] = float(allocation[bank_name]['value'])
        
        return allocation
    
    def _calculate_annual_income(self, positions) -> float:
        """Calculate estimated annual income (coupon_rate * quantity)."""
        annual_income = Decimal('0')
        
        for position in positions:
            if position.coupon_rate and position.quantity:
                # All coupon rates are stored as percentages, always divide by 100
                # This matches the Position model calculation logic
                coupon_rate = Decimal(str(position.coupon_rate)) / 100
                position_income = coupon_rate * position.quantity
                annual_income += position_income
        
        return float(annual_income)
    
    def _calculate_modified_dietz_returns(self, client: Client, snapshot_date: str, 
                                        positions, transactions) -> dict:
        """
        Calculate Modified Dietz returns (excludes external cash flows).
        
        This is a simplified implementation. For full accuracy, we'd need:
        - Cash flow categorization by transaction type
        - Time-weighted cash flow calculations
        - Previous period values for comparison
        """
        # Get previous snapshot for comparison
        previous_snapshot = self._get_previous_snapshot(client, snapshot_date)
        
        if not previous_snapshot:
            # First snapshot - no comparison possible
            return {
                'real_gain_loss_dollar': 0.0,
                'real_gain_loss_percent': 0.0,
                'inception_gain_loss_dollar': 0.0,
                'inception_gain_loss_percent': 0.0,
                'period_return': 0.0
            }
        
        # Current values
        current_value = sum(float(p.market_value) for p in positions)
        previous_value = previous_snapshot.portfolio_metrics.get('total_value', 0)
        
        # Calculate period cash flows (simplified - excludes external flows)
        period_start = previous_snapshot.snapshot_date if isinstance(previous_snapshot.snapshot_date, date) else datetime.strptime(previous_snapshot.snapshot_date, '%Y-%m-%d').date()
        period_end = datetime.strptime(snapshot_date, '%Y-%m-%d').date() if isinstance(snapshot_date, str) else snapshot_date
        
        period_transactions = transactions.filter(
            date__gt=period_start,
            date__lte=period_end
        )
        
        # Simplified cash flow calculation (would need bank-specific categorization)
        net_cash_flow = sum(float(tx.amount) for tx in period_transactions)
        
        # Modified Dietz calculation (simplified)
        if previous_value > 0:
            # Real gain/loss excludes cash flows
            real_gain_loss = current_value - previous_value - net_cash_flow
            real_gain_loss_percent = (real_gain_loss / previous_value) * 100
        else:
            real_gain_loss = 0.0
            real_gain_loss_percent = 0.0
        
        # Inception returns (from first snapshot)
        first_snapshot = PortfolioSnapshot.objects.filter(client=client).order_by('snapshot_date').first()
        if first_snapshot and first_snapshot.snapshot_date != snapshot_date:
            inception_value = first_snapshot.portfolio_metrics.get('total_value', 0)
            if inception_value > 0:
                # Use Modified Dietz method for accurate performance calculation
                dietz_service = ModifiedDietzService()
                detailed_result = dietz_service.calculate_portfolio_return_detailed(
                    client.code,
                    first_snapshot.snapshot_date,
                    snapshot_date
                )
                inception_gain_loss = detailed_result.get('gain_loss', 0)
                inception_gain_loss_percent = detailed_result.get('return_percentage', 0)
            else:
                inception_gain_loss = 0.0
                inception_gain_loss_percent = 0.0
        else:
            inception_gain_loss = 0.0
            inception_gain_loss_percent = 0.0
        
        return {
            'real_gain_loss_dollar': real_gain_loss,
            'real_gain_loss_percent': real_gain_loss_percent,
            'inception_gain_loss_dollar': inception_gain_loss,
            'inception_gain_loss_percent': inception_gain_loss_percent,
            'period_return': real_gain_loss_percent,
            'net_cash_flow': net_cash_flow
        }
    
    def _calculate_top_movers(self, client: Client, snapshot_date: str, positions) -> dict:
        """Calculate top movers by dollar change."""
        previous_snapshot = self._get_previous_snapshot(client, snapshot_date)
        
        if not previous_snapshot:
            return {'gainers': [], 'losers': []}
        
        # Get previous positions
        previous_positions = Position.objects.filter(
            snapshot__client=client,
            snapshot__snapshot_date=previous_snapshot.snapshot_date
        ).exclude(asset__bank='ALT').select_related('asset')
        
        # Create lookup for previous values
        previous_values = {pos.asset.ticker: float(pos.market_value) for pos in previous_positions}
        
        # Calculate changes
        movers = []
        for position in positions:
            ticker = position.asset.ticker
            current_value = float(position.market_value)
            previous_value = previous_values.get(ticker, 0)
            
            if previous_value > 0:
                dollar_change = current_value - previous_value
                percent_change = (dollar_change / previous_value) * 100
                
                movers.append({
                    'ticker': ticker,
                    'name': position.asset.name,
                    'asset_type': position.asset.asset_type,
                    'dollar_change': dollar_change,
                    'percent_change': percent_change,
                    'current_value': current_value,
                    'previous_value': previous_value
                })
        
        # Sort by absolute dollar change
        movers.sort(key=lambda x: abs(x['dollar_change']), reverse=True)
        
        # Get top 5 gainers and losers
        gainers = [m for m in movers if m['dollar_change'] > 0][:5]
        losers = [m for m in movers if m['dollar_change'] < 0][:5]
        
        return {'gainers': gainers, 'losers': losers}
    
    def _calculate_bond_maturity(self, positions) -> dict:
        """Calculate bond maturity timeline."""
        bonds = positions.filter(
            asset__asset_type='Fixed Income',
            asset__maturity_date__isnull=False
        )
        
        maturity_data = {}
        for bond in bonds:
            year = bond.asset.maturity_date.year
            if year not in maturity_data:
                maturity_data[year] = {
                    'count': 0,
                    'market_value': 0,
                    'bonds': []
                }
            
            maturity_data[year]['count'] += 1
            maturity_data[year]['market_value'] += float(bond.market_value)
            maturity_data[year]['bonds'].append({
                'name': bond.asset.name,
                'ticker': bond.asset.ticker,
                'market_value': float(bond.market_value),
                'maturity_date': bond.asset.maturity_date.isoformat(),
                'coupon_rate': bond.coupon_rate
            })
        
        # Sort bonds within each year by market value
        for year in maturity_data:
            maturity_data[year]['bonds'].sort(
                key=lambda x: x['market_value'], reverse=True
            )
        
        return maturity_data
    
    def _group_positions_by_type(self, positions) -> dict:
        """Group positions by asset type for report display."""
        grouped = {}
        
        for position in positions:
            asset_type = position.asset.asset_type
            if asset_type not in grouped:
                grouped[asset_type] = []
            
            # Calculate annual income for this position
            annual_income = 0
            if position.coupon_rate and position.quantity:
                coupon_rate = float(position.coupon_rate)
                if coupon_rate > 1:  # Assume it's a percentage
                    coupon_rate = coupon_rate / 100
                annual_income = coupon_rate * float(position.quantity)
            
            grouped[asset_type].append({
                'custody': f"{position.bank} {position.account}".strip(),
                'name': position.asset.name,
                'ticker': position.asset.ticker,
                'quantity': float(position.quantity),
                'market_value': float(position.market_value),
                'cost_basis': float(position.cost_basis),
                'unrealized_gain_loss': float(position.market_value - position.cost_basis),
                'unrealized_gain_loss_pct': float(
                    ((position.market_value - position.cost_basis) / position.cost_basis * 100)
                    if position.cost_basis > 0 else 0
                ),
                'coupon_rate': position.coupon_rate or 0,
                'annual_income': annual_income,
                'maturity_date': position.asset.maturity_date.isoformat() if position.asset.maturity_date else None
            })
        
        return grouped
    
    def _get_recent_transactions(self, transactions, limit: int = 20) -> list:
        """Get recent transactions for display."""
        recent = transactions.order_by('-date')[:limit]
        
        return [{
            'custody': f"{tx.bank} {tx.account}".strip(),
            'date': tx.date.isoformat(),
            'transaction_type': tx.transaction_type,
            'ticker': tx.asset.ticker,
            'cusip': tx.asset.cusip or '',
            'price': float(tx.price) if tx.price else 0,
            'quantity': float(tx.quantity) if tx.quantity else 0,
            'amount': float(tx.amount)
        } for tx in recent]
    
    def _prepare_chart_data(self, metrics: dict) -> dict:
        """Prepare chart data for ApexCharts."""
        return {
            'asset_allocation': {
                'labels': list(metrics['asset_allocation'].keys()),
                'series': [metrics['asset_allocation'][k]['value'] for k in metrics['asset_allocation'].keys()]
            },
            'custody_allocation': {
                'labels': list(metrics['custody_allocation'].keys()),
                'series': [metrics['custody_allocation'][k]['value'] for k in metrics['custody_allocation'].keys()]
            },
            'bank_allocation': {
                'labels': list(metrics['bank_allocation'].keys()),
                'series': [metrics['bank_allocation'][k]['value'] for k in metrics['bank_allocation'].keys()]
            },
            'portfolio_value': {
                'current_value': metrics['total_value'],
                'previous_value': metrics.get('previous_total_value', metrics['total_value'])
            }
        }
    
    def _get_previous_snapshot(self, client: Client, current_date: str) -> Optional[PortfolioSnapshot]:
        """Get the most recent snapshot before the current date."""
        return PortfolioSnapshot.objects.filter(
            client=client,
            snapshot_date__lt=current_date
        ).order_by('-snapshot_date').first()
    
    def get_portfolio_summary(self, client_code: str, snapshot_date: str) -> dict:
        """Get a summary of portfolio metrics for dashboard display."""
        try:
            client = Client.objects.get(code=client_code)
            snapshot = PortfolioSnapshot.objects.get(client=client, snapshot_date=snapshot_date)
            
            metrics = snapshot.portfolio_metrics
            
            return {
                'client_name': client.name,
                'client_code': client_code,
                'snapshot_date': snapshot_date,
                'total_value': metrics.get('total_value', 0),
                'unrealized_gain_loss': metrics.get('unrealized_gain_loss', 0),
                'unrealized_gain_loss_pct': metrics.get('unrealized_gain_loss_pct', 0),
                'estimated_annual_income': metrics.get('estimated_annual_income', 0),
                'position_count': metrics.get('position_count', 0),
                'asset_allocation': metrics.get('asset_allocation', {}),
                'last_updated': snapshot.updated_at.isoformat()
            }
        except (Client.DoesNotExist, PortfolioSnapshot.DoesNotExist):
            return None