"""
Modified Dietz return calculation service for AurumFinance.
Migrated from ProjectAurum calculations/returns.py with Django ORM integration.

Implements industry-standard Modified Dietz method for time-weighted returns.
Formula: (End Value - Begin Value - Net External Flows) / (Begin Value + Weighted External Flows)
"""

import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from django.db.models import Sum, Q
from ..models import Position, Transaction

logger = logging.getLogger(__name__)


class ModifiedDietzService:
    """
    Modified Dietz return calculator using Django ORM.
    Ported from ProjectAurum with identical calculation logic.
    """
    
    def __init__(self):
        """Initialize Modified Dietz calculator."""
        logger.debug("ModifiedDietzService initialized")
    
    def _calculate_alt_position_adjustments(self, transactions: List[Transaction], 
                                           cash_flow_service) -> Tuple[float, float]:
        """
        Calculate position value adjustments for ALT transactions to prevent phantom gains/losses.
        
        ALT Purchases (TRADING_EXCLUDED): Create phantom gains when portfolio value increases
        but transaction is excluded from cash flows. Solution: Subtract purchase amount.
        
        ALT Sales (INVESTMENT_INCLUDED): Create phantom losses when money exits portfolio
        but sale is counted as investment performance. Solution: Add back sale amount.
        
        Returns:
            Tuple[float, float]: (purchase_adjustment, sale_adjustment)
        """
        from collections import defaultdict
        
        alt_purchase_adjustment = 0.0  # Amount to subtract from end value
        alt_sale_adjustment = 0.0      # Amount to add back to end value
        
        # Group transactions by date to detect withdrawal patterns
        tx_by_date = defaultdict(list)
        for tx in transactions:
            tx_by_date[tx.date].append(tx)
        
        # Process ALT transactions
        for tx in transactions:
            if tx.bank == 'ALT':
                is_excluded = cash_flow_service.is_external_cash_flow(tx)
                cf_amount = cash_flow_service.get_cash_flow_amount(tx)
                
                # ALT PURCHASE: Excluded from cash flows but creates position increase
                if (tx.transaction_type.lower() == 'purchase' and 
                    is_excluded and cf_amount == 0.0):
                    alt_purchase_adjustment += float(tx.amount)
                    logger.debug(f"ALT purchase adjustment: -${tx.amount:,.2f} "
                               f"(removes phantom gain)")
                
                # ALT SALE: Included in returns but may have money exit portfolio
                elif (tx.transaction_type.lower() == 'sale' and 
                      not is_excluded):
                    # Check for corresponding withdrawal on same date
                    same_date_withdrawals = [
                        t for t in tx_by_date[tx.date]
                        if ('withdrawal' in t.transaction_type.lower() and
                            abs(float(t.amount)) >= float(tx.amount) * 0.95)  # 95% match tolerance
                    ]
                    
                    if same_date_withdrawals:
                        alt_sale_adjustment += float(tx.amount)
                        logger.debug(f"ALT sale adjustment: +${tx.amount:,.2f} "
                                   f"(removes phantom loss from money exit)")
        
        return alt_purchase_adjustment, alt_sale_adjustment

    def calculate_return(self, client: str, start_date: Union[str, date], 
                        end_date: Union[str, date]) -> float:
        """
        Calculate Modified Dietz return for a client over a period.
        
        Args:
            client: Client code (e.g., 'JN', 'AU')
            start_date: Start date of the period
            end_date: End date of the period
            
        Returns:
            Float: Return percentage (Modified Dietz return)
        """
        try:
            # Convert string dates to date objects
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            elif isinstance(start_date, datetime):
                start_date = start_date.date()
                
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            elif isinstance(end_date, datetime):
                end_date = end_date.date()
            
            # Get period length in days
            period_days = (end_date - start_date).days
            if period_days <= 0:
                logger.warning(f"Invalid period: {start_date} to {end_date}")
                return 0.0
            
            # Get portfolio values at start and end
            start_value = self._get_portfolio_value_on_date(client, start_date)
            end_value = self._get_portfolio_value_on_date(client, end_date)
            
            # Get transactions during the period
            transactions = self._get_transactions_for_period(client, start_date, end_date)
            
            # Calculate cash flows using our cash flow service
            from .cash_flow_service import CashFlowService
            cash_flow_service = CashFlowService()
            
            net_external_flows = 0.0
            weighted_external_flows = 0.0
            external_flow_count = 0
            
            logger.debug(f"Calculating Modified Dietz return for client {client}: {start_date} to {end_date} ({period_days} days)")
            logger.debug(f"Start value: ${start_value:,.2f}, End value: ${end_value:,.2f}")
            
            # No ALT adjustments needed since ALT transactions are excluded
            alt_purchase_adjustment, alt_sale_adjustment = 0.0, 0.0
            adjusted_end_value = end_value
            
            if alt_purchase_adjustment > 0 or alt_sale_adjustment > 0:
                logger.info(f"ALT adjustments applied: Purchase adj: -${alt_purchase_adjustment:,.2f}, "
                           f"Sale adj: +${alt_sale_adjustment:,.2f}")
                logger.info(f"End value adjusted: ${end_value:,.2f} → ${adjusted_end_value:,.2f}")
            
            # Process each transaction
            for tx in transactions:
                # Check if it's an external cash flow
                if cash_flow_service.is_external_cash_flow(tx):
                    cf_amount = cash_flow_service.get_cash_flow_amount(tx)
                    
                    # Calculate time weight: (Days remaining after cash flow) / (Total period days)
                    tx_date = tx.date
                    if isinstance(tx_date, datetime):
                        tx_date = tx_date.date()
                    
                    days_from_start = (tx_date - start_date).days
                    weight = (period_days - days_from_start) / period_days
                    
                    # Accumulate cash flows
                    net_external_flows += cf_amount
                    weighted_external_flows += cf_amount * weight
                    external_flow_count += 1
                    
                    logger.debug(f"External flow: {tx.transaction_type} "
                               f"${cf_amount:,.2f} on {tx_date} (weight: {weight:.4f})")
            
            # Calculate Modified Dietz return using correct formula with ALT adjustments
            gain_or_loss = adjusted_end_value - start_value - net_external_flows
            average_capital = start_value + weighted_external_flows
            
            logger.debug(f"External flows summary: {external_flow_count} flows, "
                        f"Net: ${net_external_flows:,.2f}, Weighted: ${weighted_external_flows:,.2f}")
            logger.debug(f"Gain/Loss: ${gain_or_loss:,.2f}, Average Capital: ${average_capital:,.2f}")
            
            if alt_purchase_adjustment > 0 or alt_sale_adjustment > 0:
                logger.debug(f"ALT adjustments summary: Purchase: -${alt_purchase_adjustment:,.2f}, "
                           f"Sale: +${alt_sale_adjustment:,.2f}, Net effect: ${alt_sale_adjustment - alt_purchase_adjustment:,.2f}")
            
            # Handle edge cases
            if average_capital == 0:
                logger.warning("Average capital is zero - cannot calculate return")
                return 0.0
                
            if abs(average_capital) < 0.01:  # Very small average capital
                logger.warning(f"Very small average capital: ${average_capital:.2f} - return may be unreliable")
            
            # Calculate return as percentage
            return_percentage = (gain_or_loss / average_capital) * 100
            
            logger.debug(f"Modified Dietz return: {return_percentage:.4f}%")
            
            return return_percentage
            
        except Exception as e:
            logger.error(f"Error calculating Modified Dietz return for {client}: {str(e)}")
            return 0.0
    
    def calculate_portfolio_return_detailed(self, client: str, start_date: Union[str, date], 
                                          end_date: Union[str, date]) -> Dict[str, Any]:
        """
        Calculate portfolio return with detailed breakdown.
        
        Args:
            client: Client code
            start_date: Start date of the period
            end_date: End date of the period
        
        Returns:
            Dict with detailed return calculation breakdown
        """
        try:
            # Convert dates
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            elif isinstance(start_date, datetime):
                start_date = start_date.date()
                
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            elif isinstance(end_date, datetime):
                end_date = end_date.date()
            
            period_days = (end_date - start_date).days
            
            # Get values
            start_value = self._get_portfolio_value_on_date(client, start_date)
            end_value = self._get_portfolio_value_on_date(client, end_date)
            
            # Calculate return
            return_percentage = self.calculate_return(client, start_date, end_date)
            
            # Get detailed cash flow breakdown
            transactions = self._get_transactions_for_period(client, start_date, end_date)
            
            from .cash_flow_service import CashFlowService
            cash_flow_service = CashFlowService()
            
            net_external_flows = 0.0
            weighted_external_flows = 0.0
            external_flow_count = 0
            
            # No ALT adjustments needed since ALT transactions are excluded
            alt_purchase_adjustment, alt_sale_adjustment = 0.0, 0.0
            
            for tx in transactions:
                if cash_flow_service.is_external_cash_flow(tx):
                    cf_amount = cash_flow_service.get_cash_flow_amount(tx)
                    
                    tx_date = tx.date
                    if isinstance(tx_date, datetime):
                        tx_date = tx_date.date()
                    
                    days_from_start = (tx_date - start_date).days
                    weight = (period_days - days_from_start) / period_days if period_days > 0 else 0
                    
                    net_external_flows += cf_amount
                    weighted_external_flows += cf_amount * weight
                    external_flow_count += 1
            
            # Apply ALT adjustments to end value
            adjusted_end_value = end_value - alt_purchase_adjustment + alt_sale_adjustment
            
            gain_loss = adjusted_end_value - start_value - net_external_flows
            average_capital = start_value + weighted_external_flows
            
            return {
                'client': client,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'period_days': period_days,
                'start_value': start_value,
                'end_value': end_value,
                'adjusted_end_value': adjusted_end_value,
                'return_percentage': return_percentage,
                'gain_loss': gain_loss,
                'net_external_flows': net_external_flows,
                'weighted_external_flows': weighted_external_flows,
                'average_capital': average_capital,
                'external_flow_count': external_flow_count,
                'alt_purchase_adjustment': alt_purchase_adjustment,
                'alt_sale_adjustment': alt_sale_adjustment
            }
            
        except Exception as e:
            logger.error(f"Error calculating detailed portfolio return for {client}: {str(e)}")
            return {
                'client': client,
                'return_percentage': 0.0,
                'error': str(e)
            }
    
    def _get_portfolio_value_on_date(self, client: str, target_date: date) -> float:
        """
        Get total NON-ALT portfolio value for a client on a specific date.
        
        Args:
            client: Client code
            target_date: Date to get value for
            
        Returns:
            Float: Total NON-ALT portfolio market value
        """
        try:
            total_value = Position.objects.filter(
                snapshot__client__code=client,
                snapshot__snapshot_date=target_date
            ).exclude(asset__bank='ALT').aggregate(total=Sum('market_value'))['total']
            
            return float(total_value) if total_value else 0.0
            
        except Exception as e:
            logger.error(f"Error getting portfolio value for {client} on {target_date}: {str(e)}")
            return 0.0
    
    def _get_transactions_for_period(self, client: str, start_date: date, end_date: date) -> List[Transaction]:
        """
        Get all NON-ALT transactions for a client during a period.
        
        Args:
            client: Client code
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            List of NON-ALT Transaction objects
        """
        try:
            transactions = Transaction.objects.filter(
                client__code=client,
                date__gte=start_date,
                date__lte=end_date
            ).exclude(bank='ALT').select_related('asset').order_by('date')
            
            return list(transactions)
            
        except Exception as e:
            logger.error(f"Error getting transactions for {client} from {start_date} to {end_date}: {str(e)}")
            return []