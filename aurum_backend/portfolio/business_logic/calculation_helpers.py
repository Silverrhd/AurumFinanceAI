"""
Calculation helpers that provide AurumFinance calculation classes.
Provides financial calculation functionality using Django models and local logic.
"""
import logging
import pandas as pd
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import numpy as np

logger = logging.getLogger(__name__)

def normalize_asset_type(asset_type: str) -> str:
    """
    Normalize asset type names for consistency.
    
    Args:
        asset_type (str): Raw asset type string
        
    Returns:
        str: Normalized asset type
    """
    if not asset_type or not str(asset_type).strip():
        return "Unknown"
    
    asset_type = str(asset_type).strip().lower()
    
    # ProjectAurum-compatible mappings
    type_mappings = {
        # Fixed Income (all bond-related types)
        'fixed income': 'Fixed Income',
        'bond': 'Fixed Income',
        'bonds': 'Fixed Income', 
        'treasury': 'Fixed Income',
        'corporate bond': 'Fixed Income',
        'government bond': 'Fixed Income',
        'municipal bond': 'Fixed Income',
        'treasury bond': 'Fixed Income',
        'treasury note': 'Fixed Income',
        'treasury bill': 'Fixed Income',
        
        # Equities (all stock-related types)  
        'equity': 'Equities',
        'equities': 'Equities',
        'stock': 'Equities',
        'stocks': 'Equities',
        'common stock': 'Equities',
        'preferred stock': 'Equities',
        'share': 'Equities',
        'shares': 'Equities',
        
        # Cash (exact match)
        'cash': 'Cash',
        
        # Money Market (exact match) 
        'money market': 'Money Market',
        'money_market': 'Money Market',
        'mm': 'Money Market',
        
        # Alternatives (catch-all for non-standard types)
        'alternatives': 'Alternatives', 
        'alternative': 'Alternatives',
        'alternative assets': 'Alternatives',
        'commodity': 'Alternatives',
        'real estate': 'Alternatives',
        'private equity': 'Alternatives',
        'hedge fund': 'Alternatives',
        'mutual fund': 'Alternatives',
        'etf': 'Alternatives',
        'reit': 'Alternatives',
        'derivative': 'Alternatives',
        'option': 'Alternatives',
        'future': 'Alternatives',
        'other': 'Alternatives',
        'unknown': 'Alternatives',
    }
    
    for key, value in type_mappings.items():
        if key in asset_type:
            return value
            
    return asset_type.title()

def calculate_safe_unrealized_gain_loss(current_value: float, cost_basis: float) -> float:
    """
    Safely calculate unrealized gains/losses with error handling.
    
    Args:
        current_value (float): Current market value
        cost_basis (float): Original cost basis
        
    Returns:
        float: Unrealized gain/loss amount
    """
    try:
        current_value = float(current_value or 0)
        cost_basis = float(cost_basis or 0)
            
        return current_value - cost_basis
    except (ValueError, TypeError) as e:
        logger.warning(f"Error calculating unrealized gain/loss: {str(e)}")
        return 0.0

def calculate_safe_gain_loss_percentage(current_value: float, cost_basis: float) -> float:
    """
    Safely calculate unrealized gain/loss percentage with error handling.
    
    Args:
        current_value (float): Current market value
        cost_basis (float): Original cost basis
        
    Returns:
        float: Unrealized gain/loss percentage
    """
    try:
        current_value = float(current_value or 0)
        cost_basis = float(cost_basis or 0)
        
        if cost_basis == 0:
            return 0.0
            
        return ((current_value - cost_basis) / cost_basis) * 100
    except (ValueError, TypeError) as e:
        logger.warning(f"Error calculating gain/loss percentage: {str(e)}")
        return 0.0

def calculate_bond_maturity_timeline(positions: List[Dict]) -> Dict:
    """
    Calculate bond maturity timeline from positions.
    
    Args:
        positions (List[Dict]): List of position dictionaries
        
    Returns:
        Dict: Bond maturity timeline data
    """
    try:
        bond_positions = [p for p in positions if 'bond' in str(p.get('asset_type', '')).lower()]
        
        # Group by maturity ranges
        maturity_ranges = {
            '0-1 years': [],
            '1-3 years': [],
            '3-5 years': [],
            '5-10 years': [],
            '10+ years': []
        }
        
        for position in bond_positions:
            # This would need actual maturity date parsing
            # For now, return basic structure
            maturity_ranges['1-3 years'].append(position)
        
        return {
            'maturity_ranges': maturity_ranges,
            'total_bonds': len(bond_positions),
            'total_value': sum(p.get('market_value', 0) for p in bond_positions)
        }
    except Exception as e:
        logger.error(f"Error calculating bond maturity timeline: {str(e)}")
        return {'maturity_ranges': {}, 'total_bonds': 0, 'total_value': 0}

class ModifiedDietzCalculator:
    """
    Modified Dietz return calculation for portfolio performance.
    """
    
    def __init__(self):
        """Initialize the calculator."""
        from ..services.cash_flow_service import CashFlowService
        self.classifier = CashFlowService()
        logger.debug("ModifiedDietzCalculator initialized with CashFlowService")
    
    def calculate_return(self, beginning_value: float, ending_value: float, 
                        cash_flows: List[Tuple[datetime, float]]) -> float:
        """
        Calculate Modified Dietz return.
        
        Args:
            beginning_value (float): Portfolio value at start of period
            ending_value (float): Portfolio value at end of period
            cash_flows (List[Tuple]): List of (date, amount) tuples
            
        Returns:
            float: Modified Dietz return as percentage
        """
        try:
            if beginning_value <= 0:
                return 0.0
            
            # Basic Modified Dietz calculation
            net_cash_flow = sum(cf[1] for cf in cash_flows)
            
            # Simplified weight calculation (would need actual dates for precision)
            weighted_cash_flow = net_cash_flow * 0.5  # Assume mid-period weighting
            
            denominator = beginning_value + weighted_cash_flow
            if denominator <= 0:
                return 0.0
                
            return_value = (ending_value - beginning_value - net_cash_flow) / denominator * 100
            
            logger.debug(f"Modified Dietz return calculated: {return_value:.4f}%")
            return return_value
            
        except Exception as e:
            logger.error(f"Error calculating Modified Dietz return: {str(e)}")
            return 0.0

class PerformanceCalculator:
    """
    Performance metrics calculator for portfolios.
    """
    
    def __init__(self):
        """Initialize the performance calculator."""
        logger.debug("PerformanceCalculator initialized")
        
    def calculate_portfolio_metrics(self, positions: List[Dict]) -> Dict:
        """
        Calculate various portfolio performance metrics.
        
        Args:
            positions (List[Dict]): List of position data
            
        Returns:
            Dict: Portfolio performance metrics
        """
        try:
            total_value = sum(p.get('market_value', 0) for p in positions)
            total_cost = sum(p.get('cost_basis', 0) for p in positions)
            
            unrealized_gain_loss = total_value - total_cost
            unrealized_percentage = (unrealized_gain_loss / total_cost * 100) if total_cost > 0 else 0
            
            return {
                'total_market_value': total_value,
                'total_cost_basis': total_cost,
                'unrealized_gain_loss': unrealized_gain_loss,
                'unrealized_percentage': unrealized_percentage,
                'position_count': len(positions)
            }
        except Exception as e:
            logger.error(f"Error calculating portfolio metrics: {str(e)}")
            return {}
    
    def calculate_all_metrics(self, client: str, snapshot_date: str, portfolio_metrics: Dict, positions_data: List[Dict]) -> Dict:
        """
        Calculate comprehensive performance metrics using original ProjectAurum logic.
        
        Args:
            client: Client identifier
            snapshot_date: Date for the snapshot
            portfolio_metrics: Existing portfolio metrics
            positions_data: List of position data
            
        Returns:
            Dict: Complete performance metrics
        """
        try:
            logger.info(f"Calculating all performance metrics for client {client} on {snapshot_date}")
            
            # Use existing portfolio metrics calculation
            basic_metrics = self.calculate_portfolio_metrics(positions_data)
            
            # Calculate additional performance metrics
            total_value = basic_metrics.get('total_market_value', 0)
            total_cost = basic_metrics.get('total_cost_basis', 0)
            unrealized_gain_loss = basic_metrics.get('unrealized_gain_loss', 0)
            
            # Calculate annual income from positions
            total_annual_income = sum(p.get('annual_income', 0) for p in positions_data)
            
            # Calculate real gain/loss (same as unrealized for current period)
            real_gain_loss_dollar = unrealized_gain_loss
            
            # Calculate total value change (for comparison charts)
            total_value_change_dollar = unrealized_gain_loss
            
            performance_metrics = {
                'total_value': total_value,
                'total_cost_basis': total_cost,
                'unrealized_gain_loss': unrealized_gain_loss,
                'real_gain_loss_dollar': real_gain_loss_dollar,
                'total_value_change_dollar': total_value_change_dollar,
                'total_annual_income': total_annual_income,
                'position_count': len(positions_data),
                'calculation_date': snapshot_date,
                'client': client
            }
            
            logger.info(f"Performance metrics calculated successfully for client {client}")
            logger.info(f"  - Total Value: ${total_value:,.2f}")
            logger.info(f"  - Real Gain/Loss: ${real_gain_loss_dollar:,.2f}")
            logger.info(f"  - Annual Income: ${total_annual_income:,.2f}")
            
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Error calculating all performance metrics for {client}: {str(e)}")
            return {
                'total_value': 0,
                'total_cost_basis': 0,
                'unrealized_gain_loss': 0,
                'real_gain_loss_dollar': 0,
                'total_value_change_dollar': 0,
                'total_annual_income': 0,
                'position_count': 0,
                'calculation_date': snapshot_date,
                'client': client,
                'error': str(e)
            }

class CashFlowClassifier:
    """
    Cash flow classification logic for transactions.
    """
    
    def __init__(self):
        """Initialize the cash flow classifier."""
        logger.debug("CashFlowClassifier initialized")
        
    def classify_transaction(self, transaction: Dict) -> str:
        """
        Classify a transaction by cash flow type.
        
        Args:
            transaction (Dict): Transaction data
            
        Returns:
            str: Cash flow classification
        """
        try:
            transaction_type = str(transaction.get('transaction_type', '')).lower()
            
            if 'dividend' in transaction_type:
                return 'Dividend'
            elif 'interest' in transaction_type:
                return 'Interest'
            elif 'buy' in transaction_type or 'purchase' in transaction_type:
                return 'Purchase'
            elif 'sell' in transaction_type or 'sale' in transaction_type:
                return 'Sale'
            elif 'deposit' in transaction_type:
                return 'Deposit'
            elif 'withdrawal' in transaction_type:
                return 'Withdrawal'
            else:
                return 'Other'
                
        except Exception as e:
            logger.error(f"Error classifying transaction: {str(e)}")
            return 'Other'
    
    def is_external_cash_flow(self, transaction: Dict) -> bool:
        """
        Determine if a transaction represents an external cash flow.
        
        Args:
            transaction (Dict): Transaction data
            
        Returns:
            bool: True if transaction is external cash flow
        """
        try:
            transaction_type = str(transaction.get('transaction_type', '')).lower()
            
            # External cash flows are deposits, withdrawals, and other non-investment transactions
            external_types = [
                'deposit', 'withdrawal', 'wire transfer', 'bank deposit', 
                'misc cash entry', 'journal', 'wire sent', 'bill pmt'
            ]
            
            for ext_type in external_types:
                if ext_type in transaction_type:
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Error determining external cash flow: {str(e)}")
            return False
    
    def get_cash_flow_amount(self, transaction: Dict) -> float:
        """
        Get the cash flow amount from a transaction.
        
        Args:
            transaction (Dict): Transaction data
            
        Returns:
            float: Cash flow amount (positive for inflows, negative for outflows)
        """
        try:
            amount = transaction.get('amount', 0)
            transaction_type = str(transaction.get('transaction_type', '')).lower()
            
            # Convert to float
            amount = float(amount or 0)
            
            # Determine sign based on transaction type
            if 'deposit' in transaction_type or 'wire transfer' in transaction_type:
                return abs(amount)  # Positive for deposits
            elif 'withdrawal' in transaction_type or 'wire sent' in transaction_type:
                return -abs(amount)  # Negative for withdrawals
            else:
                # For other types, use the sign as provided
                return amount
                
        except Exception as e:
            logger.error(f"Error getting cash flow amount: {str(e)}")
            return 0.0

class TransactionLinker:
    """
    Links transactions to positions for performance calculations.
    """
    
    def __init__(self):
        """Initialize the transaction linker."""
        logger.debug("TransactionLinker initialized")
        
    def link_transactions_to_positions(self, transactions: List[Dict], 
                                     positions: List[Dict]) -> Dict:
        """
        Link transactions to their corresponding positions.
        
        Args:
            transactions (List[Dict]): List of transactions
            positions (List[Dict]): List of positions
            
        Returns:
            Dict: Linked transaction data
        """
        try:
            linked_data = {}
            
            for position in positions:
                asset_id = position.get('asset_id')
                if asset_id:
                    linked_data[asset_id] = {
                        'position': position,
                        'transactions': []
                    }
            
            for transaction in transactions:
                asset_id = transaction.get('asset_id')
                if asset_id and asset_id in linked_data:
                    linked_data[asset_id]['transactions'].append(transaction)
                    
            return linked_data
            
        except Exception as e:
            logger.error(f"Error linking transactions to positions: {str(e)}")
            return {}

class IncomeAnalyzer:
    """
    Analyzes income from dividends and interest.
    """
    
    def __init__(self):
        """Initialize the income analyzer."""
        logger.debug("IncomeAnalyzer initialized")
        
    def analyze_income(self, transactions: List[Dict]) -> Dict:
        """
        Analyze income from transactions.
        
        Args:
            transactions (List[Dict]): List of transactions
            
        Returns:
            Dict: Income analysis data
        """
        try:
            income_transactions = [
                t for t in transactions 
                if 'dividend' in str(t.get('transaction_type', '')).lower() or
                   'interest' in str(t.get('transaction_type', '')).lower()
            ]
            
            total_income = sum(t.get('amount', 0) for t in income_transactions)
            dividend_income = sum(
                t.get('amount', 0) for t in income_transactions 
                if 'dividend' in str(t.get('transaction_type', '')).lower()
            )
            interest_income = sum(
                t.get('amount', 0) for t in income_transactions 
                if 'interest' in str(t.get('transaction_type', '')).lower()
            )
            
            return {
                'total_income': total_income,
                'dividend_income': dividend_income,
                'interest_income': interest_income,
                'income_transaction_count': len(income_transactions)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing income: {str(e)}")
            return {}

class RiskAnalyzer:
    """
    Risk analysis for portfolios.
    """
    
    def __init__(self):
        """Initialize the risk analyzer."""
        logger.debug("RiskAnalyzer initialized")
        
    def analyze_portfolio_risk(self, positions: List[Dict]) -> Dict:
        """
        Analyze portfolio risk metrics.
        
        Args:
            positions (List[Dict]): List of positions
            
        Returns:
            Dict: Risk analysis data
        """
        try:
            # Basic risk metrics
            total_value = sum(p.get('market_value', 0) for p in positions)
            
            # Asset allocation diversity
            asset_types = {}
            for position in positions:
                asset_type = position.get('asset_type', 'Unknown')
                if asset_type not in asset_types:
                    asset_types[asset_type] = 0
                asset_types[asset_type] += position.get('market_value', 0)
            
            # Calculate concentration risk
            max_allocation = max(asset_types.values()) if asset_types else 0
            concentration_risk = (max_allocation / total_value * 100) if total_value > 0 else 0
            
            return {
                'total_positions': len(positions),
                'asset_type_count': len(asset_types),
                'concentration_risk_percentage': concentration_risk,
                'asset_allocation': asset_types
            }
            
        except Exception as e:
            logger.error(f"Error analyzing portfolio risk: {str(e)}")
            return {}

class InvestmentCashFlowCalculator:
    """
    Investment cash flow calculations with comprehensive bank-specific logic.
    
    This class provides backward compatibility while delegating to the comprehensive
    InvestmentCashFlowService for accurate bank-specific transaction categorization.
    """
    
    def __init__(self):
        """Initialize the investment cash flow calculator."""
        logger.debug("InvestmentCashFlowCalculator initialized")
        # Import here to avoid circular imports
        from ..services.investment_cash_flow_service import InvestmentCashFlowService
        self._service = InvestmentCashFlowService()
        
    def calculate_cash_flows(self, transactions: List[Dict]) -> Dict:
        """
        Calculate investment cash flows from transactions.
        
        Args:
            transactions (List[Dict]): List of transactions
            
        Returns:
            Dict: Cash flow calculation results
        """
        try:
            # Use comprehensive service for accurate calculation
            net_cash_flow = self._service.calculate_investment_cash_flows_from_dicts(transactions)
            
            # For backward compatibility, provide the expected dict structure
            # Note: Individual inflow/outflow counts not available from comprehensive service
            return {
                'total_inflows': max(0, net_cash_flow),
                'total_outflows': max(0, -net_cash_flow),
                'net_cash_flow': net_cash_flow,
                'inflow_count': 1 if net_cash_flow > 0 else 0,
                'outflow_count': 1 if net_cash_flow < 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error calculating cash flows: {str(e)}")
            return {
                'total_inflows': 0.0,
                'total_outflows': 0.0,
                'net_cash_flow': 0.0,
                'inflow_count': 0,
                'outflow_count': 0
            }
    
    def calculate_investment_cash_flows(self, transactions: List[Dict]) -> float:
        """
        Calculate net investment cash flows from transactions.
        This method is called by portfolio_database.py and generate_html_report.py.
        
        Uses comprehensive bank-specific transaction categorization for accurate results.
        Formula: (Dividends + Interest + Other Income) - (Taxes + Fees)
        
        Args:
            transactions (List[Dict]): List of transactions
            
        Returns:
            float: Net cash flow amount (positive for net inflows, negative for net outflows)
        """
        try:
            logger.debug(f"Calculating investment cash flows for {len(transactions)} transactions")
            
            # Delegate to comprehensive service for accurate bank-specific calculation
            net_cash_flow = self._service.calculate_investment_cash_flows_from_dicts(transactions)
            
            logger.debug(f"Investment cash flow calculated: ${net_cash_flow:,.2f}")
            return float(net_cash_flow)
            
        except Exception as e:
            logger.error(f"Error calculating investment cash flows: {str(e)}")
            return 0.0

class ChartCalculator:
    """
    Chart data calculations for dashboard displays.
    """
    
    def __init__(self):
        """Initialize the chart calculator."""
        logger.debug("ChartCalculator initialized")
        
    def calculate_allocation_chart(self, positions: List[Dict]) -> Dict:
        """
        Calculate data for allocation charts.
        
        Args:
            positions (List[Dict]): List of positions
            
        Returns:
            Dict: Chart data for allocation
        """
        try:
            allocation_data = {}
            total_value = 0
            
            for position in positions:
                asset_type = position.get('asset_type', 'Unknown')
                market_value = position.get('market_value', 0)
                
                if asset_type not in allocation_data:
                    allocation_data[asset_type] = 0
                allocation_data[asset_type] += market_value
                total_value += market_value
            
            # Convert to percentages
            percentages = {}
            for asset_type, value in allocation_data.items():
                percentages[asset_type] = (value / total_value * 100) if total_value > 0 else 0
            
            return {
                'labels': list(allocation_data.keys()),
                'values': list(allocation_data.values()),
                'percentages': list(percentages.values()),
                'total_value': total_value
            }
            
        except Exception as e:
            logger.error(f"Error calculating allocation chart: {str(e)}")
            return {}
    
    def calculate_all_chart_data(self, client: str, snapshot_date: str, portfolio_metrics: Dict, positions_data: List[Dict], include_current_date: bool = True) -> Dict:
        """
        Calculate all chart data for dashboard displays using original ProjectAurum logic.
        
        Args:
            client: Client identifier
            snapshot_date: Date for the snapshot
            portfolio_metrics: Portfolio metrics data
            positions_data: List of position data
            include_current_date: Whether to include current date in calculations
            
        Returns:
            Dict: Complete chart data for all dashboard charts
        """
        try:
            logger.info(f"Calculating all chart data for client {client} on {snapshot_date}")
            
            # 1. Asset Allocation Chart Data
            asset_allocation_data = self._create_asset_allocation_data(positions_data)
            
            # 2. Portfolio Comparison Chart Data
            portfolio_comparison_data = self._create_portfolio_comparison_data(portfolio_metrics, client, snapshot_date)
            
            # 3. Cumulative Return Chart Data
            cumulative_return_data = self._create_cumulative_return_data(client, snapshot_date)
            
            # 4. Portfolio History Chart Data
            portfolio_history_data = self._create_portfolio_history_data(client, snapshot_date)
            
            chart_data = {
                'asset_allocation': asset_allocation_data,
                'portfolio_comparison': portfolio_comparison_data,
                'cumulative_return': cumulative_return_data,
                'portfolio_history': portfolio_history_data,
                'calculation_date': snapshot_date,
                'client': client,
                'hasData': True
            }
            
            logger.info(f"Chart data calculated successfully for client {client}")
            return chart_data
            
        except Exception as e:
            logger.error(f"Error calculating all chart data for {client}: {str(e)}")
            # Return fallback data structure
            return {
                'asset_allocation': {'hasData': False, 'message': 'Asset allocation data unavailable'},
                'portfolio_comparison': {'hasData': False, 'message': 'Portfolio comparison data unavailable'},
                'cumulative_return': {'hasData': False, 'message': 'Cumulative return data unavailable'},
                'portfolio_history': {'hasData': False, 'message': 'Portfolio history data unavailable'},
                'calculation_date': snapshot_date,
                'client': client,
                'hasData': False,
                'error': str(e)
            }
    
    def _create_asset_allocation_data(self, positions_data: List[Dict]) -> Dict:
        """
        Create asset allocation chart data using original ProjectAurum logic.
        
        Args:
            positions_data: List of position data
            
        Returns:
            Dict: Asset allocation chart data for ApexCharts
        """
        try:
            if not positions_data:
                return {'hasData': False, 'message': 'No position data available'}
            
            # Use original ProjectAurum asset allocation logic
            allocation_data = {}
            total_value = 0
            
            # Group by asset type (same logic as original)
            for position in positions_data:
                asset_type = normalize_asset_type(position.get('asset_type', 'Unknown'))
                market_value = position.get('market_value', 0)
                
                if asset_type not in allocation_data:
                    allocation_data[asset_type] = 0
                allocation_data[asset_type] += market_value
                total_value += market_value
            
            if total_value == 0:
                return {'hasData': False, 'message': 'No market value data available'}
            
            # Convert to ApexCharts format
            labels = []
            series = []  # Percentage values
            monetary_values = []
            
            # Sort by market value (largest first)
            sorted_allocation = sorted(allocation_data.items(), key=lambda x: x[1], reverse=True)
            
            for asset_type, market_value in sorted_allocation:
                if market_value > 0:
                    percentage = (market_value / total_value) * 100
                    labels.append(asset_type)
                    series.append(round(percentage, 1))
                    monetary_values.append(market_value)
            
            return {
                'chart_type': 'donut',
                'series': series,
                'labels': labels,
                'colors': ['#5f76a1', '#072061', '#dae1f3', '#b7babe', '#8aa5c6', '#4e6b8f'],
                'total_value': total_value,
                'monetary_values': monetary_values,
                'hasData': True
            }
            
        except Exception as e:
            logger.error(f"Error creating asset allocation data: {str(e)}")
            return {'hasData': False, 'message': f'Error: {str(e)}'}
    
    def _create_portfolio_comparison_data(self, portfolio_metrics: Dict, client: str, snapshot_date: str) -> Dict:
        """
        Create portfolio comparison chart data.
        
        Args:
            portfolio_metrics: Portfolio metrics data
            client: Client identifier
            snapshot_date: Current snapshot date
            
        Returns:
            Dict: Portfolio comparison chart data for ApexCharts
        """
        try:
            # Get current metrics
            performance_metrics = portfolio_metrics.get('performance_metrics', {})
            current_value = performance_metrics.get('total_value', 0)
            real_gain_loss = performance_metrics.get('real_gain_loss_dollar', 0)
            cash_flow = portfolio_metrics.get('cash_flow', 0)
            annual_income = performance_metrics.get('total_annual_income', 0)
            
            # Create comparison data
            metrics = ['Total Value Change', 'Real Gain/Loss', 'Net Cash Flow', 'Est. Annual Income']
            values = [current_value, real_gain_loss, cash_flow, annual_income]
            
            return {
                'series': [{
                    'name': 'Portfolio Metrics',
                    'data': values
                }],
                'categories': metrics,
                'colors': ['#5f76a1', '#b7babe'],
                'hasData': True
            }
            
        except Exception as e:
            logger.error(f"Error creating portfolio comparison data: {str(e)}")
            return {'hasData': False, 'message': f'Error: {str(e)}'}
    
    def _create_cumulative_return_data(self, client: str, snapshot_date: str) -> Dict:
        """
        Create cumulative return chart data.
        
        Args:
            client: Client identifier
            snapshot_date: Current snapshot date
            
        Returns:
            Dict: Cumulative return chart data for ApexCharts
        """
        try:
            # For now, return basic structure - would need historical return data
            return {
                'series': [{
                    'name': 'Cumulative Return (%)',
                    'data': [0]
                }],
                'categories': [snapshot_date],
                'colors': ['#072061'],
                'hasData': False,
                'message': 'Historical return data not yet available'
            }
            
        except Exception as e:
            logger.error(f"Error creating cumulative return data: {str(e)}")
            return {'hasData': False, 'message': f'Error: {str(e)}'}
    
    def _create_portfolio_history_data(self, client: str, snapshot_date: str) -> Dict:
        """
        Create portfolio history chart data.
        
        Args:
            client: Client identifier
            snapshot_date: Current snapshot date
            
        Returns:
            Dict: Portfolio history chart data for ApexCharts
        """
        try:
            # For now, return basic structure - would need historical portfolio values
            return {
                'chart_type': 'line',
                'series': [{
                    'name': 'Portfolio Value',
                    'data': [0]
                }],
                'categories': [snapshot_date],
                'colors': ['#5f76a1'],
                'hasData': False,
                'message': 'Historical portfolio data not yet available'
            }
            
        except Exception as e:
            logger.error(f"Error creating portfolio history data: {str(e)}")
            return {'hasData': False, 'message': f'Error: {str(e)}'}