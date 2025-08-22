"""
Test suite for AurumFinance business logic validation.
Tests calculation helpers, services, and data processing logic.
"""

import unittest
from decimal import Decimal
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock

from django.test import TestCase

# Import business logic components
from ..business_logic.calculation_helpers import (
    normalize_asset_type,
    calculate_safe_unrealized_gain_loss,
    calculate_safe_gain_loss_percentage,
    ModifiedDietzCalculator,
    PerformanceCalculator,
    CashFlowClassifier,
    InvestmentCashFlowCalculator,
    ChartCalculator
)


class TestAssetTypeNormalization(TestCase):
    """Test asset type normalization logic."""
    
    def test_normalize_equity_types(self):
        """Test equity type normalization."""
        equity_inputs = ['equity', 'EQUITY', 'stock', 'Stock', 'STOCK']
        for input_type in equity_inputs:
            with self.subTest(input_type=input_type):
                self.assertEqual(normalize_asset_type(input_type), 'Equity')
    
    def test_normalize_bond_types(self):
        """Test bond/fixed income type normalization."""
        bond_inputs = ['bond', 'BOND', 'fixed income', 'Fixed Income', 'FIXED INCOME']
        for input_type in bond_inputs:
            with self.subTest(input_type=input_type):
                self.assertEqual(normalize_asset_type(input_type), 'Bond')
    
    def test_normalize_cash_types(self):
        """Test cash type normalization."""
        cash_inputs = ['cash', 'CASH', 'money market', 'Money Market', 'MONEY MARKET']
        for input_type in cash_inputs:
            with self.subTest(input_type=input_type):
                expected = 'Cash'
                self.assertEqual(normalize_asset_type(input_type), expected)
    
    def test_normalize_unknown_types(self):
        """Test handling of unknown asset types."""
        unknown_inputs = ['cryptocurrency', 'real estate', 'art']
        for input_type in unknown_inputs:
            with self.subTest(input_type=input_type):
                # Should return title case of input
                expected = input_type.title()
                self.assertEqual(normalize_asset_type(input_type), expected)
    
    def test_normalize_empty_types(self):
        """Test handling of empty or None asset types."""
        empty_inputs = [None, '', '   ']
        for input_type in empty_inputs:
            with self.subTest(input_type=input_type):
                self.assertEqual(normalize_asset_type(input_type), 'Unknown')


class TestSafeCalculations(TestCase):
    """Test safe calculation functions with error handling."""
    
    def test_safe_unrealized_gain_loss(self):
        """Test unrealized gain/loss calculation."""
        # Normal case
        self.assertEqual(calculate_safe_unrealized_gain_loss(110.0, 100.0), 10.0)
        self.assertEqual(calculate_safe_unrealized_gain_loss(90.0, 100.0), -10.0)
        
        # Zero cost basis
        self.assertEqual(calculate_safe_unrealized_gain_loss(110.0, 0.0), 110.0)
        
        # None values
        self.assertEqual(calculate_safe_unrealized_gain_loss(None, 100.0), -100.0)
        self.assertEqual(calculate_safe_unrealized_gain_loss(110.0, None), 110.0)
        self.assertEqual(calculate_safe_unrealized_gain_loss(None, None), 0.0)
        
        # String values (should convert)
        self.assertEqual(calculate_safe_unrealized_gain_loss('110.0', '100.0'), 10.0)
    
    def test_safe_gain_loss_percentage(self):
        """Test gain/loss percentage calculation."""
        # Normal case
        self.assertEqual(calculate_safe_gain_loss_percentage(110.0, 100.0), 10.0)
        self.assertEqual(calculate_safe_gain_loss_percentage(90.0, 100.0), -10.0)
        
        # Zero cost basis
        self.assertEqual(calculate_safe_gain_loss_percentage(110.0, 0.0), 0.0)
        
        # None values
        self.assertEqual(calculate_safe_gain_loss_percentage(None, 100.0), -100.0)
        self.assertEqual(calculate_safe_gain_loss_percentage(None, None), 0.0)


class TestModifiedDietzCalculator(TestCase):
    """Test Modified Dietz return calculations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.calculator = ModifiedDietzCalculator()
    
    def test_basic_dietz_calculation(self):
        """Test basic Modified Dietz calculation."""
        beginning_value = 100000.0
        ending_value = 110000.0
        cash_flows = [(datetime(2024, 6, 15), 5000.0)]  # Mid-year contribution
        
        result = self.calculator.calculate_return(beginning_value, ending_value, cash_flows)
        
        # Should be positive return
        self.assertGreater(result, 0.0)
        self.assertIsInstance(result, float)
    
    def test_no_cash_flows(self):
        """Test calculation with no cash flows."""
        beginning_value = 100000.0
        ending_value = 110000.0
        cash_flows = []
        
        result = self.calculator.calculate_return(beginning_value, ending_value, cash_flows)
        
        # Should be 10% return
        self.assertAlmostEqual(result, 10.0, places=2)
    
    def test_zero_beginning_value(self):
        """Test calculation with zero beginning value."""
        beginning_value = 0.0
        ending_value = 110000.0
        cash_flows = [(datetime(2024, 1, 1), 100000.0)]
        
        result = self.calculator.calculate_return(beginning_value, ending_value, cash_flows)
        
        # Should return 0 for zero beginning value
        self.assertEqual(result, 0.0)


class TestCashFlowClassifier(TestCase):
    """Test cash flow classification logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.classifier = CashFlowClassifier()
    
    def test_classify_dividend_transactions(self):
        """Test dividend transaction classification."""
        dividend_transactions = [
            {'transaction_type': 'dividend'},
            {'transaction_type': 'DIVIDEND PAYMENT'},
            {'transaction_type': 'Dividend Received'}
        ]
        
        for transaction in dividend_transactions:
            with self.subTest(transaction=transaction):
                result = self.classifier.classify_transaction(transaction)
                self.assertEqual(result, 'Dividend')
    
    def test_classify_purchase_transactions(self):
        """Test purchase transaction classification."""
        purchase_transactions = [
            {'transaction_type': 'buy'},
            {'transaction_type': 'BUY ORDER'},
            {'transaction_type': 'purchase'},
            {'transaction_type': 'Stock Purchase'}
        ]
        
        for transaction in purchase_transactions:
            with self.subTest(transaction=transaction):
                result = self.classifier.classify_transaction(transaction)
                self.assertEqual(result, 'Purchase')
    
    def test_external_cash_flow_detection(self):
        """Test external cash flow detection."""
        external_transactions = [
            {'transaction_type': 'deposit'},
            {'transaction_type': 'WIRE TRANSFER'},
            {'transaction_type': 'Bank Deposit'},
            {'transaction_type': 'withdrawal'},
            {'transaction_type': 'Wire Sent'}
        ]
        
        for transaction in external_transactions:
            with self.subTest(transaction=transaction):
                result = self.classifier.is_external_cash_flow(transaction)
                self.assertTrue(result)
    
    def test_internal_cash_flow_detection(self):
        """Test that investment transactions are not external cash flows."""
        internal_transactions = [
            {'transaction_type': 'buy'},
            {'transaction_type': 'sell'},
            {'transaction_type': 'dividend'},
            {'transaction_type': 'interest'}
        ]
        
        for transaction in internal_transactions:
            with self.subTest(transaction=transaction):
                result = self.classifier.is_external_cash_flow(transaction)
                self.assertFalse(result)
    
    def test_get_cash_flow_amount(self):
        """Test cash flow amount calculation."""
        # Deposit (positive)
        deposit = {'transaction_type': 'deposit', 'amount': 1000.0}
        self.assertEqual(self.classifier.get_cash_flow_amount(deposit), 1000.0)
        
        # Withdrawal (negative)
        withdrawal = {'transaction_type': 'withdrawal', 'amount': 500.0}
        self.assertEqual(self.classifier.get_cash_flow_amount(withdrawal), -500.0)
        
        # Other transaction (preserve sign)
        other = {'transaction_type': 'other', 'amount': -200.0}
        self.assertEqual(self.classifier.get_cash_flow_amount(other), -200.0)


class TestPerformanceCalculator(TestCase):
    """Test portfolio performance calculations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.calculator = PerformanceCalculator()
    
    def test_calculate_portfolio_metrics(self):
        """Test portfolio metrics calculation."""
        positions = [
            {'market_value': 50000.0, 'cost_basis': 45000.0},
            {'market_value': 30000.0, 'cost_basis': 35000.0},
            {'market_value': 20000.0, 'cost_basis': 20000.0}
        ]
        
        result = self.calculator.calculate_portfolio_metrics(positions)
        
        self.assertEqual(result['total_market_value'], 100000.0)
        self.assertEqual(result['total_cost_basis'], 100000.0)
        self.assertEqual(result['unrealized_gain_loss'], 0.0)
        self.assertEqual(result['unrealized_percentage'], 0.0)
        self.assertEqual(result['position_count'], 3)
    
    def test_calculate_all_metrics(self):
        """Test comprehensive metrics calculation."""
        positions = [
            {'market_value': 55000.0, 'cost_basis': 50000.0, 'annual_income': 1000.0},
            {'market_value': 45000.0, 'cost_basis': 50000.0, 'annual_income': 500.0}
        ]
        
        result = self.calculator.calculate_all_metrics(
            client='TEST',
            snapshot_date='2024-07-22',
            portfolio_metrics={},
            positions_data=positions
        )
        
        self.assertEqual(result['client'], 'TEST')
        self.assertEqual(result['calculation_date'], '2024-07-22')
        self.assertEqual(result['total_value'], 100000.0)
        self.assertEqual(result['total_annual_income'], 1500.0)
        self.assertEqual(result['position_count'], 2)


class TestInvestmentCashFlowCalculator(TestCase):
    """Test investment cash flow calculations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.calculator = InvestmentCashFlowCalculator()
    
    def test_calculate_cash_flows(self):
        """Test cash flow calculation from transactions."""
        transactions = [
            {'transaction_type': 'buy', 'amount': 10000.0},
            {'transaction_type': 'sell', 'amount': 5000.0},
            {'transaction_type': 'dividend', 'amount': 200.0},
            {'transaction_type': 'interest', 'amount': 100.0}
        ]
        
        result = self.calculator.calculate_cash_flows(transactions)
        
        # With comprehensive logic: buy/sell are excluded (trading), only dividend/interest count
        # Net investment cash flow = 200 + 100 = 300
        self.assertEqual(result['net_cash_flow'], 300.0)
        self.assertEqual(result['total_inflows'], 300.0)  # Only dividend + interest income
        self.assertEqual(result['total_outflows'], 0.0)   # No fees in this test
    
    def test_calculate_investment_cash_flows(self):
        """Test investment-specific cash flow calculation."""
        transactions = [
            {'transaction_type': 'purchase', 'amount': 8000.0},
            {'transaction_type': 'sale', 'amount': 3000.0},
            {'transaction_type': 'dividend', 'amount': 150.0}
        ]
        
        result = self.calculator.calculate_investment_cash_flows(transactions)
        
        # Should return net cash flow as float
        self.assertIsInstance(result, float)
        # With comprehensive logic: purchase/sale are excluded (trading activity)
        # Only dividend counts as investment income = 150.0
        self.assertEqual(result, 150.0)


class TestChartCalculator(TestCase):
    """Test chart data calculations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.calculator = ChartCalculator()
    
    def test_calculate_allocation_chart(self):
        """Test asset allocation chart calculation."""
        positions = [
            {'asset_type': 'Equity', 'market_value': 60000.0},
            {'asset_type': 'Bond', 'market_value': 30000.0},
            {'asset_type': 'Cash', 'market_value': 10000.0}
        ]
        
        result = self.calculator.calculate_allocation_chart(positions)
        
        self.assertEqual(result['total_value'], 100000.0)
        self.assertEqual(len(result['labels']), 3)
        self.assertEqual(len(result['values']), 3)
        self.assertEqual(len(result['percentages']), 3)
        
        # Check percentages add up to 100
        self.assertAlmostEqual(sum(result['percentages']), 100.0, places=1)
    
    def test_calculate_all_chart_data(self):
        """Test comprehensive chart data calculation."""
        positions = [
            {'asset_type': 'Equity', 'market_value': 70000.0},
            {'asset_type': 'Bond', 'market_value': 30000.0}
        ]
        
        portfolio_metrics = {
            'performance_metrics': {
                'total_value': 100000.0,
                'real_gain_loss_dollar': 5000.0,
                'total_annual_income': 2000.0
            },
            'cash_flow': 1000.0
        }
        
        result = self.calculator.calculate_all_chart_data(
            client='TEST',
            snapshot_date='2024-07-22',
            portfolio_metrics=portfolio_metrics,
            positions_data=positions
        )
        
        self.assertEqual(result['client'], 'TEST')
        self.assertEqual(result['calculation_date'], '2024-07-22')
        self.assertTrue(result['hasData'])
        self.assertIn('asset_allocation', result)
        self.assertIn('portfolio_comparison', result)
        self.assertIn('cumulative_return', result)
        self.assertIn('portfolio_history', result)


if __name__ == '__main__':
    unittest.main()