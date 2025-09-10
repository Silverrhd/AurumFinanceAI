"""
Enhanced Report Generation Service
Provides ProjectAurum-compatible position tables, transaction tables, and ApexCharts data generation.
Builds on DjangoReportService with exact template structure matching.
"""

from decimal import Decimal
from django.db.models import Q, Sum, Count
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from ..models import Client, PortfolioSnapshot, Position, Transaction, Asset
from jinja2 import Environment, FileSystemLoader
from django.conf import settings
import os

logger = logging.getLogger(__name__)

class EnhancedReportService:
    """Enhanced report service with exact ProjectAurum position/transaction table generation."""
    
    def __init__(self):
        self._setup_jinja2()
        # Initialize Modified Dietz service
        from .modified_dietz_service import ModifiedDietzService
        self.dietz_service = ModifiedDietzService()
        
        # Initialize Investment Cash Flow service for weekly comparison
        from .investment_cash_flow_service import InvestmentCashFlowService
        self.investment_cash_flow_service = InvestmentCashFlowService()
        
        # Initialize Benchmark service for benchmark comparison
        from .benchmark_service import BenchmarkService
        self.benchmark_service = BenchmarkService()
    
    def _setup_jinja2(self):
        """Setup Jinja2 environment with custom filters."""
        template_dir = os.path.join(settings.BASE_DIR, 'templates')
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir))
        
        # Register custom filters - Django-native versions
        self.jinja_env.filters['format_currency'] = self._format_currency
        self.jinja_env.filters['format_percentage'] = self._format_percentage
        self.jinja_env.filters['format_number'] = self._format_number
    
    def _format_currency(self, value):
        """Format value as currency."""
        try:
            return f"${float(value):,.2f}"
        except (ValueError, TypeError):
            return "$0.00"
    
    def _format_percentage(self, value):
        """Format value as percentage."""
        try:
            return f"{float(value):.2f}%"
        except (ValueError, TypeError):
            return "0.00%"
    
    def _format_number(self, value):
        """Format value as number with commas."""
        try:
            return f"{float(value):,.2f}"
        except (ValueError, TypeError):
            return "0.00"
    
    def generate_weekly_report(self, client_code: str, current_date: str, 
                             comparison_date: str = None) -> str:
        """
        Generate enhanced weekly report with exact ProjectAurum position/transaction tables.
        """
        logger.info(f"Generating enhanced report for {client_code}: {comparison_date} -> {current_date}")
        
        try:
            # Get client and snapshots
            client = Client.objects.get(code=client_code)
            current_snapshot = PortfolioSnapshot.objects.get(
                client=client, snapshot_date=current_date
            )
            
            # Get comparison snapshot (automatically find previous if none specified)
            if comparison_date and comparison_date != current_date:
                # Explicit comparison date provided
                try:
                    comparison_snapshot = PortfolioSnapshot.objects.get(
                        client=client, snapshot_date=comparison_date
                    )
                except PortfolioSnapshot.DoesNotExist:
                    logger.warning(f"Comparison snapshot not found for {comparison_date}, using current")
                    comparison_snapshot = current_snapshot
                    comparison_date = current_date
            else:
                # No comparison date provided - automatically find previous snapshot
                previous_snapshot = PortfolioSnapshot.objects.filter(
                    client=client,
                    snapshot_date__lt=current_date
                ).order_by('-snapshot_date').first()
                
                if previous_snapshot:
                    comparison_snapshot = previous_snapshot
                    comparison_date = str(previous_snapshot.snapshot_date)
                    logger.info(f"Auto-detected comparison date: {comparison_date} -> {current_date}")
                else:
                    # This is the first snapshot - no comparison possible
                    comparison_snapshot = current_snapshot
                    comparison_date = current_date
                    logger.info(f"First snapshot for {client.code} - no comparison available")
            
            # Get enhanced metrics
            current_metrics = self._calculate_enhanced_metrics(current_snapshot)
            comparison_metrics = self._calculate_enhanced_metrics(comparison_snapshot) if comparison_snapshot != current_snapshot else current_metrics
            
            # Calculate biggest movers (ProjectAurum algorithm)
            biggest_movers = self._calculate_biggest_movers_fixed(current_snapshot, comparison_snapshot)
            current_metrics['biggest_movers'] = biggest_movers
            print(f"DEBUG: Setting biggest_movers to {len(biggest_movers)} items: {biggest_movers[:2] if biggest_movers else 'EMPTY'}")
            
            # Generate position tables (exact ProjectAurum format)
            position_tables = self._generate_position_tables(current_snapshot)
            
            # Generate transaction tables HTML (conditional rendering)
            transaction_tables_html = self._generate_transaction_tables_html(client, current_date, comparison_date)
            
            # Generate ApexCharts data
            charts_data = self._generate_charts_data(client, current_snapshot, comparison_snapshot, current_date)
            
            # Prepare enhanced template context
            context = self._prepare_enhanced_template_context(
                client, current_date, comparison_date,
                current_metrics, comparison_metrics,
                position_tables, transaction_tables_html, charts_data
            )
            
            # Render template using Jinja2
            try:
                template = self.jinja_env.get_template('report_template.html')
                html_content = template.render(context)
            except Exception as e:
                logger.error(f"Template rendering error: {e}")
                raise ValueError(f"Failed to render enhanced report template: {str(e)}")
            
            logger.info(f"Enhanced report generated successfully for {client_code}")
            return html_content
            
        except Exception as e:
            logger.error(f"Error generating enhanced report for {client_code}: {e}")
            raise
    
    def _calculate_enhanced_metrics(self, snapshot: PortfolioSnapshot) -> dict:
        """
        Calculate enhanced metrics including position tables and ApexCharts data.
        """
        positions = snapshot.positions.select_related('asset').exclude(asset__bank='ALT').all()
        
        # Basic calculations
        total_value = sum(pos.market_value for pos in positions)
        total_cost_basis = sum(pos.cost_basis for pos in positions)
        unrealized_gain_loss = total_value - total_cost_basis
        unrealized_gain_loss_pct = (unrealized_gain_loss / total_cost_basis * 100) if total_cost_basis > 0 else 0
        
        # Annual income calculation
        estimated_annual_income = sum(pos.estimated_annual_income or Decimal('0') for pos in positions)
        annual_income_yield = (estimated_annual_income / total_value * 100) if total_value > 0 else 0
        
        # Asset allocation
        asset_allocation = self._calculate_asset_allocation(positions)
        
        # Custody allocation
        custody_allocation = self._calculate_custody_allocation(positions)
        
        # Position count by type
        positions_by_type = self._group_positions_by_type(positions)
        
        # Bond maturity analysis
        bond_maturity = self._calculate_bond_maturity_timeline(positions)
        
        # Top movers - will be calculated separately with comparison data
        top_movers = {'gainers': [], 'losers': []}
        
        # Calculate period performance using Modified Dietz
        period_performance = self._calculate_period_performance(snapshot)
        
        # Calculate since inception performance using Modified Dietz
        inception_performance = self._calculate_since_inception_performance(snapshot)
        
        return {
            'total_value': float(total_value),
            'total_cost_basis': float(total_cost_basis),
            'unrealized_gain_loss': float(unrealized_gain_loss),
            'unrealized_gain_loss_pct': float(unrealized_gain_loss_pct),
            'estimated_annual_income': float(estimated_annual_income),
            'annual_income_yield': float(annual_income_yield),
            'position_count': len(positions),
            'asset_allocation': asset_allocation,
            'custody_allocation': custody_allocation,
            'positions_by_type': positions_by_type,
            'bond_maturity': bond_maturity,
            'top_movers': top_movers,
            'real_gain_loss_dollar': period_performance['period_dollar'],
            'real_gain_loss_percent': period_performance['period_percent'],
            'inception_gain_loss_dollar': inception_performance['inception_dollar'],
            'inception_gain_loss_percent': inception_performance['inception_percent'],
            'net_cash_flow': 0,  # Placeholder - needs transaction analysis
        }
    
    def _generate_position_tables(self, snapshot: PortfolioSnapshot) -> dict:
        """
        Generate exact ProjectAurum position tables with 10 columns:
        custody, name, ticker, quantity, market_value, cost_basis, 
        unrealized_gain, unrealized_gain_pct, coupon_rate, annual_income
        """
        positions = snapshot.positions.select_related('asset').exclude(asset__bank='ALT').all()
        
        # Group positions by asset type
        grouped_positions = defaultdict(list)
        
        for position in positions:
            asset_type = position.asset.asset_type
            
            # Calculate unrealized gain
            unrealized_gain = position.market_value - position.cost_basis
            unrealized_gain_pct = ((position.market_value - position.cost_basis) / position.cost_basis * 100) if position.cost_basis > 0 else 0
            
            # Get coupon rate (position overrides asset)
            coupon_rate = position.coupon_rate or position.asset.coupon_rate or 0
            
            # Annual income
            annual_income = position.estimated_annual_income or Decimal('0')
            
            position_data = {
                'custody': f"{position.bank}/{position.account}" if position.bank else "Unknown",
                'name': position.asset.name,
                'ticker': position.asset.ticker,
                'quantity': float(position.quantity),
                'market_value': float(position.market_value),
                'cost_basis': float(position.cost_basis),
                'unrealized_gain': float(unrealized_gain),
                'unrealized_gain_pct': float(unrealized_gain_pct),
                'coupon_rate': float(coupon_rate),
                'annual_income': float(annual_income)
            }
            
            # Map normalized asset types to ProjectAurum categories
            if asset_type == 'Fixed Income':
                grouped_positions['bonds'].append(position_data)
            elif asset_type == 'Equities':
                grouped_positions['equities'].append(position_data)
            elif asset_type in ['Cash', 'Money Market']:
                grouped_positions['cash'].append(position_data)
            else:
                grouped_positions['other'].append(position_data)
        
        # Sort each group by market_value descending
        for asset_type in grouped_positions:
            grouped_positions[asset_type].sort(key=lambda x: x['market_value'], reverse=True)
        
        return dict(grouped_positions)
    
    def _generate_position_tables_html(self, snapshot: PortfolioSnapshot) -> str:
        """Generate HTML tables for positions instead of dict."""
        from collections import defaultdict
        
        positions = snapshot.positions.select_related('asset').exclude(asset__bank='ALT').all()
        
        # Group positions by asset type
        grouped_positions = defaultdict(list)
        
        for position in positions:
            # Calculate unrealized gain
            unrealized_gain = position.market_value - position.cost_basis
            unrealized_gain_pct = ((position.market_value - position.cost_basis) / position.cost_basis * 100) if position.cost_basis > 0 else 0
            
            # Get coupon rate (position overrides asset)
            coupon_rate = position.coupon_rate or position.asset.coupon_rate or 0
            
            # Annual income
            annual_income = position.estimated_annual_income or 0
            
            # Custody format: "Account Bank"
            custody = f"{position.account} {position.bank}" if position.account and position.bank else "Unknown"
            
            position_data = {
                'custody': custody,
                'name': position.asset.name,
                'ticker': position.asset.ticker,
                'quantity': float(position.quantity),
                'market_value': float(position.market_value),
                'cost_basis': float(position.cost_basis),
                'unrealized_gain': float(unrealized_gain),
                'unrealized_gain_pct': float(unrealized_gain_pct),
                'coupon_rate': float(coupon_rate),
                'annual_income': float(annual_income)
            }
            
            # Map asset types to ProjectAurum categories using exact database values
            asset_type = position.asset.asset_type  # Don't convert to uppercase
            if asset_type in ['Fixed Income', 'Bond', 'Treasury', 'Corporate Bond']:
                grouped_positions['Fixed Income'].append(position_data)
            elif asset_type in ['Equities', 'Equity', 'Stock', 'Common Stock']:
                grouped_positions['Equities'].append(position_data)
            elif asset_type in ['Cash', 'Money Market']:
                grouped_positions['Cash/Money Market'].append(position_data)
            else:
                grouped_positions['Alternatives'].append(position_data)
        
        # Sort each group by market_value descending
        for asset_type in grouped_positions:
            grouped_positions[asset_type].sort(key=lambda x: x['market_value'], reverse=True)
        
        # Generate HTML sections in desired order
        html_sections = []
        
        # Define desired section order
        section_order = [
            'Cash/Money Market',
            'Fixed Income', 
            'Equities',
            'Alternatives'
        ]
        
        for asset_type in section_order:
            positions_list = grouped_positions.get(asset_type, [])
            if not positions_list:
                continue
                
            html_sections.append(f'<h3>{asset_type}</h3>')
            html_sections.append('<div class="position-table-container">')
            html_sections.append('<table class="position-table">')
            html_sections.append('''
                <thead>
                    <tr>
                        <th class="col-custody">Custody</th>
                        <th class="col-name">Name</th>
                        <th class="col-ticker">Ticker</th>
                        <th class="col-quantity">Quantity</th>
                        <th class="col-market-value">Market Value</th>
                        <th class="col-cost-basis">Cost Basis</th>
                        <th class="col-unrealized-gain">Unrealized Gain</th>
                        <th class="col-unrealized-gain-pct">Unrealized Gain %</th>
                        <th class="col-coupon-rate">Coupon Rate</th>
                        <th class="col-annual-income">Annual Income</th>
                    </tr>
                </thead>
                <tbody>
            ''')
            
            # Calculate subtotal for this asset type
            subtotal_market_value = sum(p['market_value'] for p in positions_list)
            subtotal_cost_basis = sum(p['cost_basis'] for p in positions_list)
            subtotal_unrealized_gain = sum(p['unrealized_gain'] for p in positions_list)
            subtotal_annual_income = sum(p['annual_income'] for p in positions_list)
            
            # Calculate subtotal unrealized gain percentage
            if subtotal_cost_basis > 0:
                subtotal_unrealized_gain_pct = (subtotal_unrealized_gain / subtotal_cost_basis) * 100
                # Apply same color logic as individual positions
                if subtotal_unrealized_gain_pct > 0:
                    subtotal_gain_class = 'positive'
                elif subtotal_unrealized_gain_pct < 0:
                    subtotal_gain_class = 'negative'
                else:
                    subtotal_gain_class = ''  # 0.00% = default black
                subtotal_pct_display = f'{subtotal_unrealized_gain_pct:.2f}%'
            else:
                subtotal_gain_class = ''
                subtotal_pct_display = '-'
            
            for position in positions_list:
                # Only color non-zero percentages: positive = green, negative = red, zero = default black
                if position['unrealized_gain_pct'] > 0:
                    gain_class = 'positive'
                elif position['unrealized_gain_pct'] < 0:
                    gain_class = 'negative'
                else:
                    gain_class = ''  # No class for 0.00% - default black color
                html_sections.append(f'''
                    <tr>
                        <td class="col-custody">{position['custody']}</td>
                        <td class="col-name">{position['name']}</td>
                        <td class="col-ticker">{position['ticker']}</td>
                        <td class="numeric">{position['quantity']:,.2f}</td>
                        <td class="numeric">${position['market_value']:,.2f}</td>
                        <td class="numeric">${position['cost_basis']:,.2f}</td>
                        <td class="numeric">${position['unrealized_gain']:,.2f}</td>
                        <td class="numeric {gain_class}">{position['unrealized_gain_pct']:.2f}%</td>
                        <td class="numeric">{position['coupon_rate']:.2f}%</td>
                        <td class="numeric">${position['annual_income']:,.2f}</td>
                    </tr>
                ''')
            
            # Add subtotal row
            html_sections.append(f'''
                <tr class="subtotal-row">
                    <td class="col-custody"></td>
                    <td class="col-name">Subtotal</td>
                    <td class="col-ticker"></td>
                    <td class="numeric">-</td>
                    <td class="numeric">${subtotal_market_value:,.2f}</td>
                    <td class="numeric">${subtotal_cost_basis:,.2f}</td>
                    <td class="numeric">${subtotal_unrealized_gain:,.2f}</td>
                    <td class="numeric {subtotal_gain_class}">{subtotal_pct_display}</td>
                    <td class="numeric">-</td>
                    <td class="numeric">${subtotal_annual_income:,.2f}</td>
                </tr>
            ''')
            
            html_sections.append('</tbody></table></div>')
        
        return ''.join(html_sections)
    
    def _calculate_period_performance(self, snapshot: PortfolioSnapshot) -> dict:
        """Calculate period performance using Modified Dietz."""
        # Get previous snapshot for this client
        previous_snapshot = PortfolioSnapshot.objects.filter(
            client=snapshot.client,
            snapshot_date__lt=snapshot.snapshot_date
        ).order_by('-snapshot_date').first()
        
        if not previous_snapshot:
            logger.debug(f"No previous snapshot found for {snapshot.client.code} before {snapshot.snapshot_date}")
            return {'period_dollar': 0, 'period_percent': 0}
        
        try:
            # Use Modified Dietz service for accurate period return and detailed calculation
            detailed_result = self.dietz_service.calculate_portfolio_return_detailed(
                snapshot.client.code, 
                previous_snapshot.snapshot_date, 
                snapshot.snapshot_date
            )
            
            # Get the actual Modified Dietz gain/loss (accounts for external flows)
            period_dollar = detailed_result.get('gain_loss', 0)
            period_return = detailed_result.get('return_percentage', 0)
            
            logger.debug(f"Period performance for {snapshot.client.code}: ${period_dollar:,.2f} ({period_return:.2f}%)")
            
            return {
                'period_dollar': period_dollar,
                'period_percent': period_return
            }
        except Exception as e:
            logger.error(f"Error calculating period performance: {e}")
            return {'period_dollar': 0, 'period_percent': 0}
    
    def _calculate_since_inception_performance(self, snapshot: PortfolioSnapshot) -> dict:
        """Calculate since inception performance using Modified Dietz."""
        # Get first snapshot for this client
        first_snapshot = PortfolioSnapshot.objects.filter(
            client=snapshot.client
        ).order_by('snapshot_date').first()
        
        if not first_snapshot or first_snapshot.snapshot_date == snapshot.snapshot_date:
            # This IS the first snapshot - inception return is $0
            logger.debug(f"First snapshot for {snapshot.client.code} - inception return is $0")
            return {'inception_dollar': 0, 'inception_percent': 0}
        
        try:
            # Use Modified Dietz service for accurate inception return and detailed calculation
            detailed_result = self.dietz_service.calculate_portfolio_return_detailed(
                snapshot.client.code,
                first_snapshot.snapshot_date,
                snapshot.snapshot_date
            )
            
            # Get the actual Modified Dietz gain/loss (accounts for external flows)
            inception_dollar = detailed_result.get('gain_loss', 0)
            inception_return = detailed_result.get('return_percentage', 0)
            
            logger.debug(f"Since inception performance for {snapshot.client.code}: ${inception_dollar:,.2f} ({inception_return:.2f}%)")
            
            return {
                'inception_dollar': inception_dollar,
                'inception_percent': inception_return
            }
        except Exception as e:
            logger.error(f"Error calculating inception performance: {e}")
            return {'inception_dollar': 0, 'inception_percent': 0}
    
    def _calculate_period_investment_cash_flow(self, client: Client, start_date: str, end_date: str) -> float:
        """
        Calculate investment cash flow for a specific period using InvestmentCashFlowService.
        
        Formula: (Dividends + Interest + Other Income) - (Taxes + Fees)
        Excludes: Trading activity, external flows, bond maturities
        
        Args:
            client: Client object
            start_date: Period start date (YYYY-MM-DD)
            end_date: Period end date (YYYY-MM-DD)
        
        Returns:
            float: Net investment cash flow for the period
        """
        # Get transactions in the period
        transactions = Transaction.objects.filter(
            client=client,
            date__gte=start_date,
            date__lte=end_date
        ).select_related('asset')
        
        if not transactions.exists():
            logger.debug(f"No transactions found for {client.code} ({start_date} to {end_date})")
            return 0.0
        
        # Use InvestmentCashFlowService to calculate net cash flow
        net_cash_flow = self.investment_cash_flow_service.calculate_investment_cash_flows_from_models(
            list(transactions)
        )
        
        logger.debug(f"Investment cash flow for {client.code} ({start_date} to {end_date}): ${net_cash_flow:,.2f}")
        return net_cash_flow
    
    def _generate_transaction_tables(self, client: Client, current_date: str, comparison_date: str) -> dict:
        """
        Generate transaction tables for the reporting period.
        Only show if there are transactions in the period.
        """
        if comparison_date == current_date:
            # No period to show transactions for
            return {}
        
        # Get transactions in the reporting period
        transactions = Transaction.objects.filter(
            client=client,
            date__gte=comparison_date,
            date__lte=current_date
        ).select_related('asset').order_by('-date', 'transaction_type')
        
        if not transactions.exists():
            return {}
        
        # Group transactions by type
        grouped_transactions = defaultdict(list)
        
        for transaction in transactions:
            transaction_data = {
                'date': transaction.date.strftime('%Y-%m-%d'),
                'asset_name': transaction.asset.name,
                'ticker': transaction.asset.ticker,
                'transaction_type': transaction.transaction_type,
                'quantity': float(transaction.quantity) if transaction.quantity else 0,
                'price': float(transaction.price) if transaction.price else 0,
                'amount': float(transaction.amount),
                'bank': transaction.bank,
                'account': transaction.account
            }
            
            transaction_type = transaction.transaction_type.upper()
            if transaction_type in ['BUY', 'SELL']:
                grouped_transactions['trades'].append(transaction_data)
            elif transaction_type in ['DIVIDEND', 'INTEREST']:
                grouped_transactions['income'].append(transaction_data)
            elif transaction_type in ['FEE']:
                grouped_transactions['fees'].append(transaction_data)
            else:
                grouped_transactions['other'].append(transaction_data)
        
        return dict(grouped_transactions)
    
    def _generate_transaction_tables_html(self, client: Client, current_date: str, comparison_date: str) -> str:
        """Generate HTML transaction tables for the reporting period."""
        logger.info(f"Generating transaction tables: {client.code} from {comparison_date} to {current_date}")
        
        if comparison_date == current_date:
            logger.info(f"No transaction period (same dates) - returning empty")
            return ""  # No period to show transactions for
        
        # Get transactions in the reporting period
        transactions = Transaction.objects.filter(
            client=client,
            date__gte=comparison_date,
            date__lte=current_date
        ).select_related('asset').order_by('-date', 'transaction_type')
        
        transaction_count = transactions.count()
        logger.info(f"Found {transaction_count} transactions between {comparison_date} and {current_date}")
        
        if not transactions.exists():
            return ""
        
        # Generate HTML table
        html_sections = ['<h3>Recent Transactions</h3>']
        html_sections.append('<div class="transaction-table-container">')
        html_sections.append('<table class="position-table">')
        html_sections.append('''
            <thead>
                <tr>
                    <th class="col-date">Date</th>
                    <th class="col-asset">Asset</th>
                    <th class="col-type">Type</th>
                    <th class="col-quantity">Quantity</th>
                    <th class="col-price">Price</th>
                    <th class="col-amount">Amount</th>
                    <th class="col-account">Account</th>
                </tr>
            </thead>
            <tbody>
        ''')
        
        for transaction in transactions:
            # Color amounts: negative = red, positive = green, zero = default
            amount_value = float(transaction.amount)
            if amount_value > 0:
                amount_class = 'positive'
            elif amount_value < 0:
                amount_class = 'negative'
            else:
                amount_class = ''
            
            # Format account info
            account_info = f"{transaction.account} {transaction.bank}" if transaction.account and transaction.bank else (transaction.bank or 'Unknown')
            
            html_sections.append(f'''
                <tr>
                    <td class="col-date">{transaction.date.strftime('%Y-%m-%d')}</td>
                    <td class="col-asset">{transaction.asset.name if transaction.asset else 'N/A'}</td>
                    <td class="col-type">{transaction.transaction_type}</td>
                    <td class="numeric">{float(transaction.quantity or 0):,.2f}</td>
                    <td class="numeric">${float(transaction.price or 0):,.2f}</td>
                    <td class="numeric {amount_class}">${amount_value:,.2f}</td>
                    <td class="col-account">{account_info}</td>
                </tr>
            ''')
        
        html_sections.append('</tbody></table></div>')
        return ''.join(html_sections)
    
    def _generate_charts_data(self, client: Client, current_snapshot: PortfolioSnapshot, 
                            comparison_snapshot: PortfolioSnapshot, current_date: str) -> dict:
        """
        Generate ApexCharts data for all 5 required charts:
        1. Asset Allocation (pie chart)
        2. Custody Allocation (pie chart)
        3. Portfolio History (line chart)
        4. Cumulative Return (line chart)
        5. Benchmark Comparison (line chart)
        """
        positions = current_snapshot.positions.select_related('asset').exclude(asset__bank='ALT').all()
        total_value = sum(pos.market_value for pos in positions)
        
        # 1. Asset Allocation Chart
        asset_allocation_data = self._calculate_asset_allocation(positions)
        asset_allocation_chart = {
            'hasData': bool(asset_allocation_data),
            'message': 'Asset allocation data' if asset_allocation_data else 'No asset allocation data',
            'series': [data['percentage'] for data in asset_allocation_data.values()],
            'labels': list(asset_allocation_data.keys()),
            'monetaryValues': [data['market_value'] for data in asset_allocation_data.values()]
        }
        
        # 2. Custody Allocation Chart  
        custody_allocation_data = self._calculate_custody_allocation(positions)
        custody_allocation_chart = {
            'hasData': bool(custody_allocation_data),
            'message': 'Custody allocation data' if custody_allocation_data else 'No custody allocation data',
            'series': [data['percentage'] for data in custody_allocation_data.values()],
            'labels': list(custody_allocation_data.keys()),
            'monetaryValues': [data['market_value'] for data in custody_allocation_data.values()]
        }
        
        # 3. Portfolio History Chart - FIXED to use current_date and show actual values
        portfolio_history_chart = self._generate_portfolio_history_chart(client, current_date)
        
        # 4. Cumulative Return Chart - FIXED to calculate Modified Dietz returns
        cumulative_return_chart = self._generate_cumulative_return_chart(client, current_date)
        
        # 5. Benchmark Comparison Chart - NEW: Portfolio vs VOO vs AGG
        benchmark_comparison_chart = self._generate_benchmark_comparison_chart(client, current_date)
        
        # 6. Portfolio Comparison Chart - FIXED with actual 4-metric data
        if comparison_snapshot != current_snapshot:
            # Calculate the 4 required metrics
            current_metrics = self._calculate_enhanced_metrics(current_snapshot)
            comparison_metrics = self._calculate_enhanced_metrics(comparison_snapshot)
            
            # 1. Total Value Change (simple portfolio value difference)
            total_value_change = current_metrics.get('total_value', 0) - comparison_metrics.get('total_value', 0)
            
            # 2. Real Gain/Loss (Modified Dietz) - ALREADY CALCULATED CORRECTLY
            real_gain_loss = current_metrics.get('real_gain_loss_dollar', 0)  # This IS the Modified Dietz gain/loss
            
            # 3. Net Investment Cash Flow (using InvestmentCashFlowService)
            comparison_date_str = str(comparison_snapshot.snapshot_date)
            net_investment_cash_flow = self._calculate_period_investment_cash_flow(
                client, comparison_date_str, current_date
            )
            
            # 4. Estimated Annual Income Change
            annual_income_change = (current_metrics.get('estimated_annual_income', 0) - 
                                  comparison_metrics.get('estimated_annual_income', 0))
            
            # Prepare data for ApexCharts column chart
            chart_data = [total_value_change, real_gain_loss, net_investment_cash_flow, annual_income_change]
            
            # Calculate Y-axis range
            min_value = min(chart_data) if chart_data else 0
            max_value = max(chart_data) if chart_data else 0
            y_range = max_value - min_value
            y_padding = max(y_range * 0.1, 1000)
            
            portfolio_comparison_chart = {
                'hasData': True,
                'message': 'Portfolio metrics comparison',
                'series': [{'name': 'Change Amount', 'data': chart_data}],
                'categories': [
                    'Total Value Change',      # Simple portfolio value difference
                    'Real Gain/Loss',          # Modified Dietz gain/loss (same as period performance)
                    'Net Investment Cash Flow', # Dividends + Interest - Fees
                    'Est. Annual Income Change' # Projected income difference
                ],
                'yAxisMin': min_value - y_padding,
                'yAxisMax': max_value + y_padding
            }
            
            logger.debug(f"Portfolio comparison chart data: {chart_data}")
            logger.debug(f"Real Gain/Loss (Modified Dietz): ${real_gain_loss:,.2f}")
        else:
            portfolio_comparison_chart = {
                'hasData': False, 
                'message': 'No comparison period available'
            }
        
        return {
            'asset_allocation': asset_allocation_chart,
            'custody_allocation': custody_allocation_chart, 
            'portfolio_history': portfolio_history_chart,
            'cumulative_return': cumulative_return_chart,
            'benchmark_comparison': benchmark_comparison_chart,
            'portfolio_comparison': portfolio_comparison_chart
        }
    
    def _calculate_asset_allocation(self, positions) -> dict:
        """Calculate asset allocation with market_value and percentage using ProjectAurum categories."""
        allocation = defaultdict(float)
        total_value = sum(pos.market_value for pos in positions)
        
        if total_value == 0:
            return {}
        
        for position in positions:
            asset_type = position.asset.asset_type
            
            # CORRECT MAPPING - EXACT PROJECTAURUM CATEGORIES
            if asset_type == 'Fixed Income':
                category = 'Fixed Income'
            elif asset_type == 'Equities':
                category = 'Equities'
            elif asset_type in ['Cash', 'Money Market']:
                category = 'Cash/Money Market'  # FIXED - combine both
            else:
                category = 'Alternatives'
            
            allocation[category] += float(position.market_value)
        
        # Return objects with market_value and percentage
        return {
            k: {
                'market_value': v, 
                'percentage': round(v / float(total_value) * 100, 2)
            } 
            for k, v in allocation.items()
        }
    
    def _calculate_custody_allocation(self, positions) -> dict:
        """Calculate custody allocation with market_value and percentage using Account Bank format."""
        allocation = defaultdict(float)
        total_value = sum(pos.market_value for pos in positions)
        
        if total_value == 0:
            return {}
        
        for position in positions:
            # CORRECT FORMAT: "Account Bank" (e.g., "LSAdmin JPM", "LS JPM")
            if position.account and position.bank:
                custody = f"{position.account} {position.bank}"
            elif position.bank:
                custody = position.bank
            else:
                custody = 'Unknown'
                
            allocation[custody] += float(position.market_value)
        
        # Return objects with market_value and percentage
        return {
            k: {
                'market_value': v, 
                'percentage': round(v / float(total_value) * 100, 2)
            } 
            for k, v in allocation.items()
        }
    
    def _group_positions_by_type(self, positions) -> dict:
        """Group positions by asset type with summary stats using normalized asset types."""
        grouped = defaultdict(lambda: {'count': 0, 'total_value': 0, 'positions': []})
        
        for position in positions:
            # Use normalized asset type directly (no more mapping needed)
            asset_type = position.asset.asset_type
            
            # Map normalized types to display categories
            if asset_type == 'Fixed Income':
                category = 'bonds'
            elif asset_type == 'Equities':
                category = 'equities'
            elif asset_type in ['Cash', 'Money Market']:
                category = 'cash'
            else:
                category = 'other'
            
            grouped[category]['count'] += 1
            grouped[category]['total_value'] += float(position.market_value)
            grouped[category]['positions'].append({
                'name': position.asset.name,
                'ticker': position.asset.ticker,
                'market_value': float(position.market_value),
                'quantity': float(position.quantity)
            })
        
        return dict(grouped)
    
    def _calculate_bond_maturity_timeline(self, positions) -> dict:
        """
        Calculate bond maturity timeline with top 5 bonds for interactive drill-down.
        Based on ProjectAurum analytics/bond_maturity.py logic.
        """
        # Initialize timeline with both summary and detail structures
        timeline = {
            '2025': {'num_bonds': 0, 'face_value': 0, 'market_value': 0, 'bonds': [], 'top_bonds': []},
            '2026': {'num_bonds': 0, 'face_value': 0, 'market_value': 0, 'bonds': [], 'top_bonds': []},
            '2027': {'num_bonds': 0, 'face_value': 0, 'market_value': 0, 'bonds': [], 'top_bonds': []},
            '2028': {'num_bonds': 0, 'face_value': 0, 'market_value': 0, 'bonds': [], 'top_bonds': []},
            '2029': {'num_bonds': 0, 'face_value': 0, 'market_value': 0, 'bonds': [], 'top_bonds': []},
            '2030+': {'num_bonds': 0, 'face_value': 0, 'market_value': 0, 'bonds': [], 'top_bonds': []}
        }
        
        # Process each position
        for position in positions:
            # Filter for Fixed Income assets with maturity dates (keep existing logic)
            if position.asset.asset_type != 'Fixed Income' or not position.asset.maturity_date:
                continue
                
            # Determine maturity year bucket
            maturity_year = position.asset.maturity_date.year
            bucket = str(maturity_year) if maturity_year < 2030 else '2030+'
            
            if bucket not in timeline:
                continue
                
            # Update summary metrics
            timeline[bucket]['num_bonds'] += 1
            timeline[bucket]['market_value'] += float(position.market_value)
            timeline[bucket]['face_value'] += float(position.face_value or position.market_value)
            
            # Create individual bond detail for drill-down
            bond_detail = {
                'bank_account': self._format_bank_account(position),
                'name': position.asset.name,
                'market_value': float(position.market_value),
                'maturity_date': self._format_maturity_date(position.asset.maturity_date),
                'cusip': position.asset.cusip or '',
                'coupon_rate': self._format_coupon_rate(position.coupon_rate or position.asset.coupon_rate)
            }
            
            # Add to bonds list for this year
            timeline[bucket]['bonds'].append(bond_detail)
        
        # Sort bonds by market value and select top 5 for each year
        for year_bucket in timeline:
            bonds = timeline[year_bucket]['bonds']
            if bonds:
                # Sort by market value (descending) and take top 5
                sorted_bonds = sorted(bonds, key=lambda x: x['market_value'], reverse=True)
                timeline[year_bucket]['top_bonds'] = sorted_bonds[:5]
        
        return timeline
    
    def _format_bank_account(self, position):
        """Format bank and account information for display."""
        # Use existing custody format: "Account Bank" (e.g., "LSAdmin JPM")
        if position.account and position.bank:
            return f"{position.account} {position.bank}"
        elif position.bank:
            return position.bank
        else:
            return 'Unknown Account'

    def _format_maturity_date(self, maturity_date):
        """Format maturity date for display (DD/MM/YYYY format)."""
        if not maturity_date:
            return 'Unknown'
        
        try:
            # Convert from date object to DD/MM/YYYY string
            return maturity_date.strftime('%d/%m/%Y')
        except (ValueError, TypeError, AttributeError):
            return str(maturity_date)

    def _format_coupon_rate(self, coupon_rate):
        """Format coupon rate for display."""
        if coupon_rate is None or coupon_rate == '':
            return 'N/A'
        
        try:
            rate_float = float(coupon_rate)
            return f"{rate_float:.2f}%"
        except (ValueError, TypeError):
            return str(coupon_rate)
    
    def _generate_portfolio_history_chart(self, client: Client, current_date: str = None) -> dict:
        """Generate portfolio history chart - show actual portfolio values over time (includes cash flows)."""
        
        # Get snapshots up to current_date (inclusive)
        query = PortfolioSnapshot.objects.filter(client=client)
        if current_date:
            query = query.filter(snapshot_date__lte=current_date)
        
        snapshots = query.order_by('snapshot_date')
        
        if snapshots.count() < 2:
            return {
                'hasData': False,
                'message': 'Not enough historical data to display portfolio value evolution',
                'series': [],
                'yAxisMin': 0,
                'yAxisMax': 100,
                'colors': ['#007bff'],
                'gradient': {'to': '#0056b3'}
            }
        
        dates = []
        values = []
        
        # Get actual portfolio total_value for each snapshot (includes cash flows)
        for snapshot in snapshots:
            dates.append(snapshot.snapshot_date.strftime('%Y-%m-%d'))
            total_value = snapshot.portfolio_metrics.get('total_value', 0)
            values.append(float(total_value))
        
        # Format data for ApexCharts with timestamps
        chart_data = []
        for i, (date_str, value) in enumerate(zip(dates, values)):
            try:
                from datetime import datetime
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                timestamp = int(date_obj.timestamp() * 1000)
                chart_data.append({'x': timestamp, 'y': value})
            except ValueError:
                continue
        
        return {
            'hasData': True,
            'message': 'Portfolio history data',
            'series': [{'name': 'Portfolio Value', 'data': chart_data}],
            'currentValue': f"${values[-1]:,.2f}" if values else "$0.00",
            'currentDate': dates[-1] if dates else '',
            'yAxisMin': min(values) * 0.95 if values else 0,
            'yAxisMax': max(values) * 1.05 if values else 100,
            'colors': ['#5f76a1'],
            'gradient': {'to': '#dae1f3'}
        }
    
    def _generate_cumulative_return_chart(self, client: Client, current_date: str) -> dict:
        """Generate cumulative return chart using Modified Dietz (excludes cash flows, base 1000)."""
        
        # Get snapshots up to current_date (inclusive)  
        snapshots = PortfolioSnapshot.objects.filter(
            client=client,
            snapshot_date__lte=current_date
        ).order_by('snapshot_date')
        
        if snapshots.count() < 2:
            return {
                'hasData': False,
                'message': 'Not enough historical data to display cumulative returns',
                'series': [],
                'yAxisMin': 950,
                'yAxisMax': 1050,
                'colors': ['#28a745'],
                'gradient': {'to': '#1e7e34'}
            }
        
        dates = []
        cumulative_values = []
        
        first_snapshot = snapshots.first()
        dates.append(first_snapshot.snapshot_date.strftime('%Y-%m-%d'))
        cumulative_values.append(1000)  # Base 1000 start
        
        # Calculate cumulative returns using Modified Dietz from inception
        for snapshot in snapshots[1:]:
            try:
                # Calculate return from inception using Modified Dietz
                inception_return = self.dietz_service.calculate_return(
                    client.code, 
                    first_snapshot.snapshot_date, 
                    snapshot.snapshot_date
                )
                
                # Apply to base 1000: 1000 * (1 + return_percentage/100)
                cumulative_value = 1000 * (1 + inception_return / 100)
                cumulative_values.append(round(cumulative_value, 2))
                dates.append(snapshot.snapshot_date.strftime('%Y-%m-%d'))
                
            except Exception as e:
                logger.warning(f"Error calculating cumulative return: {e}")
                cumulative_values.append(cumulative_values[-1])  # Use previous value
                dates.append(snapshot.snapshot_date.strftime('%Y-%m-%d'))
        
        # Format data for ApexCharts with timestamps
        chart_data = []
        for i, (date_str, value) in enumerate(zip(dates, cumulative_values)):
            try:
                from datetime import datetime
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                timestamp = int(date_obj.timestamp() * 1000)
                chart_data.append({'x': timestamp, 'y': value})
            except ValueError:
                continue
        
        return {
            'hasData': True,
            'message': 'Cumulative return data',
            'series': [{'name': 'Cumulative Return (Base: 1000)', 'data': chart_data}],
            'currentValue': f"{cumulative_values[-1]:.2f}" if cumulative_values else "1000.00",
            'currentDate': dates[-1] if dates else '',
            'yAxisMin': min(cumulative_values) - 50 if cumulative_values else 950,
            'yAxisMax': max(cumulative_values) + 50 if cumulative_values else 1050,
            'colors': ['#5f76a1'],
            'gradient': {'to': '#dae1f3'}
        }
    
    def _generate_benchmark_comparison_chart(self, client: Client, current_date: str) -> dict:
        """Generate benchmark comparison chart with portfolio vs VOO vs AGG (all base 1000)."""
        
        # Get snapshots up to current_date (inclusive)  
        snapshots = PortfolioSnapshot.objects.filter(
            client=client,
            snapshot_date__lte=current_date
        ).order_by('snapshot_date')
        
        if snapshots.count() < 2:
            return {
                'hasData': False,
                'message': 'Not enough historical data to display benchmark comparison',
                'series': [],
                'yAxisMin': 950,
                'yAxisMax': 1050,
                'colors': ['#5f76a1', '#28a745', '#ffc107']
            }
        
        # Get portfolio data
        first_snapshot = snapshots.first()
        start_date = first_snapshot.snapshot_date.strftime('%Y-%m-%d')
        end_date = current_date
        
        # Portfolio cumulative values (reuse existing logic)
        portfolio_chart = self._generate_cumulative_return_chart(client, current_date)
        
        if not portfolio_chart.get('hasData'):
            return portfolio_chart
        
        # Get benchmark data (VOO and AGG)
        benchmark_data = self.benchmark_service.get_benchmark_data(start_date, end_date)
        
        # Start with portfolio series
        series = [{
            'name': f'{client.name} Portfolio',
            'data': portfolio_chart['series'][0]['data'],
            'color': '#5f76a1'
        }]
        
        all_values = [point['y'] for point in portfolio_chart['series'][0]['data']]
        
        # Add VOO (S&P 500) series
        if 'VOO' in benchmark_data and benchmark_data['VOO']:
            voo_chart_data = []
            for point in benchmark_data['VOO']:
                try:
                    from datetime import datetime
                    date_obj = datetime.strptime(point['date'], '%Y-%m-%d')
                    timestamp = int(date_obj.timestamp() * 1000)
                    voo_chart_data.append({'x': timestamp, 'y': point['cumulative_value']})
                    all_values.append(point['cumulative_value'])
                except ValueError:
                    continue
            
            if voo_chart_data:
                series.append({
                    'name': 'S&P 500 (VOO)',
                    'data': voo_chart_data,
                    'color': '#28a745'
                })
        
        # Add AGG (Fixed Income) series
        if 'AGG' in benchmark_data and benchmark_data['AGG']:
            agg_chart_data = []
            for point in benchmark_data['AGG']:
                try:
                    from datetime import datetime
                    date_obj = datetime.strptime(point['date'], '%Y-%m-%d')
                    timestamp = int(date_obj.timestamp() * 1000)
                    agg_chart_data.append({'x': timestamp, 'y': point['cumulative_value']})
                    all_values.append(point['cumulative_value'])
                except ValueError:
                    continue
            
            if agg_chart_data:
                series.append({
                    'name': 'Fixed Income (AGG)',
                    'data': agg_chart_data,
                    'color': '#ffc107'
                })
        
        # Calculate Y-axis range
        y_min = min(all_values) - 50 if all_values else 950
        y_max = max(all_values) + 50 if all_values else 1050
        
        return {
            'hasData': len(series) > 0,
            'message': 'Portfolio performance vs market benchmarks since inception',
            'series': series,
            'currentValue': portfolio_chart.get('currentValue', '1000.00'),
            'currentDate': portfolio_chart.get('currentDate', ''),
            'yAxisMin': y_min,
            'yAxisMax': y_max,
            'colors': ['#5f76a1', '#28a745', '#ffc107'],
            'inception_date': start_date,
            'benchmark_count': len([s for s in series if 'VOO' in s['name'] or 'AGG' in s['name']])
        }
    
    def _prepare_enhanced_template_context(self, client: Client, current_date: str, 
                                         comparison_date: str, current_metrics: dict, 
                                         comparison_metrics: dict, position_tables: dict,
                                         transaction_tables_html: str, charts_data: dict) -> dict:
        """Prepare enhanced template context with position/transaction HTML tables."""
        
        # Calculate comparison values
        portfolio_value_change = current_metrics.get('total_value', 0) - comparison_metrics.get('total_value', 0)
        portfolio_value_pct = (portfolio_value_change / comparison_metrics.get('total_value', 1)) * 100 if comparison_metrics.get('total_value', 0) > 0 else 0
        
        income_change = current_metrics.get('estimated_annual_income', 0) - comparison_metrics.get('estimated_annual_income', 0)
        income_pct = (income_change / comparison_metrics.get('estimated_annual_income', 1)) * 100 if comparison_metrics.get('estimated_annual_income', 0) > 0 else 0
        
        # FIXED: Calculate actual investment cash flows for both periods
        if comparison_date != current_date:
            # Week 1: Cash flow for comparison period (single day or short period)
            week1_cash_flow = self._calculate_period_investment_cash_flow(
                client, comparison_date, comparison_date
            )
            
            # Week 2: Cash flow for period between comparison and current
            week2_cash_flow = self._calculate_period_investment_cash_flow(
                client, comparison_date, current_date
            )
            
            # Combined: Total cash flow for the entire period
            combined_cash_flow = week1_cash_flow + week2_cash_flow
            
            logger.debug(f"Cash flows - Week1: ${week1_cash_flow:.2f}, Week2: ${week2_cash_flow:.2f}, Combined: ${combined_cash_flow:.2f}")
        else:
            # First report - no comparison period
            week1_cash_flow = 0.0
            week2_cash_flow = current_metrics.get('net_cash_flow', 0.0)
            combined_cash_flow = week2_cash_flow
        
        # Create base context matching template expectations
        context = {
            # Header information
            'client_name': client.name,
            'client_code': client.code,
            'date1': comparison_date,
            'date2': current_date,
            'report_title': f'Portfolio Report - {client.name}',
            
            # Comparison data structure expected by template
            'comparison': {
                'date1': comparison_date,
                'date2': current_date,
                'portfolio_value_change': portfolio_value_change,
                'portfolio_value_pct': portfolio_value_pct,
                'combined_cash_flow': combined_cash_flow,  # FIXED
                'income_change': income_change,
                'income_pct': income_pct,
                'week1_summary': {
                    'total_portfolio_value': comparison_metrics.get('total_value', 0),
                    'cash_flow': week1_cash_flow,  # FIXED
                    'total_annual_income': comparison_metrics.get('estimated_annual_income', 0),
                    'total_annual_yield': comparison_metrics.get('annual_income_yield', 0),
                    'count_of_assets': comparison_metrics.get('position_count', 0)
                },
                'week2_summary': {
                    'total_portfolio_value': current_metrics.get('total_value', 0),
                    'cash_flow': week2_cash_flow,  # FIXED
                    'total_annual_income': current_metrics.get('estimated_annual_income', 0),
                    'total_annual_yield': current_metrics.get('annual_income_yield', 0),
                    'count_of_assets': current_metrics.get('position_count', 0)
                }
            },
            
            # Main portfolio data (current period)
            'week2_data': {
                'total_value': current_metrics.get('total_value', 0),
                'total_cost_basis': current_metrics.get('total_cost_basis', 0),
                'unrealized_gain_loss': current_metrics.get('unrealized_gain_loss', 0),
                'unrealized_gain_loss_pct': current_metrics.get('unrealized_gain_loss_pct', 0),
                'total_annual_income': current_metrics.get('estimated_annual_income', 0)
            },
            
            # Summary metrics
            'week2_summary': {
                'weekly_dollar_performance': current_metrics.get('real_gain_loss_dollar', 0),
                'weekly_percent_performance': current_metrics.get('real_gain_loss_percent', 0),
                'total_return_pct': current_metrics.get('real_gain_loss_percent', 0),
                'inception_dollar_performance': current_metrics.get('inception_gain_loss_dollar', 0),
                'inception_return_pct': current_metrics.get('inception_gain_loss_percent', 0),
                'ytd_dollar_performance': current_metrics.get('inception_gain_loss_dollar', 0),  # Placeholder
                'ytd_return_pct': current_metrics.get('inception_gain_loss_percent', 0),  # Placeholder
                'total_annual_income': current_metrics.get('estimated_annual_income', 0),
                'total_annual_yield': current_metrics.get('annual_income_yield', 0)
            }
        }
        
        # Add enhanced data
        biggest_movers_data = current_metrics.get('biggest_movers', [])
        print(f"DEBUG: Adding to template context - biggest_movers has {len(biggest_movers_data)} items")
        context.update({
            # Position tables (exact ProjectAurum format)
            'position_tables': position_tables,
            
            # Transaction tables HTML (conditional rendering)
            'transaction_tables': transaction_tables_html,
            'has_transactions': bool(transaction_tables_html.strip()),
            
            # Biggest movers data
            'biggest_movers': biggest_movers_data,
            
            # Individual chart objects expected by template
            'cumulative_return_chart': charts_data.get('cumulative_return', {'hasData': False, 'message': 'No data available'}),
            'portfolio_history_chart': charts_data.get('portfolio_history', {'hasData': False, 'message': 'No data available'}),
            'asset_allocation_chart': charts_data.get('asset_allocation', {'hasData': False, 'message': 'No data available'}),
            'custody_allocation_chart': charts_data.get('custody_allocation', {'hasData': False, 'message': 'No data available'}),
            'portfolio_comparison_chart': charts_data.get('portfolio_comparison', {'hasData': False, 'message': 'No data available'}),
            'benchmark_comparison_chart': charts_data.get('benchmark_comparison', {'hasData': False, 'message': 'No data available'}),
            
            # Enhanced flags
            'has_bonds': 'bonds' in position_tables,
            'has_equities': 'equities' in position_tables,
            'has_cash': 'cash' in position_tables,
            'has_other': 'other' in position_tables,
            
            # Summary statistics
            'summary_stats': {
                'total_positions': current_metrics.get('position_count', 0),
                'bond_count': len(position_tables.get('bonds', [])),
                'equity_count': len(position_tables.get('equities', [])),
                'cash_count': len(position_tables.get('cash', [])),
                'other_count': len(position_tables.get('other', [])),
            },
            
            # Add missing template variables
            'asset_allocation': current_metrics.get('asset_allocation', {}),
            'custody_allocation': current_metrics.get('custody_allocation', {}),
            'bond_maturity_timeline': current_metrics.get('bond_maturity', {}) or {
                '2025': {'num_bonds': 0, 'market_value': 0, 'face_value': 0},
                '2026': {'num_bonds': 0, 'market_value': 0, 'face_value': 0},
                '2027': {'num_bonds': 0, 'market_value': 0, 'face_value': 0},
                '2028': {'num_bonds': 0, 'market_value': 0, 'face_value': 0},
                '2029': {'num_bonds': 0, 'market_value': 0, 'face_value': 0},
                '2030+': {'num_bonds': 0, 'market_value': 0, 'face_value': 0}
            },
            
            # Generate HTML position tables
            'positions_table': self._generate_position_tables_html(
                PortfolioSnapshot.objects.get(client=client, snapshot_date=current_date)
            ),
            'transactions_table': transaction_tables_html,
            'has_transactions': bool(transaction_tables_html.strip()),
            
            # Chart data
            'asset_allocation_chart': charts_data.get('asset_allocation', {'hasData': False}),
            'custody_allocation_chart': charts_data.get('custody_allocation', {'hasData': False}),
            'portfolio_history_chart': charts_data.get('portfolio_history', {'hasData': False}),
            'cumulative_return_chart': charts_data.get('cumulative_return', {'hasData': False}),
            'portfolio_comparison_chart': charts_data.get('portfolio_comparison', {'hasData': False}),
            'benchmark_comparison_chart': charts_data.get('benchmark_comparison', {'hasData': False}),
            
            # FIXED: Add missing template variables for conditional rendering
            'is_first_report': comparison_date == current_date,
            'has_comparison': comparison_date != current_date,
            'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Add rollover transparency
        rollover_info = None
        try:
            snapshot = PortfolioSnapshot.objects.get(
                client=client,
                snapshot_date=current_date
            )
            
            if snapshot.has_rolled_accounts and snapshot.rollover_summary:
                account_list = []
                for account_key, rolled_from in snapshot.rollover_summary.items():
                    bank, account = account_key.split('_', 1)
                    # Filter ALT accounts from rollover alert display (ALTs still get rolled over normally)
                    if bank != 'ALT':
                        account_list.append(f"{account} {bank}")
                
                rollover_info = {
                    'has_rollover': True,
                    'accounts': account_list,
                    'message': (f"⚠️ Data was rolled over for client {client.code} "
                               f"for accounts: {', '.join(account_list)}. "
                               f"This data reflects the last available information.")
                }
        except PortfolioSnapshot.DoesNotExist:
            pass
        
        context['rollover_info'] = rollover_info
        
        return context
    
    def _prepare_template_context(self, client: Client, current_date: str, 
                                comparison_date: str, current_metrics: dict, 
                                comparison_metrics: dict) -> dict:
        """Prepare template context in exact ProjectAurum format."""
        
        # Calculate period changes
        current_value = current_metrics.get('total_value', 0)
        comparison_value = comparison_metrics.get('total_value', 0)
        value_change = current_value - comparison_value
        
        # Get real gain/loss (Modified Dietz)
        real_gain_loss_dollar = current_metrics.get('real_gain_loss_dollar', 0)
        real_gain_loss_percent = current_metrics.get('real_gain_loss_percent', 0)
        
        # Get inception returns
        inception_gain_loss_dollar = current_metrics.get('inception_gain_loss_dollar', 0)
        inception_gain_loss_percent = current_metrics.get('inception_gain_loss_percent', 0)
        
        # Prepare context matching ProjectAurum template expectations
        context = {
            # Header information
            'client_name': client.name,
            'client_code': client.code,
            'date1': comparison_date,
            'date2': current_date,
            'report_title': f'Portfolio Report - {client.name}',
            'period_info': f'Period: {comparison_date} to {current_date}',
            'portfolio_value_change': value_change,
            'portfolio_value_pct': real_gain_loss_percent,
            
            # Main portfolio data (current period)
            'week2_data': {
                'total_value': current_value,
                'total_cost_basis': current_metrics.get('total_cost_basis', 0),
                'unrealized_gain_loss': current_metrics.get('unrealized_gain_loss', 0),
                'unrealized_gain_loss_pct': current_metrics.get('unrealized_gain_loss_pct', 0),
                'total_annual_income': current_metrics.get('estimated_annual_income', 0)
            },
            
            # Summary metrics
            'week2_summary': {
                'weekly_dollar_performance': real_gain_loss_dollar,
                'weekly_percent_performance': real_gain_loss_percent,
                'total_return_pct': real_gain_loss_percent,
                'inception_dollar_performance': inception_gain_loss_dollar,
                'inception_return_pct': inception_gain_loss_percent,
                'ytd_dollar_performance': inception_gain_loss_dollar,  # Placeholder
                'ytd_return_pct': inception_gain_loss_percent,  # Placeholder
                'estimated_annual_income': current_metrics.get('estimated_annual_income', 0),
                'total_annual_income': current_metrics.get('estimated_annual_income', 0),
                'total_annual_yield': self._calculate_yield_percentage(
                    current_metrics.get('estimated_annual_income', 0), current_value
                ),
                'annual_income_yield': self._calculate_yield_percentage(
                    current_metrics.get('estimated_annual_income', 0), current_value
                ),
                'total_portfolio_value': current_value,
                'cash_flow': current_metrics.get('net_cash_flow', 0),
                'count_of_assets': current_metrics.get('position_count', 0)
            },
            
            # Asset allocation
            'asset_allocation': current_metrics.get('asset_allocation', {}),
            
            # Custody allocation
            'custody_allocation': current_metrics.get('custody_allocation', {}),
            
            # Top movers
            'top_movers': current_metrics.get('top_movers', {'gainers': [], 'losers': []}),
            'biggest_movers': current_metrics.get('biggest_movers', []),
            
            # Bond maturity timeline (empty for now - needs maturity dates in data)
            'bond_maturity': current_metrics.get('bond_maturity', {}),
            
            # Positions grouped by type
            'positions_by_type': current_metrics.get('positions_by_type', {}),
            
            # Recent transactions
            'recent_transactions': current_metrics.get('recent_transactions', []),
            
            # Position and transaction tables for template
            'positions_table': current_metrics.get('positions_by_type', {}),
            'transactions_table': current_metrics.get('recent_transactions', []),
            
            # Comparison data structure expected by template
            'comparison': {
                'date1': comparison_date,
                'date2': current_date,
                'week1_summary': {
                    'count_of_assets': comparison_metrics.get('position_count', 0),
                    'total_annual_income': comparison_metrics.get('estimated_annual_income', 0),
                    'total_annual_yield': self._calculate_yield_percentage(
                        comparison_metrics.get('estimated_annual_income', 0), comparison_value
                    )
                },
                'week2_summary': {
                    'count_of_assets': current_metrics.get('position_count', 0),
                    'total_annual_income': current_metrics.get('estimated_annual_income', 0),
                    'total_annual_yield': self._calculate_yield_percentage(
                        current_metrics.get('estimated_annual_income', 0), current_value
                    ),
                    'total_portfolio_value': current_value,
                    'cash_flow': current_metrics.get('net_cash_flow', 0)
                },
                'combined_cash_flow': current_metrics.get('net_cash_flow', 0),
                'income_change': (
                    current_metrics.get('estimated_annual_income', 0) - 
                    comparison_metrics.get('estimated_annual_income', 0)
                ),
                'portfolio_value_change': value_change,
                'portfolio_value_pct': real_gain_loss_percent
            },
            
            # Weekly comparison data
            'weekly_comparison': {
                'previous_total_value': comparison_value,
                'current_total_value': current_value,
                'total_value_change': value_change,
                'real_gain_loss_dollar': real_gain_loss_dollar,
                'real_gain_loss_percent': real_gain_loss_percent,
                'cash_flow': current_metrics.get('net_cash_flow', 0),
                'income_change': (
                    current_metrics.get('estimated_annual_income', 0) - 
                    comparison_metrics.get('estimated_annual_income', 0)
                ),
                'position_count_change': (
                    current_metrics.get('position_count', 0) - 
                    comparison_metrics.get('position_count', 0)
                )
            },
            
            # Chart data - will be updated by enhanced context
            'cumulative_return_chart': {'hasData': False, 'message': 'No data available'},
            'portfolio_history_chart': {'hasData': False, 'message': 'No data available'},
            'asset_allocation_chart': {'hasData': False, 'message': 'No data available'},
            'custody_allocation_chart': {'hasData': False, 'message': 'No data available'},
            'portfolio_comparison_chart': {'hasData': False, 'message': 'No data available'},
            
            # Additional template variables
            'is_first_report': comparison_date == current_date,
            'has_comparison': comparison_date != current_date,
            'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return context
    
    def _calculate_yield_percentage(self, annual_income: float, total_value: float) -> float:
        """Calculate yield percentage."""
        if total_value > 0:
            return (annual_income / total_value) * 100
        return 0.0
    
    def generate_report_for_client(self, client_code: str, snapshot_date: str = None) -> str:
        """
        Generate report for a client using the most recent snapshot.
        """
        if not snapshot_date:
            # Get most recent snapshot
            client = Client.objects.get(code=client_code)
            latest_snapshot = PortfolioSnapshot.objects.filter(
                client=client
            ).order_by('-snapshot_date').first()
            
            if not latest_snapshot:
                raise ValueError(f"No snapshots found for client {client_code}")
            
            snapshot_date = latest_snapshot.snapshot_date
        
        # Get previous snapshot for comparison
        client = Client.objects.get(code=client_code)
        previous_snapshot = PortfolioSnapshot.objects.filter(
            client=client,
            snapshot_date__lt=snapshot_date
        ).order_by('-snapshot_date').first()
        
        comparison_date = previous_snapshot.snapshot_date if previous_snapshot else snapshot_date
        
        return self.generate_weekly_report(client_code, snapshot_date, comparison_date)
    
    def _calculate_biggest_movers_fixed(self, current_snapshot, comparison_snapshot, limit=5):
        """
        Calculate biggest movers using ProjectAurum's enhanced algorithm.
        Filters out cash, small positions, and quantity changes.
        Focuses on price-driven market movements only.
        """
        if not comparison_snapshot or current_snapshot.snapshot_date == comparison_snapshot.snapshot_date:
            return []
        
        # Configuration (matching ProjectAurum exactly)
        excluded_asset_types = ['Cash', 'Money Market']
        max_quantity_change_pct = 5.0  # Maximum allowed quantity change percentage
        min_position_size = 5000  # Minimum position size to avoid noise
        
        # Convert Django positions to ProjectAurum format
        week1_positions = self._convert_positions_to_dict(comparison_snapshot.positions.select_related('asset').all())
        week2_positions = self._convert_positions_to_dict(current_snapshot.positions.select_related('asset').all())
        
        # Create dictionary mapping from position identifier to position details for week 1
        week1_positions_dict = {}
        for position in week1_positions:
            # Use compound key that uniquely identifies the position (ProjectAurum format)
            cusip_str = str(position.get('cusip', '')).strip()
            key = (
                cusip_str,
                position.get('name', '').strip(), 
                position.get('ticker', '').strip(),
                position.get('asset_type', '').strip()
            )
            week1_positions_dict[key] = position
        
        # Calculate percentage changes for positions that exist in both weeks
        movers = []
        excluded_count = {'cash': 0, 'quantity_change': 0, 'small_size': 0, 'missing': 0}
        
        for position in week2_positions:
            # Create matching key for week2 position
            cusip_str = str(position.get('cusip', '')).strip()
            key = (
                cusip_str,
                position.get('name', '').strip(), 
                position.get('ticker', '').strip(),
                position.get('asset_type', '').strip()
            )
            
            # Skip if position didn't exist in week 1
            if key not in week1_positions_dict:
                excluded_count['missing'] += 1
                continue
            
            week1_position = week1_positions_dict[key]
            
            # Filter 1: Exclude cash and money market positions
            asset_type = position.get('asset_type', '').strip()
            if asset_type in excluded_asset_types:
                excluded_count['cash'] += 1
                continue
            
            # Get market values and quantities
            week1_value = float(week1_position.get('market_value', 0))
            week2_value = float(position.get('market_value', 0))
            week1_qty = float(week1_position.get('quantity', 0))
            week2_qty = float(position.get('quantity', 0))
            
            # Filter 2: Require meaningful position sizes
            if week1_value < min_position_size or week2_value < min_position_size:
                excluded_count['small_size'] += 1
                continue
            
            # Filter 3: Exclude positions with significant quantity changes (indicates transactions)
            if week1_qty != 0:
                qty_change_pct = abs(((week2_qty - week1_qty) / week1_qty) * 100)
                if qty_change_pct > max_quantity_change_pct:
                    excluded_count['quantity_change'] += 1
                    continue
            elif week2_qty != 0:
                # New position (week1_qty = 0, week2_qty > 0) - this is a purchase
                excluded_count['quantity_change'] += 1
                continue
            
            # Skip if either value is zero to avoid division by zero
            if week1_value == 0 or week2_value == 0:
                continue
            
            # Calculate price-based percentage change
            pct_change = ((week2_value - week1_value) / week1_value) * 100
            dollar_change = week2_value - week1_value
            
            # Create mover entry (ProjectAurum format)
            mover = {
                'name': position.get('name', 'Unknown'),
                'asset_type': asset_type,
                'pct_change': pct_change,
                'dollar_change': dollar_change,
                'abs_pct_change': abs(pct_change),
                'movement_type': 'Price Movement',
                'week1_value': week1_value,
                'week2_value': week2_value
            }
            movers.append(mover)
        
        # Sort by absolute dollar change (descending) - prioritizes financial impact
        movers.sort(key=lambda x: abs(x['dollar_change']), reverse=True)
        
        # Take top 'limit' movers
        top_movers = movers[:limit]
        
        logger.info(f"Biggest movers calculation complete: {len(top_movers)} of {len(movers)} total movers")
        
        return top_movers
    
    def _convert_positions_to_dict(self, positions):
        """Convert Django Position objects to ProjectAurum format dictionaries."""
        position_list = []
        for position in positions:
            position_dict = {
                'cusip': position.asset.cusip if position.asset else '',
                'name': position.asset.name if position.asset else 'Unknown',
                'ticker': position.asset.ticker if position.asset else '',
                'asset_type': position.asset.asset_type if position.asset else '',
                'market_value': float(position.market_value or 0),
                'quantity': float(position.quantity or 0)
            }
            position_list.append(position_dict)
        return position_list