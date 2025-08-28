"""
Cash flow classification service for AurumFinance.
Migrated from ProjectAurum calculations/cash_flow.py with Django ORM integration.

Classifies transactions as external cash flows for Modified Dietz calculations.
External flows (deposits/withdrawals) are excluded from performance calculations.
"""

import logging
import re
from typing import Dict, Any, Optional, List, Set, Union
from ..models import Transaction

logger = logging.getLogger(__name__)


class CashFlowService:
    """
    Enhanced classifier for Modified Dietz external cash flow detection.
    Ported from ProjectAurum with comprehensive bank-specific classification logic.
    
    Phase 1A: Core infrastructure with top 3 banks (CS, IDB, Pershing)
    """
    
    # External cash flows (money in/out of portfolio) - EXCLUDED from Modified Dietz
    INFLOW_TYPES = [
        # Generic/Legacy
        'deposit', 'bank deposit', 'contribution',
        # CSC Bank-Specific
        'Misc Cash Entry', 'Schwab ATM Rebate',
        # JPM Bank-Specific  
        'Misc Debit / Credit', 'Misc. Receipt',
        # MS Bank-Specific
        'Auto Bank Product Deposit',
        # LO Bank-Specific
        'Deposit',
        # IDB Bank-Specific
        'Wire Transfer Credit',
        # Banchile Bank-Specific
        'Wire Transfer In', 'Aporte'
    ]
    
    OUTFLOW_TYPES = [
        # Generic/Legacy
        'withdrawal', 'distribution',
        # CSC Bank-Specific
        'Journal', 'Wire Sent', 'Visa Purchase', 'ATM Withdrawal',
        # JPM Bank-Specific
        'Misc. Disbursement', 'OUTGOING',
        # MS Bank-Specific
        'Debit Card', 'Funds Transferred',
        # Valley Bank-Specific
        'WIRE OUT',
        # LO Bank-Specific
        'Withdrawal',
        # IDB Bank-Specific
        'BILL PMT',
        # CS Bank_Specific
        'Cross Border Credit Transfer',
        #banchile Bank-Specific
        'Wire Transfer Out', 'Rescate'
    ]
    
    # Problematic transactions that should also be excluded (unknown/unreliable data)
    PROBLEMATIC_TYPES = [
        'UNKNOWN',         # JPM - we don't know what this represents
        'Cost Adjustment', # JPM - could skew performance data
        'Accrued Int Pd',  # Don't include in formula (per user feedback)
        'Subscription',
        'Activity Within Your Acct'
        # Don't include in formula (per user feedback)
    ]
    
    # PHASE 1A/2A: Enhanced mappings for all 15 banks
    ENHANCED_TRANSACTION_MAPPINGS = {
        'CS': {
            'EXTERNAL_INFLOWS': [
                'Cross Border Credit Transfer', 'Transfer', 'Fiduciary call deposit',
                'Fiduciary call deposit - increase', 'Wire Transfer In'
            ],
            'EXTERNAL_OUTFLOWS': [
                'Expenses for money transfer', 'Cross Border Credit Transfer',
                'Fiduciary call deposit - reduction', 'Fiduciary call dep. - liquidation',
                'Wire Transfer Out'
            ],
            'TRADING_EXCLUDED': [
                'Securities purchase', 'Securities sale', 'Redemption',
                'Redemption of fund units', 'Issue of fund units'
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Stock dividend/spin-off', 'Foreign exchange spot transaction',
                'Equalisation payment'
            ]
        },
        'CSC': {
            'EXTERNAL_INFLOWS': [
                'Misc Cash Entry', 'Schwab ATM Rebate'
            ],
            'EXTERNAL_OUTFLOWS': [
                'Journal', 'Wire Sent', 'Visa Purchase', 'ATM Withdrawal'
            ],
            'TRADING_EXCLUDED': [
                'Buy', 'Sell', 'Full Redemption Adj'
            ],
            'PROBLEMATIC_EXCLUDED': []
        },
        'HSBC': {
            'EXTERNAL_INFLOWS': [
                'Wire Transfer Credit', 'Deposit'
            ],
            'EXTERNAL_OUTFLOWS': [
                'Wire Transfer Debit', 'Withdrawal', 'Wire Out'
            ],
            'TRADING_EXCLUDED': [
                'Security Redeemed', 'Purchase', 'Sale'
            ],
            'PROBLEMATIC_EXCLUDED': []
        },
        'JB': {
            'EXTERNAL_INFLOWS': [],
            'EXTERNAL_OUTFLOWS': [
                'Withdrawal', 'Swift payment (fax, letter)'
            ],
            'TRADING_EXCLUDED': [
                'Buy', 'Sell'
            ],
            'PROBLEMATIC_EXCLUDED': []
        },
        'JPM': {
            'EXTERNAL_INFLOWS': [
                'Misc Debit / Credit', 'ACH Deposit', 'Misc. Receipt'
            ],
            'EXTERNAL_OUTFLOWS': [
                'Misc. Disbursement', 'OUTGOING'
            ],
            'TRADING_EXCLUDED': [
                'Purchase', 'Sale', 'Redemption', 'Sales of Securities'
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Cost Adjustment', 'UNKNOWN', 'Accrued Int Pd', 'Accrued Int Rcv'
            ]
        },
        'MS': {
            'EXTERNAL_INFLOWS': [
                'Auto Bank Product Deposit', 'Bank Product Deposit'
            ],
            'EXTERNAL_OUTFLOWS': [
                'Debit Card', 'Funds Transferred', 'Bank Product Withdrawal'
            ],
            'TRADING_EXCLUDED': [
                'Redemption', 'Bought'
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Dividend Reinvestment', 'Exchange Received In', 'Exchange Deliver Out',
                'Return of Principal'
            ]
        },
        'VALLEY': {
            'EXTERNAL_INFLOWS': [
                'WIRE IN'
            ],
            'EXTERNAL_OUTFLOWS': [
                'WIRE OUT', 'WIRE OUT INTL', 'ACH DEBIT', 'ELECTRONIFIED CHECK',
                'MISCELLANEOUS FEES'
            ],
            'TRADING_EXCLUDED': [],
            'PROBLEMATIC_EXCLUDED': [
                'SECURITIES DEBIT'
            ]
        },
        'IDB': {
            'EXTERNAL_INFLOWS': [
                'Wire Transfer Credit'
            ],
            'EXTERNAL_OUTFLOWS': [
                'BILL PMT'
            ],
            'TRADING_EXCLUDED': [
                'Purchase', 'Sale'
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Final Maturity 1 USD United St atesTreasury Note/B'
            ]
        },
        'LO': {
            'EXTERNAL_INFLOWS': [
                'Deposit', 'Cross Border Credit Transfer'
            ],
            'EXTERNAL_OUTFLOWS': [
                'Withdrawal', 'Fees'
            ],
            'TRADING_EXCLUDED': [
                'Purchase', 'Sale'
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Other'
            ]
        },
        'SAFRA': {
            'EXTERNAL_INFLOWS': [],
            'EXTERNAL_OUTFLOWS': [],
            'TRADING_EXCLUDED': [],
            'PROBLEMATIC_EXCLUDED': [
                'unknown'
            ]
        },
        'PERSHING': {
            'EXTERNAL_INFLOWS': [],
            'EXTERNAL_OUTFLOWS': [],
            'TRADING_EXCLUDED': [
                'Buy', 'Purchase', 'Sell', 'Security Redeemed'
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Activity Within Your Acct'
            ]
        },
        'Pershing': {
            'EXTERNAL_INFLOWS': [],
            'EXTERNAL_OUTFLOWS': [],
            'TRADING_EXCLUDED': [
                'Buy', 'Purchase', 'Sell', 'Security Redeemed'
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Activity Within Your Acct'
            ]
        },
        'BANCHILE': {
            'EXTERNAL_INFLOWS': [
                'Aporte'
            ],
            'EXTERNAL_OUTFLOWS': [
                'Rescate'
            ],
            'TRADING_EXCLUDED': [],
            'PROBLEMATIC_EXCLUDED': [
                '--'
            ]
        },
        'ALT': {
            'EXTERNAL_INFLOWS': [],  # No external inflows for ALT
            'EXTERNAL_OUTFLOWS': [],  # No external outflows for ALT  
            'TRADING_EXCLUDED': [
                'Purchase',  # Alternative asset purchases (exclude from returns)
            ],
            'INVESTMENT_INCLUDED': [
                'Sale',          # Alternative asset sales (include in returns)
                'Income',        # Rental income, dividends from alternatives
                'Interest',      # Interest from alternative investments
                'Distribution'   # Distributions from private equity, etc.
            ],
            'PROBLEMATIC_EXCLUDED': []
        },
        'Citi': {
            'EXTERNAL_INFLOWS': [
                'Cash Deposit',                  # Client deposits
                'Funds Received',                # Wire transfers in
                'Auto Bank Product Deposit',     # Automatic deposits
                'Online Transfer'                # When positive amount
            ],
            'EXTERNAL_OUTFLOWS': [
                'Cash Withdrawal',               # ATM/branch withdrawals  
                'Funds Transferred',             # Wire transfers out
                'Bank Product Withdrawal',       # Account withdrawals
                'Adjustment - outside Commitment ', # Negative adjustments (with space)
                'Adjustment - outside Commitment'  # Negative adjustments (without space)
            ],
            'TRADING_EXCLUDED': [
                'Asset Purchased', 'Purchase', 'Bought',    # Buy transactions
                'Asset Sold', 'Sale', 'Sold', 'Redemption' # Sell transactions
            ],
            'PROBLEMATIC_EXCLUDED': [
                'Cost Adjustment',               # Accounting adjustments
                'Dividend Reinvestment',         # Automatic reinvestment
                'Accrued Int Pd', 'Accrued Int Rcv' # Accrual accounting
            ]
        }
    }
    
    def __init__(self):
        """Initialize classifier with tracking for unrecognized transactions."""
        self.unrecognized_transactions: Set[str] = set()
        self.enhanced_mode = True  # Phase 1A: Enable enhanced classification
        logger.info("CashFlowService initialized for Modified Dietz calculations (Phase 1A Enhanced)")
    
    # PHASE 1A: Bank-specific extraction methods
    def _get_bank_from_transaction(self, transaction: Transaction) -> str:
        """
        Extract bank code from transaction with fallback handling.
        
        Args:
            transaction: Transaction model instance
            
        Returns:
            Bank code (uppercase)
        """
        # Try different possible bank field locations
        bank = getattr(transaction, 'bank', None)
        if not bank and hasattr(transaction, 'asset'):
            bank = getattr(transaction.asset, 'bank', None)
        
        if bank:
            return str(bank).upper().strip()
        
        # Fallback: try to detect from other fields if needed
        logger.debug(f"Transaction missing bank field, defaulting to UNKNOWN")
        return 'UNKNOWN'
    
    def _extract_cs_transaction_type(self, transaction_type: str) -> str:
        """
        Extract clean transaction type from CS bank complex descriptions.
        
        CS transactions often have format: "Transaction Type \n Additional Info"
        Example: "Cash dividend \n1,385 SPDR S&P US DIV USD" -> "Cash dividend"
        
        Args:
            transaction_type: Raw CS transaction description
            
        Returns:
            Extracted transaction type
        """
        # Split by newline and take the first part
        parts = transaction_type.split('\n')
        if len(parts) > 1:
            extracted = parts[0].strip()
            logger.debug(f"CS extraction: '{transaction_type}' -> '{extracted}'")
            return extracted
        return transaction_type.strip()
    
    def _extract_idb_transaction_type(self, transaction_type: str) -> str:
        """
        Extract clean transaction type from IDB bank complex descriptions.
        
        IDB patterns:
        - "Interest Payment X.XX USD Unit ed States Treasury N" -> "Interest Payment"
        - "Purchase X.XX USD Unit ed States Treasury N" -> "Purchase"
        - "Wire Transfer Credit X.XX USD" -> "Wire Transfer Credit"
        
        Args:
            transaction_type: Raw IDB transaction description
            
        Returns:
            Extracted transaction type
        """
        # Define patterns in order of specificity
        patterns = [
            (r'^(Annual Service Fee - Hold Mail)', r'\1'),
            (r'^(Annual Maintenance Fee)', r'\1'),
            (r'^(Annual Management Fee)', r'\1'),
            (r'^(Interest Payment)', r'\1'),
            (r'^(Periodic Fee)', r'\1'),
            (r'^(Wire Transfer Credit)', r'\1'),
            (r'^(BILL PMT)', r'\1'),
            (r'^(Purchase)', r'\1'),
            (r'^(Sale)', r'\1'),
        ]
        
        for pattern, replacement in patterns:
            match = re.match(pattern, transaction_type, re.IGNORECASE)
            if match:
                extracted = match.group(1)
                logger.debug(f"IDB extraction: '{transaction_type}' -> '{extracted}'")
                return extracted
        
        return transaction_type.strip()
    
    def _extract_pershing_transaction_type(self, transaction_type: str) -> str:
        """
        Extract clean transaction type from Pershing bank complex descriptions.
        
        Pershing patterns:
        - "Buy 150000.00000 Parvalue Of Orcl5903544 At 97.0000" -> "Buy"
        - "Purchase 161 Shares of SPDR Gold Shares @ 308.8199" -> "Purchase"
        
        Args:
            transaction_type: Raw Pershing transaction description
            
        Returns:
            Extracted transaction type
        """
        patterns = [
            (r'^(Buy)\s+[\d,]+\.?\d*\s+Parvalue\s+Of\s+\w+\s+At\s+[\d.]+', r'\1'),
            (r'^(Purchase)\s+\d+\s+Shares\s+of', r'\1'),
            (r'^(Sell)\s+[\d,]+\.?\d*\s+Parvalue\s+Of', r'\1'),
        ]
        
        for pattern, replacement in patterns:
            match = re.match(pattern, transaction_type, re.IGNORECASE)
            if match:
                extracted = match.group(1)
                logger.debug(f"Pershing extraction: '{transaction_type}' -> '{extracted}'")
                return extracted
        
        return transaction_type.strip()
    
    def _extract_transaction_type(self, transaction_type: str, bank: str) -> str:
        """
        Extract clean transaction type with bank-specific logic.
        
        Args:
            transaction_type: Raw transaction type string
            bank: Bank code
            
        Returns:
            Extracted transaction type
        """
        if bank == 'CS':
            return self._extract_cs_transaction_type(transaction_type)
        elif bank == 'IDB':
            return self._extract_idb_transaction_type(transaction_type)
        elif bank in ['Pershing', 'PERSHING']:
            return self._extract_pershing_transaction_type(transaction_type)
        return transaction_type.strip()
    
    def is_external_cash_flow_enhanced(self, transaction: Transaction) -> bool:
        """
        PHASE 1A/1B: Enhanced classification with comprehensive bank mappings.
        
        Args:
            transaction: Django Transaction model instance
            
        Returns:
            bool: True if should be excluded from Modified Dietz calculation
        """
        bank = self._get_bank_from_transaction(transaction)
        tx_type = getattr(transaction, 'transaction_type', '').strip()
        
        if not tx_type:
            return True  # Exclude empty transaction types
        
        # Extract clean transaction type
        clean_tx_type = self._extract_transaction_type(tx_type, bank)
        
        # Check enhanced mappings for this bank
        if bank in self.ENHANCED_TRANSACTION_MAPPINGS:
            bank_mappings = self.ENHANCED_TRANSACTION_MAPPINGS[bank]
            
            # Check all exclusion categories
            for category in ['EXTERNAL_INFLOWS', 'EXTERNAL_OUTFLOWS', 'TRADING_EXCLUDED', 'PROBLEMATIC_EXCLUDED']:
                category_list = bank_mappings.get(category, [])
                # Case-insensitive comparison
                if any(clean_tx_type.lower() == item.lower() for item in category_list):
                    logger.debug(f"Enhanced exclusion: {bank} {clean_tx_type} -> {category}")
                    return True  # Exclude from Modified Dietz
        
        # Fallback to original logic for unknown banks
        is_excluded = (clean_tx_type in self.INFLOW_TYPES or 
                      clean_tx_type in self.OUTFLOW_TYPES or
                      clean_tx_type in self.PROBLEMATIC_TYPES)
        
        return is_excluded
    
    # PHASE 1B: Parallel testing and comparison methods
    def compare_classification_methods(self, transactions: List[Transaction]) -> Dict[str, Any]:
        """
        Compare old vs enhanced classification methods for analysis.
        
        Args:
            transactions: List of Transaction objects to analyze
            
        Returns:
            Comprehensive comparison results
        """
        results = {
            'total_transactions': len(transactions),
            'old_external_flows': 0,
            'enhanced_external_flows': 0,
            'classification_changes': 0,
            'bank_breakdown': {},
            'sample_changes': [],
            'improvement_details': {}
        }
        
        for tx in transactions:
            bank = self._get_bank_from_transaction(tx)
            tx_type = getattr(tx, 'transaction_type', '').strip()
            
            # Initialize bank stats
            if bank not in results['bank_breakdown']:
                results['bank_breakdown'][bank] = {
                    'total': 0, 'old_external': 0, 'enhanced_external': 0
                }
            
            results['bank_breakdown'][bank]['total'] += 1
            
            # Get old classification (original logic)
            old_excluded = (tx_type in self.INFLOW_TYPES or 
                           tx_type in self.OUTFLOW_TYPES or
                           tx_type in self.PROBLEMATIC_TYPES)
            
            # Get enhanced classification
            enhanced_excluded = self.is_external_cash_flow_enhanced(tx)
            
            # Count results
            if old_excluded:
                results['old_external_flows'] += 1
                results['bank_breakdown'][bank]['old_external'] += 1
            
            if enhanced_excluded:
                results['enhanced_external_flows'] += 1
                results['bank_breakdown'][bank]['enhanced_external'] += 1
            
            # Track changes
            if old_excluded != enhanced_excluded:
                results['classification_changes'] += 1
                
                # Store sample changes (first 10)
                if len(results['sample_changes']) < 10:
                    results['sample_changes'].append({
                        'bank': bank,
                        'transaction_type': tx_type[:50] + '...' if len(tx_type) > 50 else tx_type,
                        'old_result': old_excluded,
                        'enhanced_result': enhanced_excluded
                    })
        
        # Calculate improvement metrics
        total = results['total_transactions']
        if total > 0:
            results['improvement_details'] = {
                'old_detection_rate': (results['old_external_flows'] / total) * 100,
                'enhanced_detection_rate': (results['enhanced_external_flows'] / total) * 100,
                'improvement_percentage_points': ((results['enhanced_external_flows'] - results['old_external_flows']) / total) * 100,
                'change_rate': (results['classification_changes'] / total) * 100
            }
        
        return results
    
    def is_external_cash_flow(self, transaction: Transaction) -> bool:
        """
        Determine if transaction should be EXCLUDED from Modified Dietz.
        
        Excludes: External cash flows + problematic/unknown transactions
        Includes: All known investment performance (income, fees, trading, bond activities)
        
        Args:
            transaction: Django Transaction model instance
            
        Returns:
            bool: True if should be excluded from Modified Dietz calculation
        """
        tx_type = getattr(transaction, 'transaction_type', '').strip()
        
        if not tx_type:
            # No transaction type - exclude (safer approach)
            return True
        
        # PHASE 1A: Use enhanced classification if enabled
        if self.enhanced_mode:
            return self.is_external_cash_flow_enhanced(transaction)
            
        # Original logic (for fallback/comparison)
        is_excluded = (tx_type in self.INFLOW_TYPES or 
                      tx_type in self.OUTFLOW_TYPES or
                      tx_type in self.PROBLEMATIC_TYPES)
        
        return is_excluded
    
    def is_external_cash_flow_from_dict(self, transaction: Dict[str, Any]) -> bool:
        """
        Determine if transaction dict should be EXCLUDED from Modified Dietz.
        For backward compatibility with dictionary-based transaction data.
        
        Args:
            transaction: Transaction dictionary
            
        Returns:
            bool: True if should be excluded from Modified Dietz calculation
        """
        tx_type = transaction.get('transaction_type', '').strip()
        
        if not tx_type:
            # No transaction type - exclude (safer approach)
            return True
            
        # Check if it's an external cash flow or problematic transaction
        is_excluded = (tx_type in self.INFLOW_TYPES or 
                      tx_type in self.OUTFLOW_TYPES or
                      tx_type in self.PROBLEMATIC_TYPES)
        
        return is_excluded
    
    def is_external_cash_flow_with_logging(self, transaction: Transaction) -> bool:
        """
        Enhanced version that logs unrecognized transactions for dictionary updates.
        
        Args:
            transaction: Django Transaction model instance
            
        Returns:
            bool: True if should be excluded from Modified Dietz calculation
        """
        tx_type = getattr(transaction, 'transaction_type', '').strip()
        
        if not tx_type:
            # No transaction type - exclude but don't log (common case)
            return True
            
        # Check if it's an external cash flow or problematic transaction
        is_excluded = (tx_type in self.INFLOW_TYPES or 
                      tx_type in self.OUTFLOW_TYPES or
                      tx_type in self.PROBLEMATIC_TYPES)
        
        if not is_excluded:
            # This transaction will be INCLUDED in Modified Dietz calculation
            # Check if we've seen this transaction type before
            if not hasattr(self, '_known_investment_types'):
                self._known_investment_types = self._get_known_investment_types()
            
            if tx_type not in self._known_investment_types:
                # New/unrecognized investment transaction type
                self.unrecognized_transactions.add(tx_type)
        
        return is_excluded
    
    def _get_known_investment_types(self) -> Set[str]:
        """Get all known investment transaction types for comparison."""
        return {
            # Income Types
            'Cash Dividend', 'Pr Yr Cash Div', 'Dividend', 'Foreign Dividend', 'Dividends',
            'Bond Interest', 'Credit Interest', 'Bond Interest Recieved', 'Interest',
            'U.S. Government Interest', 'Corporate Interest', 'Interest', 'Interest Received',
            'Interest Income', 'Income', 'SECURITIES CREDIT', 'Cash Liquidation', 'Foreign Interest', 'Bond Interest Received',
            'Received Interest',  # LO Bank
            'Cash dividend',  # CS Bank (extracted)
            'Interest Payment',  # IDB Bank (extracted)
            'Interest payment',  # CS/LO Bank (extracted)
            'Payment',  # CS/LO/IDB Bank (extracted)
            
            # Fee Types  
            'NRA Tax Adj', 'NRA Tax', 'Pr Yr NRA Tax', 'Taxes', 'Tax Withholding',
            'Non-resident Alien Tax', 'Service Fee', 'MISCELLANEOS FEES', 'BALANCE FEES',
            'Management fee', 'Safekeeping fees',  # CS Bank
            'Annual Service Fee - Hold Mail', 'Annual Maintenance Fee', 'Annual Management Fee',  # IDB Bank
            'Periodic Fee',  # IDB Bank (extracted)
            'Fees',  # LO Bank
            'Investment Debit 0',  # Safra Bank
            
            # Trading Types
            'Buy', 'Sell', 'Purchase', 'Sale', 'Sales of Securities', 'Bank Product Withdrawal',
            
            # Bond Activities (included per user feedback)
            'Redemption', 'Full Redemption Adj', 'Security Redeemed',
            
            # Other Investment Activities
            'Dividend Reinvestment',
            'Stock dividend/spin-off',  # CS Bank - excluded type
            'Other',  # LO Bank - excluded type
            'Accured Int Pd'
        }
    
    def get_cash_flow_amount(self, transaction: Transaction) -> float:
        """
        Get the external cash flow amount from Django Transaction model.
        
        Args:
            transaction: Django Transaction model instance
            
        Returns:
            float: Cash flow amount (positive for inflows, negative for outflows)
        """
        tx_type = getattr(transaction, 'transaction_type', '').strip()
        amount = getattr(transaction, 'amount', 0)
        
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            logger.warning(f"Invalid amount for transaction {tx_type}: {amount}")
            return 0.0
        
        # Only process external cash flows, not problematic types
        if tx_type in self.OUTFLOW_TYPES:
            return -abs(amount)  # Ensure it's negative
        elif tx_type in self.INFLOW_TYPES:
            return abs(amount)   # Ensure it's positive
        else:
            return 0.0  # Not an external cash flow (or is problematic type)
    
    def get_cash_flow_amount_from_dict(self, transaction: Dict[str, Any]) -> float:
        """
        Get the external cash flow amount from transaction dictionary.
        For backward compatibility with dictionary-based transaction data.
        
        Args:
            transaction: Transaction dictionary
            
        Returns:
            float: Cash flow amount (positive for inflows, negative for outflows)
        """
        tx_type = transaction.get('transaction_type', '').strip()
        amount = transaction.get('amount', 0)
        
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            logger.warning(f"Invalid amount for transaction {tx_type}: {amount}")
            return 0.0
        
        # Only process external cash flows, not problematic types
        if tx_type in self.OUTFLOW_TYPES:
            return -abs(amount)  # Ensure it's negative
        elif tx_type in self.INFLOW_TYPES:
            return abs(amount)   # Ensure it's positive
        else:
            return 0.0  # Not an external cash flow (or is problematic type)
    
    def log_unrecognized_transactions(self):
        """Log unrecognized investment transaction types for dictionary updates."""
        if self.unrecognized_transactions:
            logger.info("=== UNRECOGNIZED INVESTMENT TRANSACTION TYPES FOR MODIFIED DIETZ ===")
            logger.info("These transactions were INCLUDED in Modified Dietz calculation as investment performance:")
            for tx_type in sorted(self.unrecognized_transactions):
                logger.info(f"  ℹ️  New investment transaction type: '{tx_type}'")
            logger.info("If any of these should be EXTERNAL FLOWS or PROBLEMATIC, please update the classifier")
            logger.info("=== END UNRECOGNIZED TRANSACTIONS ===")
            self.unrecognized_transactions.clear()
    
    def get_excluded_types(self) -> Dict[str, List[str]]:
        """Get all transaction types excluded from Modified Dietz calculation."""
        return {
            'external_inflows': self.INFLOW_TYPES.copy(),
            'external_outflows': self.OUTFLOW_TYPES.copy(),
            'problematic_types': self.PROBLEMATIC_TYPES.copy()
        }
    
    def classify_transactions_for_client(self, client: str, start_date: str, end_date: str) -> Dict[str, List[Transaction]]:
        """
        Classify all transactions for a client during a period.
        
        Args:
            client: Client code
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dict with categorized transactions
        """
        try:
            transactions = Transaction.objects.filter(
                client=client,
                transaction_date__gte=start_date,
                transaction_date__lte=end_date
            ).select_related('asset')
            
            external_flows = []
            investment_activities = []
            problematic = []
            
            for tx in transactions:
                tx_type = tx.transaction_type
                
                if tx_type in self.INFLOW_TYPES or tx_type in self.OUTFLOW_TYPES:
                    external_flows.append(tx)
                elif tx_type in self.PROBLEMATIC_TYPES:
                    problematic.append(tx)
                else:
                    investment_activities.append(tx)
            
            return {
                'external_flows': external_flows,
                'investment_activities': investment_activities,
                'problematic': problematic,
                'total_count': len(transactions)
            }
            
        except Exception as e:
            logger.error(f"Error classifying transactions for {client}: {str(e)}")
            return {
                'external_flows': [],
                'investment_activities': [],
                'problematic': [],
                'total_count': 0
            }