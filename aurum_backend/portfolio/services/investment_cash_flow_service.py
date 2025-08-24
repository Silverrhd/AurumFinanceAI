"""
Investment cash flow calculation service for AurumFinance.
Migrated from ProjectAurum calculations/investment_cash_flow.py with Django ORM integration.

This module calculates actual investment cash flows (income - fees) for display
in weekly comparison sections, separate from Modified Dietz external cash flows.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Union
from ..models import Transaction

logger = logging.getLogger(__name__)

class InvestmentCashFlowService:
    """
    Calculate investment cash flows for weekly comparison display.
    
    Includes: Investment income (dividends, interest) minus fees and taxes
    Excludes: External flows, trading activity, bond maturities, reinvestments
    
    Migrated from ProjectAurum with full Django ORM integration.
    """
    
    # Comprehensive bank-specific transaction type mappings
    TRANSACTION_MAPPINGS = {
        'CSC': {
            'DIVIDEND_INCOME': ['Cash Dividend', 'Pr Yr Cash Div','Cash Dividend Received', 'Qualified Dividend'],
            'INTEREST_INCOME': ['Bond Interest', 'Credit Interest'],
            'OTHER_INCOME': ['Cash Liquidation'],
            'TAX_FEES': ['NRA Tax Adj', 'NRA Tax', 'Pr Yr NRA Tax', 'Non-resident Alien Tax'],
            'SERVICE_FEES': ['Service Fee'],
            # EXCLUDED CATEGORIES (documented for reference)
            'TRADING_BUY': ['Buy'],
            'TRADING_SELL': ['Sell', 'Full Redemption Adj'],
            'EXTERNAL_FLOWS': ['Journal', 'Wire Sent', 'Visa Purchase', 'ATM Withdrawl', 
                              'Misc Cash Entry', 'Schwab ATM Rebate']
        },
        'HSBC': {
            'DIVIDEND_INCOME': ['Foreign Security Dividend Received'],
            'INTEREST_INCOME': ['Bond Interest Recieved', 'Bond Interest Received'],
            'TAX_FEES': ['Fee On Foreign Dividend   Withheld At The Source', 
                         'Foreign Tax Withheld At   The Source'],
            'TRADING_SELL': ['Security Redeemed']  # Bond maturity = trading
        },
        'JB': {
            'OTHER_INCOME': ['Income'],  # General income for JB
            'TRADING_BUY': ['Buy'],
            'TRADING_SELL': ['Sell']
        },
        'JPM': {
            'DIVIDEND_INCOME': ['Dividend', 'Dividends', 'Foreign Dividend'],
            'INTEREST_INCOME': ['U.S. Government Interest', 'Corporate Interest', 
                               'Interest', 'Interest Received', 'Interest Income', 'Foreign Interest'],
            'TAX_FEES': ['Taxes', 'Tax Withholding', 'FATCA Tax Withheld'],
            'SERVICE_FEES': ['Fees', 'Fees and Commissions'],
            'TRADING_BUY': ['Purchase'],
            'TRADING_SELL': ['Sale', 'Redemption', 'Sales of Securities'],
            'EXTERNAL_FLOWS': ['Misc. Disbursement', 'Misc Debit / Credit', 'ACH Deposit', 'Misc. Receipt', 'Misc.Receipt', 'Misc Receipt'],
            'OTHER_EXCLUDED': ['Cost Adjustment', 'UNKNOWN', 'Accrued Int Pd', 'Accrued Int Rcv']
        },
        'MS': {
            'DIVIDEND_INCOME': ['Dividend', 'Interest', 'Qualified Dividend', 'Dividend - Adjustment'],
            'INTEREST_INCOME': ['Interest Income'],
            'TAX_FEES': ['Tax Withholding', 'Tax'],
            'SERVICE_FEES': ['Service Fee'],
            'TRADING_BUY': ['Purchase'],
            'TRADING_SELL': ['Sale', 'Redemption', 'Sold'],
            'EXTERNAL_FLOWS': ['Debit Card', 'Funds Transferred', 'Auto Bank Product Deposit', 
                              'Bank Product Withdrawal', 'Bank Product Deposit'],
            'OTHER_EXCLUDED': ['Dividend Reinvestment', 'Exchange Received In', 'Exchange Deliver Out', 
                               'Bought', 'Return of Principal']
        },
        'PERSHING': {
            'DIVIDEND_INCOME': ['Cash Dividend Received'],
            'INTEREST_INCOME': ['Bond Interest Received'],
            'TAX_FEES': ['Non-resident Alien Tax'],
            'OTHER_EXCLUDED': ['Activity Within Your Acct']
        },
        'VALLEY': {
            'OTHER_INCOME': ['SECURITIES CREDIT'],
            'SERVICE_FEES': ['MISCELLANEOS FEES', 'BALANCE FEES', 'MISCELLANEOUS FEES'],
            'EXTERNAL_FLOWS': ['WIRE OUT', 'WIRE IN', 'WIRE OUT INTL']
        },
        'CS': {
            'DIVIDEND_INCOME': ['Cash dividend', 'Payment', 'Interest payment', 'Reversal Cash dividend'],
            'INTEREST_INCOME': ['Coupons', 'Reversal Interest payment'],
            'SERVICE_FEES': ['Management fee', 'Safekeeping fees', 'Lending commission', 'All-in fee'],
            'TRADING_BUY': ['Securities purchase', 'Reversal Securities purchase'],
            'TRADING_SELL': ['Securities sale', 'Redemption', 'Redemption of fund units'],
            'EXTERNAL_FLOWS': ['Cross Border Credit Transfer', 'Transfer', 'Fiduciary call deposit', 
                               'Fiduciary call deposit - reduction', 'Fiduciary call deposit - increase',
                               'Fid. call deposit int. settlemt.', 'Expenses for money transfer',
                               'Fiduciary call dep. - liquidation'],
            'OTHER_EXCLUDED': ['Stock dividend/spin-off', 'Foreign exchange spot transaction',
                               'Issue of fund units', 'Equalisation payment']
        },
        'IDB': {
            'INTEREST_INCOME': ['Interest Payment'],
            'SERVICE_FEES': ['Annual Service Fee - Hold Mail', 'Annual Maintenance Fee', 'Annual Management Fee', 'Periodic Fee'],
            'TRADING_BUY': ['Purchase'],
            'TRADING_SELL': ['Sale'],
            'EXTERNAL_FLOWS': ['Wire Transfer Credit', 'BILL PMT'],
            'OTHER_EXCLUDED': ['Final Maturity 1 USD United St atesTreasury Note/B']
        },
        'LO': {
            'INTEREST_INCOME': ['Received Interest', 'Interest payment', 'Cash dividend', 'Payment'],
            'EXTERNAL_FLOWS': ['Deposit', 'Withdrawal', 'Cross Border Credit Transfer'],
            'TRADING_BUY': ['Purchase'],
            'TRADING_SELL': ['Sale'],
            'SERVICE_FEES': ['Fees'],
            'OTHER_EXCLUDED': ['Other']
        },
        'SAFRA': {
            'SERVICE_FEES': ['Investment Debit 0']
        },
        # Lowercase versions for case sensitivity issues
        'Banchile': {
            'EXTERNAL_FLOWS': ['Aporte', 'Rescate'],
            'OTHER_EXCLUDED': ['--']
        },
        'Pershing': {
            'DIVIDEND_INCOME': ['Cash Dividend Received', 'Foreign Bond Interest'],
            'INTEREST_INCOME': ['Bond Interest Received'],
            'TAX_FEES': ['Non-resident Alien Tax'],
            'TRADING_BUY': ['Buy', 'Purchase'],
            'OTHER_EXCLUDED': ['Activity Within Your Acct', 'Security Redeemed']
        },
        'Safra': {
            'SERVICE_FEES': ['Investment Debit 0', 'Account Maintenance Fee  Account Maintenance Fee 0', 'CUSTODY FEE                    AS OF 06/30/25  Investment Debit 0'],
            'OTHER_EXCLUDED': ['unknown']
        },
        'Valley': {
            'OTHER_INCOME': ['SECURITIES CREDIT'],
            'SERVICE_FEES': ['MISCELLANEOS FEES', 'BALANCE FEES', 'MISCELLANEOUS FEES'],
            'EXTERNAL_FLOWS': ['WIRE OUT', 'WIRE IN', 'WIRE OUT INTL', 'ACH DEBIT', 'ELECTRONIFIED CHECK'],
            'OTHER_EXCLUDED': ['SECURITIES DEBIT']
        },
         'ALT': {
            'OTHER_INCOME': ['SECURITIES CREDIT'],
            'SERVICE_FEES': ['MISCELLANEOS FEES', 'BALANCE FEES', 'MISCELLANEOUS FEES'],
            'TRADING_BUY': ['purchase', 'Purchase'],
            'TRADING_SELL': ['sale', 'sold', 'Sale'],
            'EXTERNAL_FLOWS': ['WIRE OUT', 'WIRE IN', 'WIRE OUT INTL', 'ACH DEBIT', 'ELECTRONIFIED CHECK'],
            'OTHER_EXCLUDED': ['SECURITIES DEBIT']
        }
    }
    
    # Categories that contribute to investment cash flow
    INCOME_CATEGORIES = ['DIVIDEND_INCOME', 'INTEREST_INCOME', 'OTHER_INCOME']
    FEE_CATEGORIES = ['TAX_FEES', 'SERVICE_FEES']
    
    def __init__(self):
        """Initialize the investment cash flow service."""
        self.unrecognized_transactions = {}  # Track for logging
        logger.debug("InvestmentCashFlowService initialized")
    
    def _get_bank_from_transaction(self, transaction: Union[Transaction, Dict[str, Any]]) -> str:
        """
        Extract bank code from transaction, with fallback handling.
        
        Args:
            transaction: Transaction model or dictionary
            
        Returns:
            Bank code (uppercase)
        """
        # Handle Django model
        if hasattr(transaction, '_meta'):
            # Try different possible bank field locations
            bank = getattr(transaction, 'bank', None)
            if not bank and hasattr(transaction, 'asset'):
                bank = getattr(transaction.asset, 'bank', None)
            if not bank:
                bank = ''
        else:
            # Handle dictionary
            bank = transaction.get('bank', '')
        
        if bank:
            return str(bank).upper().strip()
        
        # Fallback: try to detect from other fields if needed
        logger.warning(f"Transaction missing bank field: {transaction}")
        return 'UNKNOWN'
    
    def _get_transaction_type(self, transaction: Union[Transaction, Dict[str, Any]]) -> str:
        """
        Extract transaction type from transaction.
        
        Args:
            transaction: Transaction model or dictionary
            
        Returns:
            Transaction type string
        """
        if hasattr(transaction, '_meta'):
            # Django model - try different possible field names
            return getattr(transaction, 'transaction_type', '') or getattr(transaction, 'type', '') or ''
        else:
            # Dictionary
            return transaction.get('transaction_type', '') or transaction.get('type', '') or ''
    
    def _get_amount(self, transaction: Union[Transaction, Dict[str, Any]]) -> Any:
        """
        Extract amount from transaction.
        
        Args:
            transaction: Transaction model or dictionary
            
        Returns:
            Transaction amount
        """
        if hasattr(transaction, '_meta'):
            # Django model - use correct field name
            return getattr(transaction, 'amount', 0)
        else:
            # Dictionary
            return transaction.get('amount', 0)
    
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
        
        IDB patterns identified:
        - "Interest Payment X.XX USD Unit ed States Treasury N" -> "Interest Payment"
        - "Periodic Fee X.XX USD" -> "Periodic Fee"  
        - "Annual Service Fee - Hold Mail X.XX USD" -> "Annual Service Fee - Hold Mail"
        - "Annual Maintenance Fee X.XX USD" -> "Annual Maintenance Fee"
        - "Annual Management Fee X.XX USD" -> "Annual Management Fee"
        - "Purchase X.XX USD Unit ed States Treasury N" -> "Purchase"
        - "Sale X.XX USD Unit ed States Treasury N" -> "Sale"
        - "Wire Transfer Credit X.XX USD" -> "Wire Transfer Credit"
        - "BILL PMT X.XX USD" -> "BILL PMT"
        
        Args:
            transaction_type: Raw IDB transaction description
            
        Returns:
            Extracted transaction type
        """
        # Define patterns in order of specificity (most specific first)
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
            (r'^(Payment)', r'\1'),
            (r'^(Cross Border Credit Transfer)', r'\1'),
        ]
        
        for pattern, replacement in patterns:
            match = re.match(pattern, transaction_type, re.IGNORECASE)
            if match:
                extracted = match.group(1)
                logger.debug(f"IDB extraction: '{transaction_type}' -> '{extracted}'")
                return extracted
        
        # If no pattern matches, return original
        logger.debug(f"IDB no pattern match: '{transaction_type}' -> '{transaction_type}'")
        return transaction_type.strip()
    
    def _extract_pershing_transaction_type(self, transaction_type: str) -> str:
        """
        Extract clean transaction type from Pershing bank complex descriptions.
        
        Pershing patterns identified:
        - "Buy 150000.00000 Parvalue Of Orcl5903544 At 97.0000" -> "Buy"
        - "Buy 100000.00000 Parvalue Of Pnc5918799 At 100.3750" -> "Buy"
        - "Purchase 161 Shares of SPDR Gold Shares @ 308.8199" -> "Purchase"
        - "Security Redeemed" -> "Security Redeemed" (no change)
        
        Args:
            transaction_type: Raw Pershing transaction description
            
        Returns:
            Extracted transaction type
        """
        # Define patterns in order of specificity
        patterns = [
            # Enhanced Buy pattern to catch all variations
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
        
        # If no pattern matches, return original
        logger.debug(f"Pershing no pattern match: '{transaction_type}' -> '{transaction_type}'")
        return transaction_type.strip()
    
    def _categorize_transaction(self, transaction_type: str, bank: str) -> Optional[str]:
        """
        Categorize transaction type based on bank mappings.
        
        Args:
            transaction_type: Raw transaction type string
            bank: Bank code
            
        Returns:
            Category string or None if unrecognized
        """
        transaction_type = transaction_type.strip()
        logger.debug(f"Categorizing transaction: bank='{bank}', type='{transaction_type}' (length: {len(transaction_type)})")
        
        # Apply bank-specific extraction before categorization
        if bank == 'CS':
            transaction_type = self._extract_cs_transaction_type(transaction_type)
        elif bank == 'IDB':
            transaction_type = self._extract_idb_transaction_type(transaction_type)
        elif bank == 'Pershing' or bank == 'PERSHING':
            transaction_type = self._extract_pershing_transaction_type(transaction_type)
        
        # Handle unknown/missing bank with generic fallback categorization
        if bank not in self.TRANSACTION_MAPPINGS or bank == 'UNKNOWN':
            return self._categorize_generic_transaction(transaction_type, bank)
        
        bank_mappings = self.TRANSACTION_MAPPINGS[bank]
        
        # Search through all categories for this transaction type
        logger.debug(f"Looking for transaction type '{transaction_type}' in bank {bank} mappings")
        
        # SPECIFIC DEBUG FOR MISC. RECEIPT ISSUE
        if transaction_type == 'Misc. Receipt' and bank == 'JPM':
            logger.info(f"🔍 DEBUGGING MISC. RECEIPT: bank='{bank}', type='{transaction_type}' (len={len(transaction_type)})")
            logger.info(f"🔍 JPM EXTERNAL_FLOWS list: {bank_mappings.get('EXTERNAL_FLOWS', [])}")
            logger.info(f"🔍 Is 'Misc. Receipt' in EXTERNAL_FLOWS? {'Misc. Receipt' in bank_mappings.get('EXTERNAL_FLOWS', [])}")
        
        for category, transaction_types in bank_mappings.items():
            if transaction_type in transaction_types:
                logger.debug(f"Found match: '{transaction_type}' in {category}")
                return category
        
        # Track unrecognized transaction type for this bank
        logger.debug(f"Transaction type '{transaction_type}' not found in {bank} mappings")
        if bank not in self.unrecognized_transactions:
            self.unrecognized_transactions[bank] = {}
        if 'UNRECOGNIZED_TYPES' not in self.unrecognized_transactions[bank]:
            self.unrecognized_transactions[bank]['UNRECOGNIZED_TYPES'] = []
        self.unrecognized_transactions[bank]['UNRECOGNIZED_TYPES'].append(transaction_type)
        
        return None
    
    def _categorize_generic_transaction(self, transaction_type: str, bank: str) -> Optional[str]:
        """
        Fallback categorization for transactions without specific bank mappings.
        Used for legacy data and tests.
        
        Args:
            transaction_type: Transaction type string
            bank: Bank code (for logging)
            
        Returns:
            Category string or None if unrecognized
        """
        transaction_type_lower = transaction_type.lower()
        
        # Generic income patterns
        if ('dividend' in transaction_type_lower or 
            'interest' in transaction_type_lower or 
            'income' in transaction_type_lower):
            return 'DIVIDEND_INCOME'
        
        # Generic fee patterns  
        if ('fee' in transaction_type_lower or 
            'tax' in transaction_type_lower):
            return 'TAX_FEES'
        
        # Generic trading patterns (excluded from investment cash flow)
        if ('buy' in transaction_type_lower or 
            'purchase' in transaction_type_lower or
            'sell' in transaction_type_lower or 
            'sale' in transaction_type_lower):
            return 'TRADING_BUY'  # Will be excluded
            
        # Track unrecognized for unknown banks
        if bank == 'UNKNOWN':
            if bank not in self.unrecognized_transactions:
                self.unrecognized_transactions[bank] = {}
            if 'GENERIC_UNRECOGNIZED' not in self.unrecognized_transactions[bank]:
                self.unrecognized_transactions[bank]['GENERIC_UNRECOGNIZED'] = []
            self.unrecognized_transactions[bank]['GENERIC_UNRECOGNIZED'].append(transaction_type)
        
        return None
    
    def _validate_amount(self, amount: Any, transaction: Union[Transaction, Dict[str, Any]]) -> Optional[float]:
        """
        Validate and convert transaction amount to float.
        
        Args:
            amount: Transaction amount (any type)
            transaction: Full transaction for logging
            
        Returns:
            Valid float amount or None if invalid
        """
        try:
            if amount is None or amount == '':
                return 0.0
            return float(amount)
        except (ValueError, TypeError) as e:
            tx_type = self._get_transaction_type(transaction)
            bank = self._get_bank_from_transaction(transaction)
            logger.debug(f"Skipping transaction with invalid amount: {bank} {tx_type} amount='{amount}' ({e})")
            return None
    
    def calculate_investment_cash_flows_from_models(self, transactions: List[Transaction]) -> float:
        """
        Calculate net investment cash flows from Django Transaction models.
        
        Formula: (Dividends + Interest + Other Income) - (Taxes + Fees)
        
        Args:
            transactions: List of Django Transaction models
            
        Returns:
            Net investment cash flow amount
        """
        if not transactions:
            return 0.0
        
        logger.debug(f"Calculating investment cash flows for {len(transactions)} Transaction models")
        return self._calculate_investment_cash_flows_internal(transactions)
    
    def calculate_investment_cash_flows_from_dicts(self, transactions: List[Dict[str, Any]]) -> float:
        """
        Calculate net investment cash flows from transaction dictionaries.
        
        Formula: (Dividends + Interest + Other Income) - (Taxes + Fees)
        
        Args:
            transactions: List of transaction dictionaries
            
        Returns:
            Net investment cash flow amount
        """
        if not transactions:
            return 0.0
        
        logger.debug(f"Calculating investment cash flows for {len(transactions)} transaction dicts")
        return self._calculate_investment_cash_flows_internal(transactions)
    
    def calculate_investment_cash_flows(self, transactions: Union[List[Transaction], List[Dict[str, Any]]]) -> float:
        """
        Calculate net investment cash flows with auto-detection of data type.
        
        Formula: (Dividends + Interest + Other Income) - (Taxes + Fees)
        
        Args:
            transactions: List of Django Transaction models or dictionaries
            
        Returns:
            Net investment cash flow amount
        """
        if not transactions:
            return 0.0
        
        # Auto-detect type by checking first item
        first_item = transactions[0]
        if hasattr(first_item, '_meta'):  # Django model
            return self.calculate_investment_cash_flows_from_models(transactions)
        else:  # Dictionary
            return self.calculate_investment_cash_flows_from_dicts(transactions)
    
    def _calculate_investment_cash_flows_internal(self, transactions: List[Union[Transaction, Dict[str, Any]]]) -> float:
        """
        Internal method to calculate investment cash flows.
        
        Args:
            transactions: List of transactions (models or dicts)
            
        Returns:
            Net investment cash flow amount
        """
        total_income = 0.0
        total_fees = 0.0
        processed_count = 0
        excluded_count = 0
        skipped_count = 0
        
        logger.debug(f"Calculating investment cash flows for {len(transactions)} transactions")
        
        for tx in transactions:
            transaction_type = self._get_transaction_type(tx).strip()
            amount_raw = self._get_amount(tx)
            bank = self._get_bank_from_transaction(tx)
            
            if not transaction_type:
                continue
            
            # Validate amount
            amount = self._validate_amount(amount_raw, tx)
            if amount is None:
                skipped_count += 1
                continue
                
            # Categorize the transaction
            category = self._categorize_transaction(transaction_type, bank)
            
            if category in self.INCOME_CATEGORIES:
                # Add income (always positive)
                income_amount = abs(amount)
                total_income += income_amount
                processed_count += 1
                logger.debug(f"  Income: {bank} {transaction_type} = +${income_amount:.2f}")
                
            elif category in self.FEE_CATEGORIES:
                # Subtract fees (always positive, will be subtracted)
                fee_amount = abs(amount)
                total_fees += fee_amount
                processed_count += 1
                logger.debug(f"  Fee: {bank} {transaction_type} = -${fee_amount:.2f}")
                
            elif category is not None:
                # Recognized but excluded (trading, external flows, etc.)
                excluded_count += 1
                logger.debug(f"  Excluded: {bank} {transaction_type} ({category})")
                
            # Unrecognized transactions are tracked in _categorize_transaction
        
        net_cash_flow = total_income - total_fees
        
        logger.debug(f"Investment cash flow calculation complete:")
        logger.debug(f"  Total Income: ${total_income:.2f}")
        logger.debug(f"  Total Fees: ${total_fees:.2f}")
        logger.debug(f"  Net Cash Flow: ${net_cash_flow:.2f}")
        logger.debug(f"  Processed: {processed_count}, Excluded: {excluded_count}, Skipped: {skipped_count}")
        
        # Log unrecognized transactions
        self._log_unrecognized_transactions()
        
        return net_cash_flow
    
    def _log_unrecognized_transactions(self):
        """Log any unrecognized transaction types for future dictionary updates."""
        if not self.unrecognized_transactions:
            return
            
        logger.info("=== UNRECOGNIZED TRANSACTIONS FOR DICTIONARY UPDATE ===")
        for bank, issues in self.unrecognized_transactions.items():
            logger.info(f"Bank: {bank}")
            for issue_type, transactions in issues.items():
                if issue_type == 'UNRECOGNIZED_BANK':
                    logger.info(f"  ⚠️  Bank '{bank}' not in transaction mappings")
                elif issue_type == 'UNRECOGNIZED_TYPES':
                    logger.info(f"  ⚠️  Unrecognized transaction types:")
                    for tx_type in set(transactions):  # Remove duplicates
                        logger.info(f"    - '{tx_type}'")
        logger.info("=== END UNRECOGNIZED TRANSACTIONS ===")
        
        # Clear for next calculation
        self.unrecognized_transactions = {}
    
    def get_supported_banks(self) -> List[str]:
        """Get list of supported bank codes."""
        return list(self.TRANSACTION_MAPPINGS.keys())
    
    def get_transaction_types_for_bank(self, bank: str) -> Dict[str, List[str]]:
        """Get all transaction types for a specific bank."""
        return self.TRANSACTION_MAPPINGS.get(bank.upper(), {})