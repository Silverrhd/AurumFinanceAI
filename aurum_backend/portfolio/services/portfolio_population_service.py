"""
Pure Django Portfolio Population Service
No ProjectAurum imports, no SQLite, just clean Django ORM operations.
"""

from django.db import transaction
from ..models import Client, Asset, Position, Transaction, PortfolioSnapshot
from ..parsers.excel_parser import StatementParser, TransactionParser
import logging
from datetime import datetime, date
from decimal import Decimal
import hashlib

logger = logging.getLogger(__name__)

class PortfolioPopulationService:
    """Pure Django service for populating portfolio data from Excel files."""
    
    @transaction.atomic
    def populate_from_excel(self, securities_file: str, transactions_file: str, 
                          snapshot_date: str = None) -> dict:
        """
        Populate Django database from Excel files.
        
        Args:
            securities_file: Path to securities Excel file
            transactions_file: Path to transactions Excel file
            snapshot_date: Date for snapshot (YYYY-MM-DD)
            
        Returns:
            Dict with population results
        """
        if not snapshot_date:
            snapshot_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Starting Django population for date: {snapshot_date}")
        
        # Parse Excel files using existing parser
        securities_parser = StatementParser(securities_file)
        securities_by_client = securities_parser.parse()
        
        transactions_parser = TransactionParser(transactions_file)
        transactions_by_client = transactions_parser.parse()
        
        results = {
            'clients_processed': 0,
            'assets_created': 0,
            'positions_created': 0,
            'transactions_created': 0,
            'snapshots_created': 0,
            'client_details': {}
        }
        
        # Process each client
        all_clients = set(securities_by_client.keys()) | set(transactions_by_client.keys())
        
        for client_code in all_clients:
            logger.info(f"Processing client: {client_code}")
            
            client_results = {
                'securities_processed': 0,
                'transactions_processed': 0,
                'assets_created': 0,
                'positions_created': 0,
                'transactions_created': 0
            }
            
            # Get or create client
            client, created = Client.objects.get_or_create(
                code=client_code,
                defaults={'name': f'Client {client_code}'}
            )
            
            # Create or get snapshot
            snapshot, created = PortfolioSnapshot.objects.get_or_create(
                client=client,
                snapshot_date=snapshot_date,
                defaults={'portfolio_metrics': {}}
            )
            if created:
                results['snapshots_created'] += 1
            
            # Clear existing positions for this snapshot (fresh data)
            Position.objects.filter(snapshot=snapshot).delete()
            
            # Process securities (create assets and positions)
            securities = securities_by_client.get(client_code, [])
            for security in securities:
                client_results['securities_processed'] += 1
                
                # Create or update asset with custody-based uniqueness
                ticker = security.get('ticker', '')
                bank = security.get('bank', '')
                account = security.get('account', '')
                isin = security.get('isin', '')
                cusip = security.get('cusip', '')
                
                # NEW: Validate asset name to prevent mystery assets
                asset_name = str(security.get('name', '')).strip()
                if not asset_name or asset_name.lower() in ['', 'none', 'nan', 'null']:
                    logger.warning(f"Skipping asset with empty/invalid name for client {client_code}: {security}")
                    continue  # Skip this security entirely
                
                # Don't skip assets with empty tickers - just use empty string
                if not ticker:
                    ticker = ''
                
                # Use custody-based uniqueness: ticker + bank + account + client
                asset_defaults = {
                    'name': security.get('name', 'Unknown Asset'),
                    'asset_type': security.get('asset_type', 'Unknown'),
                    'currency': security.get('currency', 'USD'),
                    'isin': isin if isin else None,
                    'cusip': cusip if cusip else None,
                    'coupon_rate': self._safe_decimal(security.get('coupon_rate')),
                    'maturity_date': self._safe_date(security.get('maturity_date'))
                }
                
                asset, created = Asset.objects.get_or_create(
                    ticker=ticker,
                    cusip=security.get('cusip', '') if security.get('cusip') else '',
                    name=security.get('name', '') if security.get('name') else '',
                    bank=bank,
                    account=account,
                    client=client_code,
                    defaults=asset_defaults
                )
                
                if created:
                    results['assets_created'] += 1
                    client_results['assets_created'] += 1
                else:
                    # Update existing asset with new data
                    for key, value in asset_defaults.items():
                        if value is not None:  # Only update non-null values
                            setattr(asset, key, value)
                    asset.save()
                
                # Create or update position
                position, position_created = Position.objects.get_or_create(
                    snapshot=snapshot,
                    asset=asset,
                    defaults={
                        'quantity': self._safe_decimal(security.get('quantity', 0)),
                        'market_value': self._safe_decimal(security.get('market_value', 0)),
                        'cost_basis': self._safe_decimal(security.get('cost_basis', 0)),
                        'price': self._safe_decimal(security.get('price', 0)),
                        'bank': security.get('bank', ''),
                        'account': security.get('account', ''),
                        'coupon_rate': security.get('coupon_rate'),
                        'maturity_date': self._safe_date(security.get('maturity_date'))
                    }
                )
                
                if position_created:
                    results['positions_created'] += 1
                    client_results['positions_created'] += 1
                else:
                    # Update existing position with new data
                    position.quantity = self._safe_decimal(security.get('quantity', 0))
                    position.market_value = self._safe_decimal(security.get('market_value', 0))
                    position.cost_basis = self._safe_decimal(security.get('cost_basis', 0))
                    position.price = self._safe_decimal(security.get('price', 0))
                    position.bank = security.get('bank', '')
                    position.account = security.get('account', '')
                    position.coupon_rate = security.get('coupon_rate')
                    position.maturity_date = self._safe_date(security.get('maturity_date'))
                    position.save()
            
            # Process transactions with proper asset linking
            transactions = transactions_by_client.get(client_code, [])
            for row_idx, tx in enumerate(transactions):
                client_results['transactions_processed'] += 1
                
                # Find asset for transaction using custody-based matching
                ticker = tx.get('ticker', '')
                bank = tx.get('bank', '')
                account = tx.get('account', '')
                cusip = tx.get('cusip', '')
                
                # Try to find existing asset with custody-based matching
                asset = None
                
                # First try: if ticker exists, exact match with ticker + bank + account + client
                if ticker and bank and account:
                    try:
                        asset = Asset.objects.get(
                            ticker=ticker,
                            bank=bank,
                            account=account,
                            client=client_code
                        )
                    except Asset.DoesNotExist:
                        pass
                
                # Second try: match by CUSIP if available (main path for CUSIP-only transactions)
                if not asset and cusip:
                    try:
                        asset = Asset.objects.filter(
                            cusip=cusip,
                            client=client_code
                        ).first()
                    except Asset.DoesNotExist:
                        pass
                
                # Third try: match by ticker only within client (fallback)
                if not asset and ticker:
                    try:
                        asset = Asset.objects.filter(
                            ticker=ticker,
                            client=client_code
                        ).first()
                    except Asset.DoesNotExist:
                        pass
                
                # If no asset found, check if it's a cash transaction
                if not asset:
                    # Extract transaction type from transaction data
                    transaction_type = tx.get('transaction_type', '')
                    amount = tx.get('amount', 0)
                    
                    # Define cash transaction types
                    cash_transaction_types = [
                        'Withdrawal', 'Deposit', 'Transfer', 'Wire Transfer', 'ACH',
                        'OUTGOING', 'Cash withdrawal', 'ATM Withdrawal', 'Wire Transfer Credit',
                        'ELECTRONIFIED CHECK', 'Cash Liquidation', 'BILL PMT', 'Zelle Payment',
                        'Online Transfer', 'TRANSFER ACCOUNT', 'Cross Border Credit Transfer'
                    ]
                    
                    if transaction_type in cash_transaction_types:
                        # Use CASH asset for cash transactions
                        asset = self._get_or_create_cash_asset(client_code, bank, account)
                        logger.info(f"Using CASH asset for transaction: {transaction_type}, Client={client_code}, Amount={amount}")
                    else:
                        # Still skip non-cash transactions without assets
                        logger.warning(f"No matching asset found for transaction, skipping: CUSIP={cusip}, Client={client_code}, Bank={bank}, Account={account}")
                        continue
                
                # Create transaction (avoid duplicates by transaction_id)
                # Use deterministic ID generation to prevent duplicates from multiple processing runs
                transaction_id = tx.get('id', self._generate_deterministic_transaction_id(client_code, tx, row_idx))
                
                transaction_obj, created = Transaction.objects.get_or_create(
                    client=client,
                    transaction_id=transaction_id,
                    defaults={
                        'asset': asset,
                        'date': tx.get('date'),
                        'transaction_type': tx.get('transaction_type', ''),
                        'quantity': self._safe_decimal(tx.get('quantity')),
                        'price': self._safe_decimal(tx.get('price')),
                        'amount': self._safe_decimal(tx.get('amount', 0)),
                        'bank': tx.get('bank', ''),
                        'account': tx.get('account', '')
                    }
                )
                
                if created:
                    results['transactions_created'] += 1
                    client_results['transactions_created'] += 1
            
            results['clients_processed'] += 1
            results['client_details'][client_code] = client_results
            
            logger.info(f"Completed client {client_code}: "
                       f"{client_results['securities_processed']} securities, "
                       f"{client_results['transactions_processed']} transactions")
        
        logger.info(f"Population complete: {results}")
        return results
    
    def _safe_decimal(self, value) -> Decimal:
        """Safely convert value to Decimal, handling None and invalid values."""
        if value is None or value == '':
            return Decimal('0')
        
        try:
            # Handle string values that might have commas
            if isinstance(value, str):
                value = value.replace(',', '').strip()
                if value == '' or value == '-':
                    return Decimal('0')
            
            return Decimal(str(value))
        except (ValueError, TypeError):
            logger.warning(f"Could not convert value to Decimal: {value}, using 0")
            return Decimal('0')
    
    def _generate_deterministic_transaction_id(self, client_code: str, tx_data: dict, row_index: int = 0) -> str:
        """
        Generate a deterministic transaction ID based on business transaction data.
        This ensures the same transaction always gets the same ID, preventing duplicates.
        
        Args:
            client_code: Client code
            tx_data: Transaction data dictionary
            
        Returns:
            Deterministic transaction ID like 'tx_a1b2c3d4e5f6'
        """
        # Extract and normalize key business fields
        bank = str(tx_data.get('bank', '')).strip().lower()
        account = str(tx_data.get('account', '')).strip().lower()
        
        # Normalize date to YYYY-MM-DD format
        tx_date = tx_data.get('date')
        if isinstance(tx_date, date):
            date_str = tx_date.strftime('%Y-%m-%d')
        elif isinstance(tx_date, str):
            date_str = tx_date.strip()
        else:
            date_str = str(tx_date) if tx_date else ''
        
        transaction_type = str(tx_data.get('transaction_type', '')).strip().lower()
        cusip = str(tx_data.get('cusip', '')).strip().upper()
        
        # Normalize decimal amounts to 2 decimal places for consistency
        amount = self._safe_decimal(tx_data.get('amount', 0))
        quantity = self._safe_decimal(tx_data.get('quantity', 0))
        price = self._safe_decimal(tx_data.get('price', 0))
        
        # Format decimals consistently (2 decimal places for amount, 4 for quantity/price)
        amount_str = f"{amount:.2f}"
        quantity_str = f"{quantity:.4f}"  # More precision for quantities
        price_str = f"{price:.4f}"       # More precision for prices
        
        # Create deterministic business key with row index to prevent identical transaction collisions
        business_key = f"{client_code.lower()}_{bank}_{account}_{date_str}_{transaction_type}_{cusip}_{amount_str}_{quantity_str}_{price_str}_row_{row_index}"
        
        # Generate MD5 hash and take first 16 characters for shorter IDs
        hash_value = hashlib.md5(business_key.encode('utf-8')).hexdigest()[:16]
        
        # Create final transaction ID
        transaction_id = f"tx_{hash_value}"
        
        # Log the business key for debugging (can be removed later)
        logger.debug(f"Generated transaction ID: {transaction_id} from business key: {business_key}")
        
        return transaction_id
    
    def _safe_date(self, value) -> date:
        """Safely convert value to date object, handling None and various date formats."""
        if value is None or value == '':
            return None
        
        # If already a date object, return as-is
        if isinstance(value, date):
            return value
            
        # If datetime object, extract date
        if isinstance(value, datetime):
            return value.date()
        
        # Handle string dates
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value == '-' or value.lower() == 'none':
                return None
            
            # Common date formats from Excel files
            date_formats = [
                '%m/%d/%Y',     # MM/DD/YYYY (US format)
                '%m/%d/%y',     # MM/DD/YY
                '%Y-%m-%d',     # YYYY-MM-DD (ISO format)
                '%d/%m/%Y',     # DD/MM/YYYY (European format)
                '%d.%m.%Y',     # DD.MM.YYYY (German format)
                '%Y/%m/%d',     # YYYY/MM/DD
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(value, fmt).date()
                    
                    # Convert 2-digit years to 4-digit (assume 20xx for years < 50, 19xx for >= 50)
                    if parsed_date.year < 1950:
                        parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
                    
                    # Validate reasonable date range for bonds
                    if 1900 <= parsed_date.year <= 2100:
                        logger.debug(f"Parsed date {parsed_date} from string: {value}")
                        return parsed_date
                        
                except ValueError:
                    continue
        
        logger.warning(f"Could not parse date from value: {value}")
        return None
    
    def _get_or_create_cash_asset(self, client_code: str, bank: str, account: str) -> Asset:
        """
        Get or create a CASH asset for cash transactions.
        
        Args:
            client_code: Client code
            bank: Bank name
            account: Account identifier
            
        Returns:
            Asset: CASH asset for this client/bank/account combination
        """
        # Create a unique but short CUSIP for the cash asset
        # Format: CASH_ + first 4 chars of client + bank + account (truncated to 9 chars max)
        cash_identifier = f"{client_code}_{bank}_{account}".replace(' ', '')[:20]
        cash_cusip = f"CASH_{cash_identifier}"[:9]
        
        # Ensure uniqueness by adding a hash if needed
        if len(cash_cusip) >= 9:
            hash_suffix = hashlib.md5(cash_identifier.encode()).hexdigest()[:2]
            cash_cusip = f"CASH_{hash_suffix}"
        
        cash_asset, created = Asset.objects.get_or_create(
            ticker="CASH",
            client=client_code,
            bank=bank,
            account=account,
            cusip=cash_cusip,
            defaults={
                'name': f'Cash - {bank} {account}',
                'asset_type': 'CASH',
                'currency': 'USD',
                'active': True,
                'isin': None,
                'coupon_rate': None,
                'maturity_date': None,
                'dividend_yield': None
            }
        )
        
        if created:
            logger.info(f"Created CASH asset: {cash_cusip} for client {client_code}, bank {bank}, account {account}")
        
        return cash_asset