"""
Excel parser module for securities and transactions.
Migrated from ProjectAurum with Django integration.
"""

import logging
import os
from typing import Dict, List, Any, Optional

import pandas as pd
from ..business_logic.calculation_helpers import normalize_asset_type

logger = logging.getLogger(__name__)


def validate_client_code(client_code: str) -> bool:
    """
    Validate client code format.
    
    Args:
        client_code: Client code to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not client_code or not isinstance(client_code, str):
        return False
    
    # Remove whitespace and convert to uppercase
    client_code = client_code.strip().upper()
    
    # Basic validation - should be 2-10 characters, alphanumeric
    if len(client_code) < 2 or len(client_code) > 10:
        return False
        
    # Should contain only letters and numbers
    if not client_code.isalnum():
        return False
        
    return True


class ExcelParser:
    """Base class for Excel parsers."""
    
    def __init__(self, file_path: str):
        """
        Initialize the Excel parser.
        
        Args:
            file_path: Path to the Excel file
        """
        self.file_path = file_path
        self.column_map = {}
    
    def read_excel(self, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Read the Excel file into a pandas DataFrame.
        
        Args:
            sheet_name: Sheet name to read. If None, reads the first sheet.
            
        Returns:
            DataFrame with the Excel data
        """
        logger.info(f"Reading Excel file: {self.file_path}")
        
        # Read the Excel file
        try:
            # First, get available sheet names
            xls = pd.ExcelFile(self.file_path)
            sheet_names = xls.sheet_names
            logger.info(f"Excel file has sheets: {sheet_names}")
            
            if not sheet_name and len(sheet_names) > 0:
                sheet_name = sheet_names[0]
                logger.info(f"Using first sheet: {sheet_name}")
        
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)
            logger.info(f"Excel file loaded successfully with {len(df)} rows")
            
            # Map column names to required column names if necessary
            columns = list(df.columns)
            logger.info(f"Columns found: {columns}")
            
            # Try to dynamically map column names based on similarity
            column_mappings = self.get_column_mappings()
            if column_mappings:
                self.map_columns(df, column_mappings)
            
            return df
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise
    
    def get_column_mappings(self) -> Dict[str, List[str]]:
        """
        Get a mapping of required column names to possible matches in the file.
        
        Returns:
            Dictionary mapping required columns to possible matches
        """
        return {}

    def map_columns(self, df: pd.DataFrame, column_mappings: Dict[str, List[str]]) -> None:
        """
        Map columns in the DataFrame to the expected column names.
        
        Args:
            df: DataFrame to modify
            column_mappings: Dictionary mapping required columns to possible matches
        """
        # Create a mapping of actual column names to required column names
        self.column_map = {}
        columns = list(df.columns)
        
        for required_col, possible_matches in column_mappings.items():
            for col in columns:
                if col.lower() in [match.lower() for match in possible_matches]:
                    self.column_map[required_col] = col
                    break
            
            # If we didn't find a match, log a warning
            if required_col not in self.column_map:
                logger.warning(f"Could not find a match for required column: {required_col}")
                # Continue processing, as validation will catch missing required columns
        
        logger.info(f"Column mapping created: {self.column_map}")

    def validate_columns(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate that the DataFrame contains all the required columns.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            True if all required columns are present, False otherwise
        """
        if not self.column_map:
            # Traditional validation - check if all required columns are in the DataFrame
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing required columns: {missing_columns}")
                return False
        else:
            # Validate using the column map
            missing_columns = [col for col in required_columns if col not in self.column_map]
        if missing_columns:
            logger.warning(f"Missing required columns: {missing_columns}")
            return False
        
        return True
    
    def parse(self) -> List[Dict[str, Any]]:
        """
        Parse the Excel file.
        
        Returns:
            List of dictionaries with parsed data
        """
        raise NotImplementedError("Subclasses must implement parse() method")


class StatementParser(ExcelParser):
    """Parser for securities Excel files."""
    
    REQUIRED_COLUMNS = [
        'ticker', 'name', 'asset_type', 'quantity', 'price', 'market_value', 'cost_basis', 'coupon_rate', 'bank', 'account', 'client'
    ]
    
    def __init__(self, file_path: str):
        """
        Initialize the securities parser.
        
        Args:
            file_path: Path to the securities Excel file
        """
        super().__init__(file_path)
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse date string and return in YYYY-MM-DD format.
        Returns None if parsing fails.
        """
        if pd.isna(date_str):
            return None
            
        try:
            # Try multiple date formats
            for date_format in ['%m/%d/%Y', '%m-%d-%y', '%m/%d/%y', '%Y-%m-%d']:
                try:
                    return pd.to_datetime(date_str, format=date_format).strftime('%Y-%m-%d')
                except ValueError:
                    continue
                    
            # If none of the formats worked, try pandas' default parser
            return pd.to_datetime(date_str).strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            return None

    def get_column_mappings(self) -> Dict[str, List[str]]:
        """
        Get column mappings for securities files.
        
        Returns:
            Dictionary mapping required columns to possible matches
        """
        return {
            'ticker': ['Ticker', 'Symbol', 'ticker', 'symbol'],
            'name': ['Name', 'Description', 'Security Name', 'name'],
            'asset_type': ['Asset Type', 'Type', 'Security Type', 'asset_type', 'Asset Class'],
            'quantity': ['Quantity', 'qty', 'quantity', 'Shares', 'Pos Qty'],
            'price': ['Price', 'price', 'Share Price'],
            'market_value': ['Market Value', 'Value', 'market_value', 'Current Value'],
            'cost_basis': ['Cost Basis', 'Cost', 'cost_basis', 'Total Cost'],
            'coupon_rate': ['Coupon Rate', 'Coupon', 'coupon_rate', 'Coupon %', 'Coupon Pct', 'Annual Coupon Rate'],
            'maturity_date': ['Maturity Date', 'maturity_date', 'Maturity', 'Maturity_Date', 'maturity date', 'MaturityDate'],
            'bank': ['Bank', 'Financial Institution', 'Institution', 'bank', 'Custodian', 'Broker'],
            'account': ['Account', 'LLC', 'account', 'Account Name', 'AccountName', 'Account Type'],
            'client': ['Client', 'client', 'Client Code', 'ClientCode', 'Client Id', 'ClientId']
        }
    
    def parse(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse the securities Excel file and group by client.
        
        Returns:
            Dictionary with client codes as keys and lists of security dictionaries as values
        """
        df = self.read_excel()
        
        # Validate columns
        if not self.validate_columns(df, self.REQUIRED_COLUMNS):
            raise ValueError("Securities file does not contain required columns")
        
        # Check if client column exists
        client_col = self.column_map.get('client', 'client')
        if client_col not in df.columns:
            raise ValueError(f"Excel file must include a client column. Available columns: {', '.join(df.columns)}")
        
        # Group records by client
        securities_by_client = {}
        
        for _, row in df.iterrows():
            # Extract client code and validate
            client_code = row[client_col] if pd.notna(row[client_col]) else None
            if not client_code:
                logger.warning(f"Skipping row with empty client code: {row}")
                continue
                
            client_code = str(client_code).strip().upper()
            if not validate_client_code(client_code):
                logger.warning(f"Skipping row with invalid client code: {client_code}")
                continue
                
            # Create security dictionary for this row
            security = {}
            
            # Get the actual column names from the mapping
            ticker_col = self.column_map.get('ticker', 'ticker')
            name_col = self.column_map.get('name', 'name')
            asset_type_col = self.column_map.get('asset_type', 'asset_type')
            quantity_col = self.column_map.get('quantity', 'quantity')
            price_col = self.column_map.get('price', 'price')
            market_value_col = self.column_map.get('market_value', 'market_value')
            cost_basis_col = self.column_map.get('cost_basis', 'cost_basis')
            coupon_rate_col = self.column_map.get('coupon_rate', 'coupon_rate')
            maturity_date_col = self.column_map.get('maturity_date', 'maturity_date')
            bank_col = self.column_map.get('bank', 'bank')
            account_col = self.column_map.get('account', 'account')
            
            # Required fields
            security['client'] = client_code
            security['ticker'] = row[ticker_col] if pd.notna(row[ticker_col]) else ''
            security['name'] = row[name_col] if pd.notna(row[name_col]) else ''
            # Normalize asset type using the utility function
            raw_asset_type = row[asset_type_col] if pd.notna(row[asset_type_col]) else 'unknown'
            security['asset_type'] = normalize_asset_type(raw_asset_type)
            
            # Handle bank field
            if pd.notna(row[bank_col]):
                security['bank'] = str(row[bank_col]).strip()
            else:
                security['bank'] = None
            
            # Handle account field
            if pd.notna(row[account_col]):
                security['account'] = str(row[account_col]).strip()
            else:
                security['account'] = None
            
            # Handle numeric fields - remove commas before converting to float
            # Enhanced to handle dash ('-') values gracefully by converting to 0
            def safe_numeric_conversion(value, field_name):
                """Safely convert numeric values, handling dashes and empty values."""
                if pd.notna(value):
                    value_str = str(value).replace(',', '').strip()
                    if value_str == '-' or value_str == '':
                        logger.debug(f"Converting dash/empty value to 0 for {field_name} in security {security.get('name', 'Unknown')}")
                        return 0.0
                    try:
                        return float(value_str)
                    except ValueError as e:
                        logger.warning(f"Error converting {field_name} value '{value_str}' to float for security {security.get('name', 'Unknown')}: {e}. Using 0.")
                        return 0.0
                else:
                    return 0.0
            
            try:
                # Apply safe conversion to all numeric fields
                security['quantity'] = safe_numeric_conversion(row[quantity_col], 'quantity')
                security['price'] = safe_numeric_conversion(row[price_col], 'price')
                security['market_value'] = safe_numeric_conversion(row[market_value_col], 'market_value')
                security['cost_basis'] = safe_numeric_conversion(row[cost_basis_col], 'cost_basis')
                    
                # Handle coupon rate - special handling for percentage values
                if pd.notna(row[coupon_rate_col]):
                    coupon_str = str(row[coupon_rate_col]).replace(',', '').replace('%', '').strip()
                    if coupon_str == '-' or coupon_str == '':
                        security['coupon_rate'] = None
                    else:
                        try:
                            security['coupon_rate'] = float(coupon_str)
                        except ValueError:
                            logger.warning(f"Error converting coupon_rate '{coupon_str}' for security {security.get('name', 'Unknown')}. Using None.")
                            security['coupon_rate'] = None
                else:
                    security['coupon_rate'] = None
                    
                # Handle maturity date for fixed income
                if security['asset_type'] == 'Fixed Income' and maturity_date_col and pd.notna(row[maturity_date_col]):
                    maturity_date = self._parse_date(str(row[maturity_date_col]))
                    if maturity_date:
                        security['maturity_date'] = maturity_date
                        logger.info(f"Parsed maturity date {maturity_date} for security {security.get('name')}")
                    else:
                        logger.debug(f"Could not parse maturity date for security {security.get('name')}")
                else:
                    logger.debug(f"No maturity date found for security {security.get('name')}")
                    
            except Exception as e:
                logger.warning(f"Unexpected error processing numeric fields for security {security.get('name', 'Unknown')}: {e}. Skipping this security.")
                continue
            
            # Optional fields - case insensitive
            isin_col = next((df_col for df_col in df.columns if df_col.lower() == 'isin'), None)
            if isin_col and pd.notna(row[isin_col]):
                security['isin'] = row[isin_col]
                
            cusip_col = next((df_col for df_col in df.columns if df_col.lower() == 'cusip'), None)
            if cusip_col and pd.notna(row[cusip_col]):
                security['cusip'] = row[cusip_col]
                
            yield_col = next((df_col for df_col in df.columns if df_col.lower() == 'yield_pct'), None)
            if yield_col and pd.notna(row[yield_col]):
                try:
                    yield_str = str(row[yield_col]).replace(',', '').strip()
                    if yield_str != '-' and yield_str != '':
                        security['yield_pct'] = float(yield_str)
                except ValueError:
                    pass
                
            currency_col = next((df_col for df_col in df.columns if df_col.lower() == 'currency'), None)
            if currency_col and pd.notna(row[currency_col]):
                security['currency'] = row[currency_col]
            
            # Enhanced identifier handling: CUSIP -> Ticker -> '0' fallback
            if 'isin' not in security and 'cusip' not in security:
                # Try to use ticker as fallback identifier
                if 'ticker' in security and security['ticker'] and str(security['ticker']).strip() != '' and str(security['ticker']).strip() != '-':
                    security['cusip'] = str(security['ticker']).strip()
                    logger.info(f"Using ticker '{security['cusip']}' as CUSIP for security {security.get('name')}")
                else:
                    # Use '0' as final fallback
                    security['cusip'] = '0'
                    logger.info(f"Using placeholder '0' as CUSIP for security {security.get('name')} (missing both ISIN/CUSIP and ticker)")
            
            # Add security to the appropriate client's list
            if client_code not in securities_by_client:
                securities_by_client[client_code] = []
            securities_by_client[client_code].append(security)
        
        # Log summary of parsed securities by client
        for client, securities in securities_by_client.items():
            logger.info(f"Parsed {len(securities)} securities for client {client}")
        
        return securities_by_client


class TransactionParser(ExcelParser):
    """Parser for transactions Excel files."""
    
    REQUIRED_COLUMNS = [
        'date', 'transaction_type', 'quantity', 'price', 'amount', 'bank', 'account', 'client'
    ]
    
    def __init__(self, file_path: str):
        """
        Initialize the transactions parser.
        
        Args:
            file_path: Path to the transactions Excel file
        """
        super().__init__(file_path)
    
    def get_column_mappings(self) -> Dict[str, List[str]]:
        """
        Get column mappings for transaction files.
        
        Returns:
            Dictionary mapping required columns to possible matches
        """
        return {
            'date': ['Date', 'Transaction Date', 'Trade Date', 'date'],
            'transaction_type': ['Type', 'Transaction Type', 'Trade Type', 'transaction_type'],
            'quantity': ['Quantity', 'Qty', 'quantity', 'Shares', 'Number of Shares'],
            'price': ['Price', 'price', 'Share Price', 'Trade Price'],
            'amount': ['Amount', 'Total', 'amount', 'Value', 'Total Amount'],
            'bank': ['Bank', 'Financial Institution', 'Institution', 'bank', 'Custodian', 'Broker'],
            'account': ['Account', 'LLC', 'account', 'Account Name', 'AccountName', 'Account Type'],
            'client': ['Client', 'client', 'Client Code', 'ClientCode', 'Client Id', 'ClientId']
        }
    
    def parse(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse the transactions Excel file and group by client.
        
        Returns:
            Dictionary with client codes as keys and lists of transaction dictionaries as values
        """
        df = self.read_excel()
        
        # Validate columns
        if not self.validate_columns(df, self.REQUIRED_COLUMNS):
            raise ValueError("Transactions file does not contain required columns")
        
        # Check if client column exists
        client_col = self.column_map.get('client', 'client')
        if client_col not in df.columns:
            raise ValueError(f"Excel file must include a client column. Available columns: {', '.join(df.columns)}")
        
        # Group records by client
        transactions_by_client = {}
        
        for _, row in df.iterrows():
            # Extract client code and validate
            client_code = row[client_col] if pd.notna(row[client_col]) else None
            if not client_code:
                logger.warning(f"Skipping row with empty client code: {row}")
                continue
                
            client_code = str(client_code).strip().upper()
            if not validate_client_code(client_code):
                logger.warning(f"Skipping row with invalid client code: {client_code}")
                continue
            
            transaction = {}
            transaction['client'] = client_code
            
            # Get the actual column names from the mapping
            date_col = self.column_map.get('date', 'date')
            type_col = self.column_map.get('transaction_type', 'transaction_type')
            quantity_col = self.column_map.get('quantity', 'quantity')
            price_col = self.column_map.get('price', 'price')
            amount_col = self.column_map.get('amount', 'amount')
            bank_col = self.column_map.get('bank', 'bank')
            account_col = self.column_map.get('account', 'account')
            
            # Required fields
            try:
                # Handle date with the MM/DD/YY format
                if pd.notna(row[date_col]):
                    try:
                        # Try multiple date formats
                        for date_format in ['%m/%d/%y', '%m/%d/%Y', '%d/%m/%y', '%d/%m/%Y']:
                            try:
                                transaction['date'] = pd.to_datetime(row[date_col], format=date_format).strftime('%Y-%m-%d')
                                break
                            except ValueError:
                                continue
                        
                        # If none of the formats worked, try pandas' default parser
                        if 'date' not in transaction:
                            transaction['date'] = pd.to_datetime(row[date_col]).strftime('%Y-%m-%d')
                    except Exception as e:
                        logger.warning(f"Error parsing date {row[date_col]}: {e}")
                        continue
                else:
                    continue  # Skip rows without dates
                
                transaction['transaction_type'] = row[type_col] if pd.notna(row[type_col]) else 'unknown'
                
                # Handle bank field
                if pd.notna(row[bank_col]):
                    transaction['bank'] = str(row[bank_col]).strip()
                else:
                    transaction['bank'] = None
                
                # Handle account field
                if pd.notna(row[account_col]):
                    transaction['account'] = str(row[account_col]).strip()
                else:
                    transaction['account'] = None
                
                # Handle numeric fields - remove commas before converting to float
                # Enhanced to handle dash ('-') values gracefully by converting to 0
                def safe_numeric_conversion_transaction(value, field_name):
                    """Safely convert numeric values for transactions, handling dashes and empty values."""
                    if pd.notna(value):
                        value_str = str(value).replace(',', '').strip()
                        if value_str == '-' or value_str == '':
                            logger.debug(f"Converting dash/empty value to 0 for {field_name} in transaction")
                            return 0.0
                        try:
                            return float(value_str)
                        except ValueError as e:
                            logger.warning(f"Error converting {field_name} value '{value_str}' to float in transaction: {e}. Using 0.")
                            return 0.0
                    else:
                        return 0.0
                
                try:
                    # Apply safe conversion to all numeric fields
                    transaction['quantity'] = safe_numeric_conversion_transaction(row[quantity_col], 'quantity')
                    transaction['price'] = safe_numeric_conversion_transaction(row[price_col], 'price')
                    transaction['amount'] = safe_numeric_conversion_transaction(row[amount_col], 'amount')
                    
                    # Look for fees column (not required)
                    fees_col = next((df_col for df_col in df.columns if df_col.lower() == 'fees'), None)
                    if fees_col and pd.notna(row[fees_col]):
                        transaction['fees'] = safe_numeric_conversion_transaction(row[fees_col], 'fees')
                except Exception as e:
                    logger.warning(f"Unexpected error processing numeric fields for transaction: {e}. Skipping this transaction.")
                    continue
                
                # Optional fields - case insensitive
                isin_col = next((df_col for df_col in df.columns if df_col.lower() == 'isin'), None)
                if isin_col and pd.notna(row[isin_col]):
                    transaction['isin'] = row[isin_col]
                
                cusip_col = next((df_col for df_col in df.columns if df_col.lower() == 'cusip'), None)
                if cusip_col and pd.notna(row[cusip_col]):
                    transaction['cusip'] = row[cusip_col]
                
                notes_col = next((df_col for df_col in df.columns if df_col.lower() == 'notes'), None)
                if notes_col and pd.notna(row[notes_col]):
                    transaction['notes'] = row[notes_col]
                
                # Enhanced identifier handling for transactions: CUSIP -> '0' fallback
                # (Transactions typically don't have tickers, so we skip that step)
                if 'isin' not in transaction and 'cusip' not in transaction:
                    transaction['cusip'] = '0'
                    logger.info(f"Using placeholder '0' as CUSIP for transaction (missing both ISIN/CUSIP)")
                
                # Add transaction to the appropriate client's list
                if client_code not in transactions_by_client:
                    transactions_by_client[client_code] = []
                transactions_by_client[client_code].append(transaction)
            except Exception as e:
                logger.warning(f"Error processing transaction row: {e}")
                continue
        
        # Log summary of parsed transactions by client
        for client, transactions in transactions_by_client.items():
            logger.info(f"Parsed {len(transactions)} transactions for client {client}")
        
        return transactions_by_client