"""JPMorgan Excel file transformer for preprocessing raw bank files."""

import pandas as pd
import logging
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class JPMorganTransformer:
    """Transformer for JPMorgan Excel files following step-by-step approach."""
    
    def __init__(self):
        self.bank_code = 'JPM'
        logger.info(f"Initialized {self.bank_code} transformer")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for JPMorgan securities files (JPM → Standard)."""
        return {
            # Standard Column → JPMorgan Column (from actual file)
            'account_number': 'Account Number',
            'account_name': 'Account Name',  # NEW: For handling duplicate account numbers
            'asset_type': 'Asset Class',
            'asset_strategy': 'Asset Strategy',  # For detailed classification
            'asset_strategy_detail': 'Asset Strategy Detail',  # For detailed classification
            'name': 'Description', 
            'ticker': 'Ticker',
            'cusip': 'CUSIP',
            'security_id': 'Security ID',  # NEW: Fallback for CUSIP when empty
            'quantity': 'Quantity',  # Will convert American→European format
            'price': 'Price',        # Keep current bond logic
            'market_value': 'Value', # Will convert American→European format
            'cost_basis': 'Cost',    # Will convert American→European format
            'maturity_date': 'Maturity Date',  # Convert YYYY-MM-DD → MM/DD/YYYY
            'coupon_rate': 'Coupon Rate (%)'
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for JPMorgan transactions files (JPM → Standard)."""
        return {
            # Standard Column → JPMorgan Column (from actual file)
            'account_number': 'Account Number',
            'account_name': 'Account Name',  # NEW: For handling duplicate account numbers
            'date': 'Settlement Date',
            'transaction_type': 'Type',
            'cusip': 'Cusip',
            'price': 'Price USD',
            'quantity': 'Quantity',    # Will convert American→European format
            'amount': 'Amount USD'     # Will convert American→European format
        }
    
    def load_account_mappings(self, mappings_file: str, sheet_name: str = 'JPM') -> Dict[str, Dict[str, str]]:
        """
        Load account mappings from Excel file with support for duplicate account numbers and transaction names.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            sheet_name: Sheet name to read from (default: 'JPM')
            
        Returns:
            Dict mapping account numbers (and account names for duplicates) to client/account info
        """
        logger.info(f"Loading account mappings from {mappings_file}, sheet: {sheet_name}")
        
        try:
            df = pd.read_excel(mappings_file, sheet_name=sheet_name)
            
            # Validate required columns (transaction_account_name is optional for backward compatibility)
            required_cols = ['account number', 'client', 'account', 'account name']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Mappings file missing required columns: {missing_cols}")
            
            # Check if enhanced transaction_account_name column exists
            has_transaction_names = 'transaction_account_name' in df.columns
            if has_transaction_names:
                logger.info("Found transaction_account_name column - using enhanced mapping logic")
            else:
                logger.info("No transaction_account_name column - using legacy mapping logic")
            
            # Create mapping dictionary
            mappings = {}
            duplicate_accounts = {}
            transaction_name_mappings = 0
            
            for _, row in df.iterrows():
                account_num = str(row['account number']).strip()
                client = row['client']
                account = row['account']
                account_name = row['account name']
                transaction_account_name = row.get('transaction_account_name', None) if has_transaction_names else None
                
                # Skip rows with missing client data
                if pd.isna(client) or pd.isna(account):
                    logger.warning(f"Skipping mapping for account {account_num} - missing client or account data")
                    continue
                
                mapping_data = {
                    'client': str(client).strip(),
                    'account': str(account).strip()
                }
                
                # Check if account name is provided (for duplicates)
                if pd.notna(account_name) and str(account_name).strip():
                    # This is a duplicate account, use combination key
                    combo_key = f"{account_num}|{str(account_name).strip()}"
                    mappings[combo_key] = mapping_data
                    
                    # ENHANCED: Also add transaction_account_name mapping if available
                    if has_transaction_names and pd.notna(transaction_account_name) and str(transaction_account_name).strip():
                        tx_combo_key = f"{account_num}|{str(transaction_account_name).strip()}"
                        mappings[tx_combo_key] = mapping_data
                        transaction_name_mappings += 1
                        logger.info(f"Added transaction name mapping: {account_num} + '{transaction_account_name}' → {client}, {account}")
                    
                    # Track duplicate accounts for later matching
                    if account_num not in duplicate_accounts:
                        duplicate_accounts[account_num] = []
                    duplicate_accounts[account_num].append(str(account_name).strip())
                    
                    logger.info(f"Added duplicate account mapping: {account_num} + '{account_name}' → {client}, {account}")
                else:
                    # Regular account (no duplicates)
                    mappings[account_num] = mapping_data
            
            logger.info(f"Loaded {len(mappings)} total account mappings")
            logger.info(f"Found {len(duplicate_accounts)} accounts with duplicates: {list(duplicate_accounts.keys())}")
            if has_transaction_names:
                logger.info(f"Created {transaction_name_mappings} enhanced transaction name mappings")
            return mappings
            
        except Exception as e:
            logger.error(f"Error loading account mappings: {e}")
            raise
    
    def clean_numeric_value(self, value) -> Optional[float]:
        """Clean and convert numeric values."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # Convert to string first
            str_value = str(value).strip()
            
            # Remove common formatting characters
            str_value = str_value.replace('$', '')  # Remove dollar signs
            str_value = str_value.replace('%', '')  # Remove percentage signs
            str_value = str_value.replace('(', '-').replace(')', '')  # Handle negative parentheses
            str_value = str_value.replace(',', '')  # Remove thousands separators
            
            # Convert to float
            numeric_value = float(str_value)
            
            return numeric_value
            
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to numeric value")
            return None
    
    def convert_american_to_european_number(self, value) -> Optional[str]:
        """
        Convert American number format to European format.
        American: 7,802.30 → European: 7.802,30
        """
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # Convert to string first
            str_value = str(value).strip()
            
            # Remove common formatting characters except commas and periods
            str_value = str_value.replace('$', '')  # Remove dollar signs
            str_value = str_value.replace('%', '')  # Remove percentage signs
            str_value = str_value.replace('(', '-').replace(')', '')  # Handle negative parentheses
            
            # Handle the conversion: American (7,802.30) → European (7.802,30)
            if ',' in str_value and '.' in str_value:
                # Both comma and period present - typical American format
                # Split by period to get decimal part
                parts = str_value.split('.')
                if len(parts) == 2:
                    integer_part = parts[0].replace(',', '.')  # Replace commas with periods
                    decimal_part = parts[1]
                    european_format = f"{integer_part},{decimal_part}"
                    return european_format
            elif ',' in str_value and '.' not in str_value:
                # Only comma present - could be thousands separator
                # Convert comma to period for thousands
                european_format = str_value.replace(',', '.')
                return european_format
            elif '.' in str_value and ',' not in str_value:
                # Only period present - convert to comma for decimal
                european_format = str_value.replace('.', ',')
                return european_format
            else:
                # No special formatting needed
                return str_value
                
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to European number format")
            return None
    
    def get_asset_classification_rules(self) -> Dict:
        """Get asset classification and price formatting rules."""
        return {
            ('Cash', 'Unclassified'): ('Cash', 'decimal_comma'),
            ('Cash & Short Term', 'Cash'): ('Money Market', 'decimal_comma'),
            ('Cash & Short Term', 'Non-USD Cash'): ('Other Currency', 'decimal_comma'),
            ('US Fixed Income', 'Taxable Core'): ('Fixed Income', 'bond_format'),
            ('Cash & Short Term', 'Short Term'): ('Fixed Income', 'bond_format'),
            ('US Fixed Income', 'Extended Credit/High Yield'): ('Fixed Income', 'bond_format'),
            ('Global Fixed Income', 'Investment Grade'): ('Fixed Income', 'decimal_comma'),
            ('Non-US Fixed Income', 'Emerging Market'): ('Fixed Income', 'bond_format'),
            ('Non-US Fixed Income', 'Investment Grade'): ('Fixed Income', 'bond_format'),
            ('Global Fixed Income', 'Extended Credit/High Yield'): ('Fixed Income', 'bond_format'),
            ('US Fixed Income', 'Inflation'): ('Fixed Income', 'decimal_comma'),
            ('US Fixed Income', 'Unclassified'): ('Fixed Income', 'decimal_comma'),
            ('Non-US Fixed Income', 'Extended Credit/High Yield'): ('Fixed Income', 'bond_format'),
            # Default for any other Fixed Income & Cash combination
            'default': ('Fixed Income', 'decimal_comma')
        }

    def is_bond_by_characteristics(self, description, maturity_date) -> bool:
        """
        Determine if an asset is a bond based on description and maturity date.
        Bonds have at least one of: % symbol, date in description, or maturity date.
        
        Args:
            description: Asset description/name
            maturity_date: Maturity date value
            
        Returns:
            True if asset appears to be a bond, False otherwise
        """
        if pd.isna(description):
            description = ""
        
        desc_str = str(description)
        
        # Check for % symbol (coupon rate)
        if '%' in desc_str:
            return True
        
        # Check for maturity date
        if pd.notna(maturity_date):
            return True
        
        # Check for date patterns in description (maturity dates)
        date_patterns = [
            r'\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b',  # MM/DD/YYYY or MM-DD-YYYY
            r'\b\d{4}[/\-]\d{1,2}[/\-]\d{1,2}\b',    # YYYY/MM/DD or YYYY-MM-DD
            r'\b\d{1,2}/\d{1,2}/\d{2,4}\b'           # M/D/YY or MM/DD/YYYY
        ]
        
        for pattern in date_patterns:
            if re.search(pattern, desc_str):
                return True
        
        return False

    def convert_bond_price(self, price, asset_type, asset_strategy=None, asset_strategy_detail=None, ticker=None, name=None, maturity_date=None) -> Optional[str]:
        """
        Convert bond prices to correct decimal format with European notation.
        
        Simplified Logic:
        - Fixed Income & Cash + Has maturity date → bond_format
        - Equity + Has maturity date → bond_format
        - Everything else → european_number_format
        
        Args:
            price: Original price value
            asset_type: Asset type (Asset Class)
            asset_strategy: Asset Strategy for detailed classification
            asset_strategy_detail: Asset Strategy Detail for detailed classification
            ticker: Ticker symbol (for special pricing logic)
            name: Asset name/description (for bond detection)
            maturity_date: Maturity date (for bond detection)
            
        Returns:
            Converted price as string with European decimal format (comma)
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        # SIMPLIFIED LOGIC: 
        # - Fixed Income & Cash + Has maturity date → bond_format
        # - Equity + Has maturity date → bond_format  
        # - Everything else → european_number_format
        if ((asset_type == 'Fixed Income & Cash' and pd.notna(maturity_date)) or 
            (asset_type == 'Equity' and pd.notna(maturity_date))):
            price_format = 'bond_format'
        else:
            price_format = 'european_number_format'
        
        try:
            # Handle string prices (like '1.00', '99.97')
            if isinstance(price, str):
                # Clean the string price
                cleaned_str = price.strip().replace('$', '').replace(',', '')
                numeric_price = float(cleaned_str)
            else:
                # Handle numeric prices
                numeric_price = float(price)
            
            # Apply price formatting based on the determined rule
            if price_format == 'bond_format':
                # Simplified JPMorgan bond price conversion
                # Remove all commas and periods, then apply placement rules
                
                # Convert price to string and remove all commas and periods
                clean_price = str(price).replace(',', '').replace('.', '')
                
                # Apply the simple rule based on first digit
                if clean_price.startswith('1'):
                    # Numbers starting with 1: place comma after the 1
                    # Example: 109389 → 1,09389
                    converted_price = float(clean_price[0] + '.' + clean_price[1:])
                else:
                    # Numbers starting with anything other than 1: place comma before the number
                    # Example: 999432 → 0,999432
                    converted_price = float('0.' + clean_price)
                
                # Format with 6 decimal places and European notation
                return f"{converted_price:.6f}".replace('.', ',')
            else:
                # For non-bonds: convert American number format to European format
                return self.convert_american_to_european_number(price)
                
        except (ValueError, TypeError):
            logger.warning(f"Could not convert price '{price}' for {asset_strategy} | {asset_strategy_detail}")
            return None
    
    def reclassify_asset_type(self, asset_type, asset_strategy, asset_strategy_detail) -> str:
        """
        Reclassify asset type from generic 'Fixed Income & Cash' to specific type.
        
        Args:
            asset_type: Original asset type (Asset Class)
            asset_strategy: Asset Strategy
            asset_strategy_detail: Asset Strategy Detail
            
        Returns:
            Reclassified asset type
        """
        # For non-Fixed Income & Cash assets, return as-is
        if not (asset_type and 'Fixed Income' in str(asset_type)):
            return asset_type
        
        # Get classification rules
        rules = self.get_asset_classification_rules()
        
        # Determine new asset type based on strategy and detail
        if asset_strategy and asset_strategy_detail:
            key = (asset_strategy, asset_strategy_detail)
            if key in rules:
                new_asset_type, _ = rules[key]
                return new_asset_type
            else:
                new_asset_type, _ = rules['default']
                return new_asset_type
        
        # Fallback to default
        new_asset_type, _ = rules['default']
        return new_asset_type
    
    def clean_text_value(self, value) -> Optional[str]:
        """Clean text values."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        # Convert to string and strip whitespace
        cleaned = str(value).strip()
        
        # Return None for empty strings
        return cleaned if cleaned else None
    
    def clean_date_value(self, value) -> Optional[str]:
        """Clean and format date values from YYYY-MM-DD to MM/DD/YYYY format."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # Convert to datetime and format to MM/DD/YYYY
            date_obj = pd.to_datetime(value, errors='coerce')
            if pd.isna(date_obj):
                return None
            return date_obj.strftime('%m/%d/%Y')
            
        except Exception:
            logger.warning(f"Could not convert '{value}' to date")
            return None
    
    def clean_date_value_transactions_only(self, value) -> Optional[str]:
        """
        Clean and format date values for TRANSACTIONS ONLY.
        Detects and corrects day/month swap when day is 12 or below.
        """
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # If already a datetime object
            if isinstance(value, datetime):
                # Check if this might be a swapped date (day <= 12)
                if value.day <= 12:
                    # This could be a swapped date. Let's swap it back.
                    # Original was DD-MM-YY, pandas read as MM-DD-YY
                    # So we need to swap day and month back
                    corrected_date = datetime(value.year, value.day, value.month)
                    return corrected_date.strftime('%m/%d/%Y')
                else:
                    # Day > 12, so pandas probably read it correctly
                    return value.strftime('%m/%d/%Y')
            
            # If string, try to parse
            value_str = str(value)
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%y'):
                try:
                    dt = datetime.strptime(value_str, fmt)
                    # Apply the same logic for parsed dates
                    if dt.day <= 12:
                        corrected_date = datetime(dt.year, dt.day, dt.month)
                        return corrected_date.strftime('%m/%d/%Y')
                    else:
                        return dt.strftime('%m/%d/%Y')
                except ValueError:
                    continue
            # If all parsing fails, return as is
            return value_str
        except Exception:
            logger.warning(f"Could not convert '{value}' to date")
            return None
    
    def map_account_to_client(self, account_number: str, account_name: str, mappings: Dict[str, Dict[str, str]]) -> Tuple[Optional[str], Optional[str]]:
        """
        Map JPMorgan account number and name to client and account using enhanced mappings with transaction names.
        
        ENHANCED PRIORITY ORDER:
        1. EXACT MATCH: account_number|transaction_account_name (NEW - HIGHEST PRIORITY)
        2. EXACT MATCH: account_number|account_name (EXISTING)
        3. PARTIAL MATCH: account_number + transaction_account_name (NEW)
        4. PARTIAL MATCH: account_number + account_name (EXISTING)
        5. EXACT MATCH: account_number only (for non-duplicates)
        6. PARTIAL MATCH: account_number only (for non-duplicates)
        
        Args:
            account_number: JPMorgan account number
            account_name: JPMorgan account name (for handling duplicates)
            mappings: Account mappings dictionary (now includes transaction names)
            
        Returns:
            Tuple of (client, account) or (None, None) if not found
        """
        if not account_number or pd.isna(account_number):
            return None, None
        
        account_str = str(account_number).strip()
        account_name_str = str(account_name).strip() if pd.notna(account_name) else ""
        
        # PRIORITY 1: Try exact match with transaction account name (NEW - HIGHEST PRIORITY)
        if account_name_str:
            tx_combo_key = f"{account_str}|{account_name_str}"
            if tx_combo_key in mappings:
                mapping = mappings[tx_combo_key]
                logger.info(f"🎯 TRANSACTION NAME MATCH: {account_str} + '{account_name_str}' → {mapping['client']}, {mapping['account']}")
                return mapping['client'], mapping['account']
        
        # PRIORITY 2: Try exact match with mapping account name (EXISTING)
        # Look for any combination key that might match the account_name_str
        for mapped_key, mapping in mappings.items():
            if '|' in mapped_key:  # This is a duplicate account mapping
                mapped_account, mapped_name = mapped_key.split('|', 1)
                if (mapped_account.replace('…', '') == account_str.replace('...', '') or
                    mapped_account == account_str) and mapped_name != account_name_str:
                    # This suggests account_name_str is actually a transaction name
                    # and mapped_name is the full mapping name - skip for now
                    continue
        
        # PRIORITY 3: Try partial match with transaction account name (NEW)
        if account_name_str:
            for mapped_key, mapping in mappings.items():
                if '|' in mapped_key:  # This is a duplicate account mapping
                    mapped_account, mapped_name = mapped_key.split('|', 1)
                    if ((mapped_account.replace('…', '') in account_str or 
                         account_str.replace('...', '') in mapped_account.replace('…', '')) and 
                        mapped_name == account_name_str):
                        logger.info(f"🎯 PARTIAL TRANSACTION MATCH: {account_str} + '{account_name_str}' → {mapped_key}")
                        return mapping['client'], mapping['account']
        
        # PRIORITY 4: Try partial match with mapping account name (EXISTING LOGIC)
        if account_name_str:
            for mapped_key, mapping in mappings.items():
                if '|' in mapped_key:  # This is a duplicate account mapping
                    mapped_account, mapped_name = mapped_key.split('|', 1)
                    # Check if account_name_str might be a shortened version of mapped_name
                    if ((mapped_account.replace('…', '') in account_str or 
                         account_str.replace('...', '') in mapped_account.replace('…', '')) and 
                        (account_name_str in mapped_name or mapped_name.startswith(account_name_str))):
                        logger.info(f"📋 PARTIAL MAPPING NAME MATCH: {account_str} + '{account_name_str}' → {mapped_key}")
                        return mapping['client'], mapping['account']
        
        # PRIORITY 5: Try exact match for regular accounts (non-duplicates)
        if account_str in mappings:
            mapping = mappings[account_str]
            logger.info(f"✅ REGULAR ACCOUNT MATCH: {account_str} → {mapping['client']}, {mapping['account']}")
            return mapping['client'], mapping['account']
        
        # PRIORITY 6: Try partial match for regular accounts (account number might be truncated with ...)
        for mapped_account, mapping in mappings.items():
            if '|' not in mapped_account:  # Skip duplicate account mappings
                if (mapped_account.replace('…', '') in account_str or 
                    account_str.replace('...', '') in mapped_account.replace('…', '')):
                    logger.info(f"✅ PARTIAL REGULAR MATCH: {account_str} → {mapped_account}")
                    return mapping['client'], mapping['account']
        
        logger.warning(f"❌ NO MAPPING FOUND: account number: {account_str} + account name: '{account_name_str}'")
        return None, None
    
    def extract_cusip_with_fallback(self, cusip_value, security_id_value, asset_name: str = "") -> Optional[str]:
        """
        Extract CUSIP value with Security ID fallback.
        
        Logic:
        1. Look for CUSIP in CUSIP column, if found use that
        2. Look for CUSIP in CUSIP column, if not found, extract from Security ID column
        
        Args:
            cusip_value: Value from CUSIP column
            security_id_value: Value from Security ID column
            asset_name: Asset name for logging purposes
            
        Returns:
            CUSIP value or Security ID as fallback, or None if both empty
        """
        # First try CUSIP column
        if pd.notna(cusip_value) and str(cusip_value).strip():
            return str(cusip_value).strip()
        
        # Fallback to Security ID if CUSIP is empty
        if pd.notna(security_id_value) and str(security_id_value).strip():
            security_id_str = str(security_id_value).strip()
            logger.info(f"Using Security ID as CUSIP fallback for '{asset_name}': {security_id_str}")
            return security_id_str
        
        # Both are empty
        logger.warning(f"No CUSIP or Security ID available for '{asset_name}'")
        return None
    
    def transform_securities(self, securities_file: str, mappings_file: str) -> pd.DataFrame:
        """
        Transform JPMorgan securities to standard format.
        
        Args:
            securities_file: Path to JPM securities Excel file
            mappings_file: Path to JPM mappings Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file
        logger.info(f"Loading securities file: {securities_file}")
        df = pd.read_excel(securities_file)
        logger.info(f"Loaded {len(df)} securities records with {len(df.columns)} columns")
        
        # Get column mappings
        column_map = self.get_securities_column_mappings()
        
        # Validate required JPM columns exist
        jpm_required = list(column_map.values())
        missing_cols = [col for col in jpm_required if col not in df.columns]
        if missing_cols:
            logger.error(f"JPMorgan securities file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"JPMorgan securities file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_jpm_columns = list(column_map.values())
        logger.info(f"Keeping {len(required_jpm_columns)} required columns: {required_jpm_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[required_jpm_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename JPMorgan columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {jpm_col: std_col for std_col, jpm_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        # Step 1d: Apply CUSIP fallback logic BEFORE removing temporary columns
        logger.info("Applying CUSIP fallback logic...")
        if 'cusip' in df_renamed.columns and 'security_id' in df_renamed.columns:
            cusip_fallback_count = 0
            for idx, row in df_renamed.iterrows():
                asset_name = row.get('name', 'Unknown')
                original_cusip = row['cusip']
                fallback_cusip = self.extract_cusip_with_fallback(
                    row['cusip'], 
                    row['security_id'], 
                    asset_name
                )
                
                # Update the CUSIP value
                df_renamed.at[idx, 'cusip'] = fallback_cusip if fallback_cusip else ''
                
                # Count fallbacks used
                if pd.isna(original_cusip) and fallback_cusip:
                    cusip_fallback_count += 1
            
            logger.info(f"Applied Security ID fallback for {cusip_fallback_count} assets")
        
        logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
        
        # Load account mappings
        mappings = self.load_account_mappings(mappings_file, 'JPM')
        
        # Step 2a: Add bank column
        df_renamed['bank'] = self.bank_code
        logger.info(f"Added bank column with value: {self.bank_code}")
        
        # Step 2b: Map account numbers to client and account
        logger.info("Mapping account numbers to client and account...")
        client_list = []
        account_list = []
        unmapped_count = 0
        
        for _, row in df_renamed.iterrows():
            client, account = self.map_account_to_client(
                row['account_number'], 
                row.get('account_name', ''), 
                mappings
            )
            client_list.append(client)
            account_list.append(account)
            
            if client is None or account is None:
                unmapped_count += 1
        
        df_renamed['client'] = client_list
        df_renamed['account'] = account_list
        
        logger.info(f"Mapped {len(df_renamed) - unmapped_count} accounts successfully")
        if unmapped_count > 0:
            logger.warning(f"{unmapped_count} accounts could not be mapped")
        
        # Remove rows with unmapped accounts
        df_mapped = df_renamed.dropna(subset=['client', 'account'])
        logger.info(f"After removing unmapped accounts: {len(df_mapped)} rows")
        
        # Step 2c: Remove the temporary account_number and account_name columns
        # Also remove security_id column as it's no longer needed after fallback processing
        temp_cols_to_remove = ['account_number', 'account_name', 'security_id']
        for col in temp_cols_to_remove:
            if col in df_mapped.columns:
                df_mapped = df_mapped.drop(columns=[col])
                logger.info(f"Removed temporary column: {col}")
        
        logger.info("=== STEP 3: FORMAT ADJUSTMENTS ===")
        
        # Step 3a: Convert American to European number format for specific columns
        european_columns = ['quantity', 'market_value', 'cost_basis']
        for col in european_columns:
            if col in df_mapped.columns:
                logger.info(f"Converting {col} from American to European number format")
                df_mapped[col] = df_mapped[col].apply(self.convert_american_to_european_number)
        
        # Clean coupon_rate as regular numeric (no European conversion needed)
        if 'coupon_rate' in df_mapped.columns:
            logger.info("Cleaning numeric column: coupon_rate")
            df_mapped['coupon_rate'] = df_mapped['coupon_rate'].apply(self.clean_numeric_value)
        
        # Special handling for price column with detailed classification
        if 'price' in df_mapped.columns:
            logger.info("Converting prices with detailed classification...")
            df_mapped['price'] = df_mapped.apply(
                lambda row: self.convert_bond_price(
                    row['price'], 
                    row.get('asset_type'),
                    row.get('asset_strategy'),
                    row.get('asset_strategy_detail'),
                    row.get('ticker'),
                    row.get('name'),
                    row.get('maturity_date')
                ), axis=1
            )
        
        # Step 3b: Clean and format date columns
        date_columns = ['maturity_date']
        for col in date_columns:
            if col in df_mapped.columns:
                logger.info(f"Cleaning date column: {col}")
                df_mapped[col] = df_mapped[col].apply(self.clean_date_value)
        
        # Step 3c: Reclassify asset types based on detailed classification
        if 'asset_type' in df_mapped.columns:
            logger.info("Reclassifying asset types based on detailed classification...")
            df_mapped['asset_type'] = df_mapped.apply(
                lambda row: self.reclassify_asset_type(
                    row.get('asset_type'),
                    row.get('asset_strategy'),
                    row.get('asset_strategy_detail')
                ), axis=1
            )
        
        # Step 3d: Clean text columns
        text_columns = ['asset_type', 'name', 'ticker', 'cusip']
        for col in text_columns:
            if col in df_mapped.columns:
                logger.info(f"Cleaning text column: {col}")
                df_mapped[col] = df_mapped[col].apply(self.clean_text_value)
        
        # Step 3e: Remove temporary classification columns
        temp_columns = ['asset_strategy', 'asset_strategy_detail']
        for col in temp_columns:
            if col in df_mapped.columns:
                df_mapped = df_mapped.drop(columns=[col])
                logger.info(f"Removed temporary column: {col}")
        
        # Step 3f: Reorder columns to match target format
        target_columns = ['client', 'account', 'bank', 'asset_type', 'name', 'ticker', 'cusip', 
                         'quantity', 'price', 'market_value', 'cost_basis', 'coupon_rate', 'maturity_date']
        
        # Only include columns that exist
        final_columns = [col for col in target_columns if col in df_mapped.columns]
        df_final = df_mapped[final_columns]
        
        logger.info(f"Final securities dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
        logger.info(f"Final columns: {list(df_final.columns)}")
        
        return df_final
    
    def transform_transactions(self, transactions_file: str, mappings_file: str) -> pd.DataFrame:
        """
        Transform JPMorgan transactions to standard format.
        
        Args:
            transactions_file: Path to JPM transactions Excel file
            mappings_file: Path to JPM mappings Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file (skip first row for transactions)
        logger.info(f"Loading transactions file: {transactions_file}")
        df = pd.read_excel(transactions_file)
        logger.info(f"Loaded {len(df)} transaction records with {len(df.columns)} columns")
        
        # Get column mappings
        column_map = self.get_transactions_column_mappings()
        
        # Validate required JPM columns exist
        jpm_required = list(column_map.values())
        missing_cols = [col for col in jpm_required if col not in df.columns]
        if missing_cols:
            logger.error(f"JPMorgan transactions file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"JPMorgan transactions file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_jpm_columns = list(column_map.values())
        logger.info(f"Keeping {len(required_jpm_columns)} required columns: {required_jpm_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[required_jpm_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename JPMorgan columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {jpm_col: std_col for std_col, jpm_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
        
        # Load account mappings
        mappings = self.load_account_mappings(mappings_file, 'JPM')
        
        # Step 2a: Add bank column
        df_renamed['bank'] = self.bank_code
        logger.info(f"Added bank column with value: {self.bank_code}")
        
        # Step 2b: Map account numbers to client and account
        logger.info("Mapping account numbers to client and account...")
        client_list = []
        account_list = []
        unmapped_count = 0
        
        for _, row in df_renamed.iterrows():
            client, account = self.map_account_to_client(
                row['account_number'], 
                row.get('account_name', ''), 
                mappings
            )
            client_list.append(client)
            account_list.append(account)
            
            if client is None or account is None:
                unmapped_count += 1
        
        df_renamed['client'] = client_list
        df_renamed['account'] = account_list
        
        logger.info(f"Mapped {len(df_renamed) - unmapped_count} accounts successfully")
        if unmapped_count > 0:
            logger.warning(f"{unmapped_count} accounts could not be mapped")
        
        # Remove rows with unmapped accounts
        df_mapped = df_renamed.dropna(subset=['client', 'account'])
        logger.info(f"After removing unmapped accounts: {len(df_mapped)} rows")
        
        # Step 2c: Remove the temporary account_number column
        # Remove temporary columns
        temp_cols_to_remove = ['account_number', 'account_name']
        for col in temp_cols_to_remove:
            if col in df_mapped.columns:
                df_mapped = df_mapped.drop(columns=[col])
        
        logger.info("=== STEP 3: FORMAT ADJUSTMENTS ===")
        
        # Step 3a: Convert American to European number format for specific columns
        european_columns = ['quantity', 'amount']
        for col in european_columns:
            if col in df_mapped.columns:
                logger.info(f"Converting {col} from American to European number format")
                df_mapped[col] = df_mapped[col].apply(self.convert_american_to_european_number)
        
        # Special handling for price column - assume all transaction prices could be bonds
        if 'price' in df_mapped.columns:
            logger.info("Converting transaction prices with bond formatting...")
            df_mapped['price'] = df_mapped['price'].apply(lambda price: self.convert_bond_price(price, "Fixed Income & Cash", None, None, None, None, None))
        
        # Step 3b: Clean and format date columns (TRANSACTIONS ONLY - with day/month swap fix)
        date_columns = ['date']
        for col in date_columns:
            if col in df_mapped.columns:
                logger.info(f"Cleaning date column: {col} (with transactions-specific fix)")
                df_mapped[col] = df_mapped[col].apply(self.clean_date_value_transactions_only)
        
        # Step 3c: Clean text columns
        text_columns = ['transaction_type', 'cusip']
        for col in text_columns:
            if col in df_mapped.columns:
                logger.info(f"Cleaning text column: {col}")
                df_mapped[col] = df_mapped[col].apply(self.clean_text_value)
        
        # Step 3d: Reorder columns to match target format
        target_columns = ['client', 'account', 'bank', 'date', 'transaction_type', 'cusip', 
                         'price', 'quantity', 'amount']
        
        # Only include columns that exist
        final_columns = [col for col in target_columns if col in df_mapped.columns]
        df_final = df_mapped[final_columns]
        
        logger.info(f"Final transactions dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
        logger.info(f"Final columns: {list(df_final.columns)}")
        
        return df_final
    
    def process_files(self, input_dir: str, output_dir: str, date_str: str) -> Tuple[bool, bool]:
        """
        Process JPMorgan files and save to output directory.
        
        Args:
            input_dir: Directory containing raw JPM files
            output_dir: Directory to save processed files
            date_str: Date string for output filenames (DD_MM_YYYY format)
            
        Returns:
            Tuple of (securities_processed, transactions_processed)
        """
        logger.info(f"Processing JPMorgan files from {input_dir}")
        
        # Expected filenames (handle both naming conventions)
        securities_file = os.path.join(input_dir, f"JPM_Securities_{date_str}.xlsx")
        if not os.path.exists(securities_file):
            securities_file = os.path.join(input_dir, f"JPM_securities_{date_str}.xlsx")
            
        transactions_file = os.path.join(input_dir, f"JPM_transactions_{date_str}.xlsx")
        mappings_file = os.path.join(input_dir, "Mappings.xlsx")
        
        # Check if mappings file exists
        if not os.path.exists(mappings_file):
            logger.error(f"Mappings file not found: {mappings_file}")
            raise FileNotFoundError(f"Required mappings file not found: {mappings_file}")
        
        securities_processed = False
        transactions_processed = False
        
        # Process securities file
        if os.path.exists(securities_file):
            logger.info(f"Processing securities file: {securities_file}")
            try:
                df_securities = self.transform_securities(securities_file, mappings_file)
                
                # Save to output directory
                output_file = os.path.join(output_dir, f"securities_{date_str.replace('_', '_')}.xlsx")
                df_securities.to_excel(output_file, index=False)
                logger.info(f"Saved processed securities to: {output_file}")
                securities_processed = True
                
            except Exception as e:
                logger.error(f"Error processing securities file: {e}")
                raise
        else:
            logger.warning(f"Securities file not found: {securities_file}")
        
        # Process transactions file
        if os.path.exists(transactions_file):
            logger.info(f"Processing transactions file: {transactions_file}")
            try:
                df_transactions = self.transform_transactions(transactions_file, mappings_file)
                
                # Save to output directory
                output_file = os.path.join(output_dir, f"transactions_{date_str.replace('_', '_')}.xlsx")
                df_transactions.to_excel(output_file, index=False)
                logger.info(f"Saved processed transactions to: {output_file}")
                transactions_processed = True
                
            except Exception as e:
                logger.error(f"Error processing transactions file: {e}")
                raise
        else:
            logger.warning(f"Transactions file not found: {transactions_file}")
        
        return securities_processed, transactions_processed 