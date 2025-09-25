"""Citigroup Excel file transformer for preprocessing raw bank files."""

import pandas as pd
import logging
from portfolio.services.mappings_encryption_service import MappingsEncryptionService
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class CitiTransformer:
    """Transformer for Citigroup Excel files following JPM/MS pattern."""
    
    def __init__(self):
        self.bank_code = 'Citi'
        logger.info(f"Initialized {self.bank_code} transformer")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Citi securities files (Citi → Standard)."""
        return {
            # Standard Column → Citi Column
            'account_number': 'Account Number',  # Internal use only, not in output
            'asset_class': 'Asset Class',  # Internal use only, not in output  
            'name': 'Description', 
            'quantity': 'Nominal Units',
            'cost_basis': 'Total Cost Basis (Nominal CCY)',
            'price': 'Market Price',
            'market_value': 'Market Value (Nominal CCY)',  # Added for output
            'cusip': 'ISIN'
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Citi transactions files (Citi → Standard)."""
        return {
            # Standard Column → Citi Column
            'account_number': 'Account Number',  # Internal use only, not in output
            'transaction_type': 'Type',
            'amount': 'Amount (Nominal CCY)',
            'cusip': 'ISIN',
            'quantity': 'Quantity',
            'date': 'Date Range'
        }
    
    def load_account_mappings(self, mappings_file: str, sheet_name: str = 'Citi') -> Dict[str, Dict[str, str]]:
        """
        Load account mappings from Excel file with enhanced transaction support.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            sheet_name: Sheet name to read from (default: 'Citi')
            
        Returns:
            Dict mapping account numbers to client/account info
        """
        logger.info(f"Loading account mappings from {mappings_file}, sheet: {sheet_name}")
        
        try:
            encryption_service = MappingsEncryptionService()
            df = encryption_service.read_encrypted_excel(mappings_file + '.encrypted', sheet_name=sheet_name)
            
            # Validate required columns
            required_cols = ['account number', 'client', 'account', 'account name']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Mappings file missing required columns: {missing_cols}")
            
            # Check if enhanced transaction_account_number column exists
            has_transaction_names = 'transactions_account_number' in df.columns
            if has_transaction_names:
                logger.info("Found transactions_account_number column - using enhanced mapping logic")
            else:
                logger.info("No transactions_account_number column - using standard mapping logic")
            
            # Create mapping dictionary
            mappings = {}
            duplicate_accounts = {}
            transaction_name_mappings = 0
            
            for _, row in df.iterrows():
                account_num = str(row['account number']).strip()
                client = row['client']
                account = row['account']
                account_name = row['account name']
                transactions_account_number = row.get('transactions_account_number', None) if has_transaction_names else None
                
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
                    
                    # ENHANCED: Also add transactions_account_number mapping if available
                    if has_transaction_names and pd.notna(transactions_account_number) and str(transactions_account_number).strip():
                        tx_combo_key = f"{account_num}|{str(transactions_account_number).strip()}"
                        mappings[tx_combo_key] = mapping_data
                        transaction_name_mappings += 1
                        logger.info(f"Added transaction name mapping: {account_num} + '{transactions_account_number}' → {client}, {account}")
                    
                    # Track duplicate accounts for later matching
                    if account_num not in duplicate_accounts:
                        duplicate_accounts[account_num] = []
                    duplicate_accounts[account_num].append(str(account_name).strip())
                    
                    logger.info(f"Added duplicate account mapping: {account_num} + '{account_name}' → {client}, {account}")
                else:
                    # Regular account (no duplicates)
                    mappings[account_num] = mapping_data
            
            logger.info(f"Loaded {len(mappings)} total Citi account mappings")
            logger.info(f"Found {len(duplicate_accounts)} accounts with duplicates: {list(duplicate_accounts.keys())}")
            if has_transaction_names:
                logger.info(f"Created {transaction_name_mappings} enhanced transaction name mappings")
            return mappings
            
        except Exception as e:
            logger.error(f"Error loading account mappings: {e}")
            raise
    
    def convert_american_to_european_number(self, value, handle_parentheses: bool = False) -> Optional[str]:
        """
        Convert American number format to European format with optional parentheses handling.
        
        Examples:
        - 300000.000 → 300000,000
        - 25,291.11 → 25.291,11
        - (373.09) → -373,09 (if handle_parentheses=True)
        """
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # Convert to string first
            str_value = str(value).strip()
            
            # Handle parentheses for negative numbers (transactions)
            is_negative = False
            if handle_parentheses and str_value.startswith('(') and str_value.endswith(')'):
                is_negative = True
                str_value = str_value[1:-1].strip()  # Remove parentheses
            
            # Remove common formatting characters except commas and periods
            str_value = str_value.replace('$', '')  # Remove dollar signs
            str_value = str_value.replace('%', '')  # Remove percentage signs
            str_value = str_value.replace(' ', '')  # Remove spaces
            
            # Handle existing negative signs
            if str_value.startswith('-'):
                is_negative = True
                str_value = str_value[1:]
            
            # Handle the conversion: American → European
            if ',' in str_value and '.' in str_value:
                # Both comma and period present - typical American format
                # Example: 25,291.11 → 25.291,11
                parts = str_value.split('.')
                if len(parts) == 2:
                    integer_part = parts[0].replace(',', '.')  # Replace commas with periods for thousands
                    decimal_part = parts[1]
                    european_format = f"{integer_part},{decimal_part}"
                else:
                    # Multiple periods - treat as is
                    european_format = str_value
            elif ',' in str_value and '.' not in str_value:
                # Only comma present - could be thousands separator or decimal
                # For large numbers like 300,000 assume thousands separator → 300.000
                # For small numbers like 1,23 assume decimal → 1,23
                if len(str_value.split(',')[-1]) == 3:  # Likely thousands: 300,000
                    european_format = str_value.replace(',', '.')
                else:  # Likely decimal: 1,23
                    european_format = str_value
            elif '.' in str_value and ',' not in str_value:
                # Only period present - convert to comma for decimal
                # Example: 300000.000 → 300000,000
                european_format = str_value.replace('.', ',')
            else:
                # No special formatting needed
                european_format = str_value
            
            # Apply negative sign if needed
            if is_negative:
                european_format = f"-{european_format}"
            
            return european_format
                
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to European number format")
            return str(value) if value else None
    
    def classify_citi_asset_type(self, asset_class: str, name: str = None) -> str:
        """
        Classify Citi asset class to standard asset type with special logic.
        
        Args:
            asset_class: Citi asset class value
            name: Asset name/description for additional classification logic
            
        Returns:
            Standard asset type
        """
        if pd.isna(asset_class):
            return asset_class
        
        asset_class_str = str(asset_class).strip()
        name_str = str(name).upper() if pd.notna(name) else ""
        
        # Priority logic: Check for Structured Products first (overrides asset class)
        if 'STRUCTURED PRODUCTS' in name_str:
            return 'Alternatives'  # All Structured Products are Alternatives
        
        # Citi Asset Class Mapping with special logic
        if asset_class_str == 'Cash/Cash Equivalents':
            # Special logic for Cash/Cash Equivalents (note: plural)
            if 'UNITED STATES TREASURY' in name_str:
                return 'Fixed Income'  # Short term treasuries should be Fixed Income
            else:
                return 'Money Market'  # Regular cash equivalents
        elif asset_class_str == 'Cash/Deposits':
            return 'Cash'
        elif asset_class_str == 'Cash/Investment Cash':
            return 'Cash'
        elif asset_class_str == 'Fixed Income':
            return 'Fixed Income'
        elif asset_class_str == 'Equities':
            return 'Equities'
        elif asset_class_str == 'Private Equity':
            return 'Alternatives'
        elif asset_class_str == 'Real Estate':
            return 'Alternatives'
        else:
            # Return original if no specific mapping found
            return asset_class_str
    
    def is_bond(self, asset_type: str, asset_class: str = None, name: str = None) -> bool:
        """
        Determine if an asset is a bond based on asset type/class and name.
        Enhanced logic for Fixed Income assets.
        
        Args:
            asset_type: Classified asset type
            asset_class: Original Citi asset class
            name: Asset name/description
            
        Returns:
            True if asset appears to be a bond, False otherwise
        """
        if pd.isna(asset_type) and pd.isna(asset_class):
            return False
        
        name_str = str(name).upper() if pd.notna(name) else ""
        asset_type_str = str(asset_type).strip() if pd.notna(asset_type) else ""
        
        # Enhanced Fixed Income bond detection
        if asset_type_str == 'Fixed Income':
            # Fixed Income + US TREASURY in name = bond
            if 'US TREASURY' in name_str:
                return True
            
            # Fixed Income + maturity date format in name = bond
            # Pattern: 15DEC27, 15FEB28 (DDMMMYY format)
            maturity_pattern = r'\b\d{1,2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}\b'
            if re.search(maturity_pattern, name_str):
                return True
            
            # Fixed Income assets that don't meet specific bond criteria are not bonds
            # (they may be other fixed income instruments that don't need bond price handling)
            return False
        
        # Special case: Cash/Cash Equivalents with UNITED STATES TREASURY in name = bond
        if (str(asset_class).strip() == 'Cash/Cash Equivalents' and 
            'UNITED STATES TREASURY' in name_str):
            return True
        
        # Special case: Alternatives with STRUCTURED PRODUCTS in name = bond (need bond price handling)
        if (asset_type_str == 'Alternatives' and 
            'STRUCTURED PRODUCTS' in name_str):
            return True
        
        # Check for other bond keywords in asset_class or asset_type
        combined_text = f"{asset_type or ''} {asset_class or ''}".lower()
        bond_keywords = [
            'fixed income', 'bond', 'treasury', 'corporate', 'government',
            'municipal', 'note', 'debenture', 'bills'
        ]
        
        return any(keyword in combined_text for keyword in bond_keywords)
    
    def apply_citi_bond_price_logic(self, price_european: str, asset_type: str, asset_class: str = None, name: str = None) -> str:
        """
        Apply bond price logic for Citi bonds.
        
        Examples:
        - If bond: 97,54 → 0,9754
        - If bond & starts with 1: 102,47 → 1,0247
        - If not bond: keep price_european unchanged
        
        Args:
            price_european: Price already converted to European format
            asset_type: Classified asset type
            asset_class: Original Citi asset class
            
        Returns:
            Price with bond logic applied
        """
        if not price_european or pd.isna(price_european):
            return price_european
        
        # Check if this is a bond
        if not self.is_bond(asset_type, asset_class, name):
            return price_european  # Not a bond, return as-is
        
        try:
            # This is a bond - apply bond price logic
            price_str = str(price_european).replace(',', '').replace('.', '')  # Remove formatting: "9754"
            
            if price_str.startswith('1'):
                # Starts with 1: place comma after first digit
                # Example: 102,47 → "10247" → "1,0247"
                return f"1,{price_str[1:]}"
            else:
                # Starts with other number: place comma before
                # Example: 97,54 → "9754" → "0,9754"
                return f"0,{price_str}"
        
        except Exception as e:
            logger.warning(f"Could not apply bond price logic to '{price_european}': {e}")
            return price_european
    
    def convert_date_format(self, date_value) -> Optional[str]:
        """
        Convert date to MM/DD/YYYY format.
        
        Example: 2025-08-26 → 08/26/2025
        """
        if pd.isna(date_value) or date_value == '' or date_value is None:
            return None
        
        try:
            # Handle different date input formats
            if isinstance(date_value, str):
                # Parse string dates
                if re.match(r'\d{4}-\d{2}-\d{2}', date_value):  # YYYY-MM-DD
                    date_obj = datetime.strptime(date_value, '%Y-%m-%d')
                elif re.match(r'\d{2}/\d{2}/\d{4}', date_value):  # MM/DD/YYYY
                    return date_value  # Already in correct format
                elif re.match(r'\d{2}-\d{2}-\d{4}', date_value):  # MM-DD-YYYY
                    date_obj = datetime.strptime(date_value, '%m-%d-%Y')
                elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_value):  # M/D/YYYY
                    date_obj = datetime.strptime(date_value, '%m/%d/%Y')
                else:
                    logger.warning(f"Unknown date format: {date_value}")
                    return str(date_value)
            else:
                # Handle datetime objects
                date_obj = pd.to_datetime(date_value)
            
            # Convert to MM/DD/YYYY format
            return date_obj.strftime('%m/%d/%Y')
            
        except Exception as e:
            logger.warning(f"Could not convert date: {date_value} - {e}")
            return str(date_value) if date_value else None
    
    def extract_maturity_date_from_name(self, name: str) -> str:
        """
        Extract maturity date from bond name and convert to MM/DD/YYYY format.
        
        Args:
            name: Bond name/description
            
        Returns:
            Maturity date in MM/DD/YYYY format or None if not found
        """
        if pd.isna(name):
            return None
        
        # Pattern: MAT 25APR30, MAT 22JUL33, etc.
        mat_pattern = r'MAT\s+(\d{1,2}[A-Z]{3}\d{2})'
        matches = re.findall(mat_pattern, str(name).upper())
        
        if matches:
            date_str = matches[0]  # e.g., '25APR30'
            return self.convert_mat_date_to_mmddyyyy(date_str)
        
        return None
    
    def convert_mat_date_to_mmddyyyy(self, date_str: str) -> str:
        """
        Convert maturity date from 25APR30 format to 04/25/2030 format.
        
        Args:
            date_str: Date string in DDMMMYY format (e.g., '25APR30')
            
        Returns:
            Date string in MM/DD/YYYY format (e.g., '04/25/2030')
        """
        month_mapping = {
            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 
            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
        }
        
        try:
            day = date_str[:2]
            month_abbr = date_str[2:5]
            year_short = date_str[5:7]
            
            month_num = month_mapping.get(month_abbr, '00')
            # All bond maturity years should be 20xx (bonds don't mature in the past)
            year_full = '20' + year_short
            
            return f'{month_num}/{day}/{year_full}'
        except (ValueError, IndexError):
            # Return None if parsing fails
            return None
    
    def extract_coupon_rate_from_name(self, name: str) -> str:
        """
        Extract coupon rate from bond name and convert to European format.
        
        Args:
            name: Bond name/description
            
        Returns:
            Coupon rate in European format (4.376% → 4,376) or None if not found
        """
        if pd.isna(name):
            return None
        
        # Pattern: 4%, 4.376%, 0.5%, etc.
        coupon_pattern = r'(\d+\.?\d*)%'
        matches = re.findall(coupon_pattern, str(name))
        
        if matches:
            coupon_value = matches[0]  # Take first match
            # Convert to European format: replace . with ,
            return coupon_value.replace('.', ',')
        
        return None
    
    def transform_securities(self, securities_file: str, mappings_file: str = None) -> pd.DataFrame:
        """
        Transform Citi securities file to standard format.
        
        Args:
            securities_file: Path to Citi securities Excel file
            mappings_file: Path to mappings Excel file
            
        Returns:
            Transformed securities DataFrame
        """
        try:
            logger.info(f"Processing Citi securities file: {securities_file}")
            
            # Load the securities file with header=1 (skip first row)
            df = pd.read_excel(securities_file, header=1)
            logger.info(f"Loaded {len(df)} securities records (header=1)")
            
            # Load mappings if provided
            account_mappings = {}
            if mappings_file:
                account_mappings = self.load_account_mappings(mappings_file)
            
            # Get column mappings
            col_mappings = self.get_securities_column_mappings()
            
            # Create output DataFrame with standard columns
            output_data = []
            
            for index, row in df.iterrows():
                try:
                    record = {}
                    
                    # Extract base data using column mappings
                    for standard_col, citi_col in col_mappings.items():
                        if citi_col in df.columns:
                            record[standard_col] = row[citi_col]
                        else:
                            logger.warning(f"Column '{citi_col}' not found in securities file")
                            record[standard_col] = None
                    
                    # Apply account mappings
                    if account_mappings and 'account_number' in record:
                        account_num = str(record['account_number']).strip()
                        
                        # Simple mapping lookup for securities
                        if account_num in account_mappings:
                            mapping = account_mappings[account_num]
                            record['client'] = mapping['client']
                            record['account'] = mapping['account']
                        else:
                            logger.warning(f"No mapping found for account: {account_num}")
                            record['client'] = 'UNMAPPED'
                            record['account'] = 'UNMAPPED'
                    else:
                        record['client'] = 'Citi'
                        record['account'] = record.get('account_number', 'DEFAULT')
                    
                    # Set bank
                    record['bank'] = 'Citi'
                    
                    # Classify asset type (pass name for special logic)
                    if 'asset_class' in record:
                        record['asset_type'] = self.classify_citi_asset_type(
                            record['asset_class'], 
                            record.get('name')
                        )
                    else:
                        record['asset_type'] = None
                    
                    # Convert numeric fields to European format
                    if 'quantity' in record:
                        record['quantity'] = self.convert_american_to_european_number(record['quantity'])
                    
                    if 'cost_basis' in record:
                        record['cost_basis'] = self.convert_american_to_european_number(record['cost_basis'])
                    
                    if 'market_value' in record:
                        record['market_value'] = self.convert_american_to_european_number(record['market_value'])
                    
                    # Handle price with bond logic
                    if 'price' in record:
                        # First convert to European format
                        price_european = self.convert_american_to_european_number(record['price'])
                        # Then apply bond logic if applicable
                        record['price'] = self.apply_citi_bond_price_logic(
                            price_european, 
                            record.get('asset_type'), 
                            record.get('asset_class'),
                            record.get('name')
                        )
                    
                    # Extract maturity date and coupon rate for bonds
                    if self.is_bond(record.get('asset_type'), record.get('asset_class'), record.get('name')):
                        record['maturity_date'] = self.extract_maturity_date_from_name(record.get('name'))
                        record['coupon_rate'] = self.extract_coupon_rate_from_name(record.get('name'))
                    else:
                        record['maturity_date'] = None
                        record['coupon_rate'] = None
                    
                    output_data.append(record)
                    
                except Exception as e:
                    logger.error(f"Error processing securities row {index}: {e}")
                    continue
            
            # Create DataFrame with all data
            full_df = pd.DataFrame(output_data)
            
            # Return only required columns for securities output
            required_columns = [
                'bank', 'client', 'account', 'asset_type', 'name', 'cusip', 
                'quantity', 'cost_basis', 'price', 'market_value', 'coupon_rate', 'maturity_date'
            ]
            
            # Select only columns that exist and are required
            result_columns = [col for col in required_columns if col in full_df.columns]
            result_df = full_df[result_columns].copy()
            
            logger.info(f"Transformed {len(result_df)} securities records successfully")
            logger.info(f"Output columns: {list(result_df.columns)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error transforming Citi securities file: {e}")
            raise
    
    def transform_transactions(self, transactions_file: str, mappings_file: str = None) -> pd.DataFrame:
        """
        Transform Citi transactions file to standard format.
        
        Args:
            transactions_file: Path to Citi transactions Excel file
            mappings_file: Path to mappings Excel file
            
        Returns:
            Transformed transactions DataFrame
        """
        try:
            logger.info(f"Processing Citi transactions file: {transactions_file}")
            
            # Load the transactions file with header=1 (skip first row like securities)
            df = pd.read_excel(transactions_file, header=1)
            logger.info(f"Loaded {len(df)} transaction records (header=1)")
            
            # Load mappings if provided
            account_mappings = {}
            if mappings_file:
                account_mappings = self.load_account_mappings(mappings_file)
            
            # Get column mappings
            col_mappings = self.get_transactions_column_mappings()
            
            # Create output DataFrame with standard columns
            output_data = []
            
            for index, row in df.iterrows():
                try:
                    record = {}
                    
                    # Extract base data using column mappings
                    for standard_col, citi_col in col_mappings.items():
                        if citi_col in df.columns:
                            record[standard_col] = row[citi_col]
                        else:
                            logger.warning(f"Column '{citi_col}' not found in transactions file")
                            record[standard_col] = None
                    
                    # Apply account mappings with enhanced transactions_account_number support
                    if account_mappings and 'account_number' in record:
                        account_num = str(record['account_number']).strip()
                        mapping_found = False
                        
                        # For transactions, find mapping using transactions_account_number
                        # Need to reload mappings to get transactions_account_number values
                        for mapping_key, mapping_data in account_mappings.items():
                            if '|' in mapping_key:
                                # This is a combination key - check if it contains our account number
                                key_parts = mapping_key.split('|', 1)
                                if key_parts[1] == account_num:  # transactions_account_number match
                                    record['client'] = mapping_data['client']
                                    record['account'] = mapping_data['account']
                                    mapping_found = True
                                    logger.debug(f"Mapped transaction account {account_num} using transactions_account_number mapping")
                                    break
                        
                        # If no transactions_account_number mapping found, try direct lookup
                        if not mapping_found:
                            # Try to match against transactions_account_number by reloading mappings
                            try:
                                encryption_service = MappingsEncryptionService()
                                mappings_df = encryption_service.read_encrypted_excel(mappings_file + '.encrypted', sheet_name='Citi')
                                for _, mapping_row in mappings_df.iterrows():
                                    tx_acc_num = mapping_row.get('transactions_account_number')
                                    if pd.notna(tx_acc_num) and str(tx_acc_num).strip() == account_num:
                                        record['client'] = str(mapping_row['client']).strip()
                                        record['account'] = str(mapping_row['account']).strip()
                                        mapping_found = True
                                        logger.debug(f"Mapped transaction account {account_num} using direct transactions_account_number lookup")
                                        break
                            except Exception as e:
                                logger.warning(f"Error doing direct transactions_account_number lookup: {e}")
                        
                        # If still no mapping found
                        if not mapping_found:
                            logger.warning(f"No mapping found for transaction account: {account_num}")
                            record['client'] = 'UNMAPPED'
                            record['account'] = 'UNMAPPED'
                    else:
                        record['client'] = 'Citi'
                        record['account'] = record.get('account_number', 'DEFAULT')
                    
                    # Set bank
                    record['bank'] = 'Citi'
                    
                    # Convert numeric fields with parentheses handling
                    if 'amount' in record:
                        record['amount'] = self.convert_american_to_european_number(
                            record['amount'], handle_parentheses=True
                        )
                    
                    if 'quantity' in record:
                        record['quantity'] = self.convert_american_to_european_number(record['quantity'])
                    
                    # Convert date format
                    if 'date' in record:
                        record['date'] = self.convert_date_format(record['date'])
                    
                    # Set empty price field as specified
                    record['price'] = None  # Leave empty for Citi transactions
                    
                    output_data.append(record)
                    
                except Exception as e:
                    logger.error(f"Error processing transactions row {index}: {e}")
                    continue
            
            # Create DataFrame with all data
            full_df = pd.DataFrame(output_data)
            
            # Return only required columns for transactions output
            required_columns = [
                'bank', 'client', 'account', 'date', 'quantity', 'cusip', 
                'amount', 'transaction_type', 'price'
            ]
            
            # Select only columns that exist and are required
            result_columns = [col for col in required_columns if col in full_df.columns]
            result_df = full_df[result_columns].copy()
            
            logger.info(f"Transformed {len(result_df)} transaction records successfully")
            logger.info(f"Output columns: {list(result_df.columns)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error transforming Citi transactions file: {e}")
            raise