"""Morgan Stanley Excel file transformer for preprocessing raw bank files."""

import pandas as pd
import logging
from portfolio.services.mappings_encryption_service import MappingsEncryptionService
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class MorganStanleyTransformer:
    """Transformer for Morgan Stanley Excel files following step-by-step approach."""
    
    def __init__(self):
        self.bank_code = 'MS'
        logger.info(f"Initialized {self.bank_code} transformer")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Morgan Stanley securities files (MS → Standard)."""
        return {
            # Standard Column → Morgan Stanley Column (from actual file)
            'account_number': 'Account Number',
            'name': 'Name',
            'asset_type': 'Product Type',
            'ticker': 'Symbol',
            'cusip': 'CUSIP',  # Uppercase in MS
            'price': 'Last ($)',
            'quantity': 'Quantity',
            'market_value': 'Market Value ($)',
            'cost_basis': 'Total Cost ($)',
            'coupon_rate': 'Coupon Rate (%)',
            'maturity_date': 'Maturity Date'
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Morgan Stanley transactions files (MS → Standard)."""
        return {
            # Standard Column → Morgan Stanley Column (from actual file)
            'date': 'Activity Date',
            'account_number': 'Account',
            'transaction_type': 'Activity',
            'cusip': 'Cusip',  # Lowercase in MS
            'price': 'Price($)',
            'quantity': 'Quantity',
            'amount': 'Amount($)'
        }
    
    def load_account_mappings(self, mappings_file: str, sheet_name: str = 'MS') -> Dict[str, Dict[str, str]]:
        """
        Load account mappings from Excel file.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            sheet_name: Sheet name to read from (default: 'MS')
            
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
            
            # Create mapping dictionary
            mappings = {}
            
            for _, row in df.iterrows():
                account_num = str(row['account number']).strip()
                client = row['client']
                account = row['account']
                
                # Skip rows with missing client data
                if pd.isna(client) or pd.isna(account):
                    logger.warning(f"Skipping mapping for account {account_num} - missing client or account data")
                    continue
                
                mapping_data = {
                    'client': str(client).strip(),
                    'account': str(account).strip()
                }
                
                # For MS, we don't expect duplicate account numbers initially
                mappings[account_num] = mapping_data
            
            logger.info(f"Loaded {len(mappings)} MS account mappings")
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
    
    def convert_ms_bond_price(self, price, maturity_date) -> Optional[str]:
        """
        Convert MS bond prices - input already in European format.
        
        Logic:
        1. Check if asset is bond (has maturity date)
        2. If bond: remove comma, check first digit, reposition comma
        3. If not bond: keep European format as-is
        
        Args:
            price: Original price value (European format like "100,224")
            maturity_date: Maturity date (for bond detection)
            
        Returns:
            Converted price as string with European decimal format
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        # Check if this is a bond (has maturity date)
        if pd.notna(maturity_date) and str(maturity_date).strip() != '-':
            # This is a bond - input is European format like "100,224"
            price_str = str(price).replace(',', '').replace('.', '')  # Remove comma and any periods: "100224"
            
            if price_str.startswith('1'):
                # Starts with 1: place comma after first digit
                return f"1,{price_str[1:]}"  # "100224" → "1,00224"
            else:
                # Starts with other number: place comma before
                return f"0,{price_str}"      # "99456" → "0,99456"
        else:
            # Not a bond - keep original format as-is (no conversion)
            return price  # Keep original value unchanged
    
    def reclassify_ms_asset_type(self, asset_type: str, ticker: str = None) -> str:
        """
        Reclassify MS asset type to standard format.
        
        Args:
            asset_type: Original MS asset type
            ticker: Ticker symbol (for special cases like WWTTN)
            
        Returns:
            Reclassified asset type
        """
        if pd.isna(asset_type):
            return asset_type
        
        asset_type_str = str(asset_type).strip()
        
        # MS Asset Type Mapping
        if asset_type_str == 'Stocks / Options':
            return 'Equity'
        elif asset_type_str == 'ETFs / CEFs':
            return 'Equity'
        elif asset_type_str == 'Corporate Fixed Income':
            return 'Fixed Income'
        elif asset_type_str == 'Government Securities':
            return 'Fixed Income'
        elif asset_type_str == 'Mutual Funds':
            # Special case: WWTTN ticker should be Money Market
            if ticker and str(ticker).strip() == 'WWTTN':
                return 'Money Market'
            else:
                return 'Fixed Income'
        elif asset_type_str == 'Cash, MMF and BDP':
            return 'Cash'
        elif asset_type_str == 'Savings & Time Deposits':
            return 'Money Market'
        else:
            # Return original if no mapping found
            return asset_type_str
    
    def clean_text_value(self, value) -> Optional[str]:
        """Clean text values."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        # Convert to string and strip whitespace
        cleaned = str(value).strip()
        
        # Return None for empty strings
        return cleaned if cleaned else None
    
    def clean_date_value(self, value) -> Optional[str]:
        """Clean and format date values to MM/DD/YYYY format."""
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
    
    def map_account_to_client(self, account_number: str, mappings: Dict[str, Dict[str, str]]) -> Tuple[Optional[str], Optional[str]]:
        """
        Map Morgan Stanley account number to client and account using mappings.
        
        Args:
            account_number: Morgan Stanley account number
            mappings: Account mappings dictionary
            
        Returns:
            Tuple of (client, account) or (None, None) if not found
        """
        if not account_number or pd.isna(account_number):
            return None, None
        
        account_str = str(account_number).strip()
        
        # Try exact match
        if account_str in mappings:
            mapping = mappings[account_str]
            return mapping['client'], mapping['account']
        
        # Try partial match (account number might be truncated)
        for mapped_account, mapping in mappings.items():
            if (mapped_account in account_str or account_str in mapped_account):
                logger.info(f"Partial match found: {account_str} → {mapped_account}")
                return mapping['client'], mapping['account']
        
        logger.warning(f"No mapping found for MS account number: {account_str}")
        return None, None
    
    def transform_securities(self, securities_file: str, mappings_file: str) -> pd.DataFrame:
        """
        Transform Morgan Stanley securities to standard format.
        
        Args:
            securities_file: Path to MS securities Excel file
            mappings_file: Path to mappings Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file with header=10 (data starts at row 11)
        logger.info(f"Loading securities file: {securities_file}")
        df = pd.read_excel(securities_file, header=10)
        logger.info(f"Loaded {len(df)} securities records with {len(df.columns)} columns")
        
        # Get column mappings
        column_map = self.get_securities_column_mappings()
        
        # Validate required MS columns exist
        ms_required = list(column_map.values())
        missing_cols = [col for col in ms_required if col not in df.columns]
        if missing_cols:
            logger.error(f"Morgan Stanley securities file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"Morgan Stanley securities file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_ms_columns = list(column_map.values())
        logger.info(f"Keeping {len(required_ms_columns)} required columns: {required_ms_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[required_ms_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename Morgan Stanley columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {ms_col: std_col for std_col, ms_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
        
        # Load account mappings
        mappings = self.load_account_mappings(mappings_file, 'MS')
        
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
        temp_cols_to_remove = ['account_number']
        for col in temp_cols_to_remove:
            if col in df_mapped.columns:
                df_mapped = df_mapped.drop(columns=[col])
                logger.info(f"Removed temporary column: {col}")
        
        logger.info("=== STEP 3: FORMAT ADJUSTMENTS ===")
        
        # Step 3a: MS numbers are already in European format - no conversion needed
        logger.info("MS numbers already in European format - no conversion needed")
        
        # Clean coupon_rate as regular numeric
        if 'coupon_rate' in df_mapped.columns:
            logger.info("Cleaning numeric column: coupon_rate")
            df_mapped['coupon_rate'] = df_mapped['coupon_rate'].apply(self.clean_numeric_value)
        
        # Step 3b: Special handling for price column with bond logic
        if 'price' in df_mapped.columns:
            logger.info("Converting prices with MS bond logic...")
            df_mapped['price'] = df_mapped.apply(
                lambda row: self.convert_ms_bond_price(
                    row['price'], 
                    row.get('maturity_date')
                ), axis=1
            )
        
        # Step 3c: Clean and format date columns
        date_columns = ['maturity_date']
        for col in date_columns:
            if col in df_mapped.columns:
                logger.info(f"Cleaning date column: {col}")
                df_mapped[col] = df_mapped[col].apply(self.clean_date_value)
        
        # Step 3d: Clean text columns
        text_columns = ['asset_type', 'name', 'ticker', 'cusip']
        for col in text_columns:
            if col in df_mapped.columns:
                logger.info(f"Cleaning text column: {col}")
                df_mapped[col] = df_mapped[col].apply(self.clean_text_value)
        
        # Step 3e: Reclassify asset types to standard format
        if 'asset_type' in df_mapped.columns:
            logger.info("Reclassifying asset types to standard format...")
            df_mapped['asset_type'] = df_mapped.apply(
                lambda row: self.reclassify_ms_asset_type(
                    row['asset_type'], 
                    row.get('ticker')
                ), axis=1
            )
        
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
        Transform Morgan Stanley transactions to standard format.
        
        Args:
            transactions_file: Path to MS transactions Excel file
            mappings_file: Path to mappings Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file with header=6 (data starts at row 7)
        logger.info(f"Loading transactions file: {transactions_file}")
        df = pd.read_excel(transactions_file, header=6)
        logger.info(f"Loaded {len(df)} transaction records with {len(df.columns)} columns")
        
        # Get column mappings
        column_map = self.get_transactions_column_mappings()
        
        # Validate required MS columns exist
        ms_required = list(column_map.values())
        missing_cols = [col for col in ms_required if col not in df.columns]
        if missing_cols:
            logger.error(f"Morgan Stanley transactions file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"Morgan Stanley transactions file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_ms_columns = list(column_map.values())
        logger.info(f"Keeping {len(required_ms_columns)} required columns: {required_ms_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[required_ms_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename Morgan Stanley columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {ms_col: std_col for std_col, ms_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
        
        # Load account mappings
        mappings = self.load_account_mappings(mappings_file, 'MS')
        
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
        temp_cols_to_remove = ['account_number']
        for col in temp_cols_to_remove:
            if col in df_mapped.columns:
                df_mapped = df_mapped.drop(columns=[col])
                logger.info(f"Removed temporary column: {col}")
        
        logger.info("=== STEP 3: FORMAT ADJUSTMENTS ===")
        
        # Step 3a: MS numbers are already in European format - no conversion needed
        logger.info("MS numbers already in European format - no conversion needed")
        
        # Step 3b: Clean and format date columns
        date_columns = ['date']
        for col in date_columns:
            if col in df_mapped.columns:
                logger.info(f"Cleaning date column: {col}")
                df_mapped[col] = df_mapped[col].apply(self.clean_date_value)
        
        # Step 3c: Clean text columns (allow null CUSIP gracefully)
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
        Process Morgan Stanley files and save to output directory.
        
        Args:
            input_dir: Directory containing raw MS files
            output_dir: Directory to save processed files
            date_str: Date string for output filenames (DD_MM_YYYY format)
            
        Returns:
            Tuple of (securities_processed, transactions_processed)
        """
        logger.info(f"Processing Morgan Stanley files from {input_dir}")
        
        # Expected filenames
        securities_file = os.path.join(input_dir, f"MS_securities_{date_str}.xlsx")
        transactions_file = os.path.join(input_dir, f"MS_transactions_{date_str}.xlsx")
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