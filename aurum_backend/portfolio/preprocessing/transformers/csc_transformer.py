"""Charles Schwab (CSC) Excel file transformer for preprocessing combined bank files."""

import pandas as pd
import logging
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class CSCTransformer:
    """Transformer for Charles Schwab (CSC) Excel files following step-by-step approach."""
    
    def __init__(self):
        self.bank_code = 'CSC'
        logger.info(f"Initialized {self.bank_code} transformer")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for CSC securities files (CSC → Standard)."""
        return {
            # System columns (already exist from combiner)
            'bank': 'bank',
            'client': 'client', 
            'account': 'account',
            # CSC-specific columns
            'asset_type': 'Security Type',
            'name': 'Description',
            'ticker': 'Symbol',
            # Note: cusip will be created by copying ticker value
            'quantity': 'Qty (Quantity)',
            'price': 'Price',
            'market_value': 'Mkt Val (Market Value)',
            'cost_basis': 'Cost Basis'
            # coupon_rate and maturity_date will be added as None
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for CSC transactions files (CSC → Standard)."""
        return {
            'client': 'client',
            'bank': 'bank',
            'account': 'account',
            'date': 'Date',
            'transaction_type': 'Action',  # Direct mapping
            'cusip': 'Symbol',  # Use Symbol as CUSIP for transactions
            'price': 'Price',
            'quantity': 'Quantity',
            'amount': 'Amount'
        }
    
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
    
    def reclassify_csc_asset_type(self, asset_type: str) -> str:
        """
        Reclassify CSC asset type to standard format.
        
        Args:
            asset_type: Original CSC asset type
            
        Returns:
            Reclassified asset type
        """
        if pd.isna(asset_type):
            return asset_type
        
        asset_type_str = str(asset_type).strip()
        
        # CSC Asset Type Mapping
        mapping = {
            'Equity': 'Equity',
            'Fixed Income': 'Fixed Income',
            'ETFs & Closed End Funds': 'Equity',  # Fixed: uppercase 'F' in 'Funds'
            'Cash and Money Market': 'Money Market'
        }
        
        result = mapping.get(asset_type_str, asset_type_str)
        if result != asset_type_str:
            logger.debug(f"Reclassified asset type: '{asset_type_str}' → '{result}'")
        
        return result
    
    def handle_cash_and_money_market_names(self, name: str, ticker: str) -> str:
        """
        Handle special case for Cash & Money Market assets and Cash & Cash Investments.
        If name is "--" and ticker is "Cash & Money Market", set name to "Money Market".
        If name is "--" and ticker is "Cash & Cash Investments", set name to "Cash & Cash Investments".
        
        Args:
            name: Original name/description
            ticker: Ticker/symbol
            
        Returns:
            Processed name
        """
        # Handle Cash & Money Market case
        if (pd.isna(name) or str(name).strip() == '--') and \
           (pd.notna(ticker) and str(ticker).strip() == 'Cash & Money Market'):
            logger.debug(f"Converting Cash & Money Market name: '--' → 'Money Market'")
            return 'Money Market'
        
        # Handle Cash & Cash Investments case
        if (pd.isna(name) or str(name).strip() == '--') and \
           (pd.notna(ticker) and str(ticker).strip() == 'Cash & Cash Investments'):
            logger.debug(f"Converting Cash & Cash Investments name: '--' → 'Cash & Cash Investments'")
            return 'Cash & Cash Investments'
            
        return name
    
    def convert_cash_tickers(self, ticker: str, name: str) -> str:
        """
        Convert cash-related tickers to simplified format.
        If asset name is "Cash & Cash Investments", convert ticker to "Cash".
        
        Args:
            ticker: Original ticker/symbol
            name: Asset name (after name conversion)
            
        Returns:
            Converted ticker
        """
        # Convert "Cash & Cash Investments" ticker to "Cash"
        if (pd.notna(name) and str(name).strip() == 'Cash & Cash Investments'):
            logger.debug(f"Converting cash ticker: '{ticker}' → 'Cash'")
            return 'Cash'
            
        return ticker
    
    def clean_dash_values(self, value) -> Optional[str]:
        """
        Clean dash values - convert "--" to None/empty.
        
        Args:
            value: Value to clean
            
        Returns:
            Cleaned value or None
        """
        if pd.isna(value) or str(value).strip() == '--':
            return None
        return str(value).strip()
    
    def convert_csc_bond_price(self, price, asset_type) -> Optional[str]:
        """
        Convert CSC bond prices with special logic for Fixed Income assets.
        
        Logic:
        - For Fixed Income: Remove commas/periods, then:
          - If starts with "1": place comma after 1 (12345 → 1,2345)
          - If starts with any other number: place comma before (89765 → 0,89765)
        - For other assets: just convert American → European
        
        Args:
            price: Original price value
            asset_type: Asset type (to determine if bond pricing needed)
            
        Returns:
            Converted price as string with European decimal format
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        if asset_type == 'Fixed Income':
            logger.debug(f"Applying bond pricing logic to: {price}")
            
            # First convert to European format to handle American formatting
            european_price = self.convert_american_to_european_number(price)
            if european_price:
                # Remove commas and periods for bond processing
                price_clean = str(european_price).replace(',', '').replace('.', '')
                
                if price_clean.startswith('1'):
                    result = f"1,{price_clean[1:]}"
                    logger.debug(f"Bond price (starts with 1): {price} → {result}")
                    return result
                else:
                    result = f"0,{price_clean}"
                    logger.debug(f"Bond price (other number): {price} → {result}")
                    return result
            else:
                logger.warning(f"Could not process bond price: {price}")
                return None
        else:
            # Non-bond: just convert American → European
            return self.convert_american_to_european_number(price)
    
    def clean_text_value(self, value) -> Optional[str]:
        """Clean text values."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        # Clean string and handle special cases
        cleaned = str(value).strip()
        
        # Convert "--" to None
        if cleaned == '--':
            return None
            
        return cleaned
    
    def clean_date_value(self, value) -> Optional[str]:
        """Clean and format date values to MM/DD/YYYY format."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # Handle various date formats
            if isinstance(value, datetime):
                return value.strftime('%m/%d/%Y')
            
            # Try to parse string dates
            date_str = str(value).strip()
            if date_str:
                # Try common formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y%m%d']:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        return parsed_date.strftime('%m/%d/%Y')
                    except ValueError:
                        continue
                        
                logger.warning(f"Could not parse date: {date_str}")
                return date_str  # Return original if can't parse
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Error cleaning date value '{value}': {str(e)}")
            return None
    
    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform CSC securities to standard format.
        
        Args:
            securities_file: Path to CSC securities Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file (header in row 1)
        logger.info(f"Loading securities file: {securities_file}")
        df = pd.read_excel(securities_file)
        logger.info(f"Loaded {len(df)} securities records with {len(df.columns)} columns")
        
        # Get column mappings
        column_map = self.get_securities_column_mappings()
        
        # Validate required CSC columns exist
        csc_required = list(column_map.values())
        missing_cols = [col for col in csc_required if col not in df.columns]
        if missing_cols:
            logger.error(f"CSC securities file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"CSC securities file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_csc_columns = list(column_map.values())
        logger.info(f"Keeping {len(required_csc_columns)} required columns: {required_csc_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[required_csc_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename CSC columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {csc_col: std_col for std_col, csc_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        # Create cusip column by copying ticker (as per requirement: "Use Symbol as Cusip too")
        if 'ticker' in df_renamed.columns:
            df_renamed['cusip'] = df_renamed['ticker']
            logger.info("Created cusip column by copying ticker values")
        
        logger.info("=== STEP 2: ADD MISSING COLUMNS ===")
        
        # Add missing standard columns with None values
        df_renamed['coupon_rate'] = None
        df_renamed['maturity_date'] = None
        logger.info("Added missing columns: coupon_rate, maturity_date (set to None)")
        
        logger.info("=== STEP 3: FORMAT ADJUSTMENTS ===")
        
        # Step 3a: Handle Cash & Money Market names
        logger.info("Handling Cash & Money Market name conversions...")
        df_renamed['name'] = df_renamed.apply(
            lambda row: self.handle_cash_and_money_market_names(row['name'], row['ticker']), 
            axis=1
        )
        
        # Step 3a2: Convert cash tickers after name conversion
        logger.info("Converting cash-related tickers...")
        df_renamed['ticker'] = df_renamed.apply(
            lambda row: self.convert_cash_tickers(row['ticker'], row['name']),
            axis=1
        )
        
        # Step 3a3: Update cusip to match converted ticker
        logger.info("Updating cusip to match converted ticker...")
        df_renamed['cusip'] = df_renamed['ticker']
        
        # Step 3b: Clean dash values
        logger.info("Cleaning dash values...")
        columns_to_clean = ['name', 'ticker', 'cusip', 'quantity', 'price', 'market_value', 'cost_basis']
        for col in columns_to_clean:
            if col in df_renamed.columns:
                logger.info(f"Cleaning dash values in column: {col}")
                df_renamed[col] = df_renamed[col].apply(self.clean_dash_values)
        
        # Step 3c: Reclassify asset types
        logger.info("Reclassifying asset types...")
        df_renamed['asset_type'] = df_renamed['asset_type'].apply(self.reclassify_csc_asset_type)
        
        # Step 3d: Convert American to European number format for specific columns
        logger.info("Converting numeric columns from American to European format...")
        numeric_columns = ['quantity', 'market_value', 'cost_basis']
        for col in numeric_columns:
            if col in df_renamed.columns:
                logger.info(f"Converting {col} from American to European number format")
                df_renamed[col] = df_renamed[col].apply(self.convert_american_to_european_number)
        
        # Step 3e: Handle bond pricing for price column
        logger.info("Converting prices with CSC bond pricing logic...")
        df_renamed['price'] = df_renamed.apply(
            lambda row: self.convert_csc_bond_price(row['price'], row['asset_type']),
            axis=1
        )
        
        # Step 3f: Clean text columns
        text_columns = ['name', 'ticker', 'cusip']
        for col in text_columns:
            if col in df_renamed.columns:
                logger.info(f"Cleaning text column: {col}")
                df_renamed[col] = df_renamed[col].apply(self.clean_text_value)
        
        # Step 3g: Reorder columns to match target format
        target_columns = ['client', 'account', 'bank', 'asset_type', 'name', 'ticker', 'cusip', 
                         'quantity', 'price', 'market_value', 'cost_basis', 'coupon_rate', 'maturity_date']
        
        # Only include columns that exist
        final_columns = [col for col in target_columns if col in df_renamed.columns]
        df_final = df_renamed[final_columns]
        
        logger.info(f"Final securities dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
        logger.info(f"Final columns: {list(df_final.columns)}")
        
        return df_final
    
    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """
        Transform CSC transactions to standard format.
        
        Args:
            transactions_file: Path to CSC transactions Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file (header in row 1)
        logger.info(f"Loading transactions file: {transactions_file}")
        df = pd.read_excel(transactions_file)
        logger.info(f"Loaded {len(df)} transaction records with {len(df.columns)} columns")
        
        # Get column mappings
        column_map = self.get_transactions_column_mappings()
        
        # Validate required CSC columns exist
        csc_required = list(column_map.values())
        missing_cols = [col for col in csc_required if col not in df.columns]
        if missing_cols:
            logger.error(f"CSC transactions file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"CSC transactions file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_csc_columns = list(column_map.values())
        logger.info(f"Keeping {len(required_csc_columns)} required columns: {required_csc_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[required_csc_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename CSC columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {csc_col: std_col for std_col, csc_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        logger.info("=== STEP 2: FORMAT ADJUSTMENTS ===")
        
        # Step 2a: Convert American to European number format for numeric columns
        logger.info("Converting numeric columns from American to European format...")
        numeric_columns = ['price', 'quantity', 'amount']
        for col in numeric_columns:
            if col in df_renamed.columns:
                logger.info(f"Converting {col} from American to European number format")
                df_renamed[col] = df_renamed[col].apply(self.convert_american_to_european_number)
        
        # Step 2b: Clean and format date columns
        date_columns = ['date']
        for col in date_columns:
            if col in df_renamed.columns:
                logger.info(f"Cleaning date column: {col}")
                df_renamed[col] = df_renamed[col].apply(self.clean_date_value)
        
        # Step 2c: Clean text columns
        text_columns = ['transaction_type', 'cusip']
        for col in text_columns:
            if col in df_renamed.columns:
                logger.info(f"Cleaning text column: {col}")
                df_renamed[col] = df_renamed[col].apply(self.clean_text_value)
        
        # Step 2d: Reorder columns to match target format
        target_columns = ['client', 'account', 'bank', 'date', 'transaction_type', 'cusip', 
                         'price', 'quantity', 'amount']
        
        # Only include columns that exist
        final_columns = [col for col in target_columns if col in df_renamed.columns]
        df_final = df_renamed[final_columns]
        
        logger.info(f"Final transactions dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
        logger.info(f"Final columns: {list(df_final.columns)}")
        
        return df_final
    
    def process_files(self, input_dir: str, output_dir: str, date_str: str) -> Tuple[bool, bool]:
        """
        Process CSC files and save to output directory.
        
        Args:
            input_dir: Directory containing combined CSC files
            output_dir: Directory to save processed files
            date_str: Date string for output filenames (DD_MM_YYYY format)
            
        Returns:
            Tuple of (securities_processed, transactions_processed)
        """
        logger.info(f"Processing CSC files from {input_dir}")
        
        # Expected filenames
        securities_file = os.path.join(input_dir, f"CSC_securities_{date_str}.xlsx")
        transactions_file = os.path.join(input_dir, f"CSC_transactions_{date_str}.xlsx")
        
        securities_processed = False
        transactions_processed = False
        
        # Process securities file
        if os.path.exists(securities_file):
            try:
                logger.info(f"Processing CSC securities: {securities_file}")
                securities_df = self.transform_securities(securities_file)
                
                # Save processed securities
                output_securities = os.path.join(output_dir, f"securities_{date_str}.xlsx")
                securities_df.to_excel(output_securities, index=False)
                logger.info(f"✅ CSC securities saved: {output_securities}")
                logger.info(f"📊 Securities: {len(securities_df)} rows, {len(securities_df.columns)} columns")
                securities_processed = True
                
            except Exception as e:
                logger.error(f"❌ Error processing CSC securities: {str(e)}")
                raise
        else:
            logger.warning(f"⚠️ CSC securities file not found: {securities_file}")
        
        # Process transactions file
        if os.path.exists(transactions_file):
            try:
                logger.info(f"Processing CSC transactions: {transactions_file}")
                transactions_df = self.transform_transactions(transactions_file)
                
                # Save processed transactions
                output_transactions = os.path.join(output_dir, f"transactions_{date_str}.xlsx")
                transactions_df.to_excel(output_transactions, index=False)
                logger.info(f"✅ CSC transactions saved: {output_transactions}")
                logger.info(f"📊 Transactions: {len(transactions_df)} rows, {len(transactions_df.columns)} columns")
                transactions_processed = True
                
            except Exception as e:
                logger.error(f"❌ Error processing CSC transactions: {str(e)}")
                raise
        else:
            logger.warning(f"⚠️ CSC transactions file not found: {transactions_file}")
        
        return securities_processed, transactions_processed 