"""Pershing Excel file transformer for preprocessing combined bank files."""

import pandas as pd
import logging
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class PershingTransformer:
    """Transformer for Pershing Excel files following step-by-step approach."""
    
    def __init__(self):
        self.bank_code = 'Pershing'
        logger.info(f"Initialized {self.bank_code} transformer")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Pershing securities files (Pershing → Standard)."""
        return {
            # System columns (already exist from combiner)
            'bank': 'Bank',
            'client': 'Client', 
            'account': 'Account',
            # Pershing-specific columns
            'cusip': 'CUSIP',
            'name': 'Description',
            'asset_type': 'Asset Classification',
            'quantity': 'Quantity',           # Already European format
            'price': 'Price',                 # Will process for bonds
            'market_value': 'Market Value',   # Already European format
            'cost_basis': 'Total Cost',       # Conditional conversion
            'ticker': 'Security ID',
            'maturity_date': 'maturity_date'  # Extracted by enricher
            # coupon_rate will be calculated
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Pershing transactions files (Pershing → Standard)."""
        return {
            'bank': 'Bank',
            'client': 'Client',
            'account': 'Account', 
            'cusip': 'Cusip',
            'transaction_type': 'Activity Description',
            'date': 'Settlement Date',        # Convert to MM/DD/YYYY
            'price': 'Price',                 # Already European format
            'quantity': 'Quantity',           # Already European format
            'amount': 'Net Amount'            # Already European format
        }
    
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
            
            # DEBUG: Log the conversion process
            logger.debug(f"🔍 AMERICAN→EUROPEAN CONVERSION:")
            logger.debug(f"  Input: {repr(value)} (type: {type(value)})")
            logger.debug(f"  After str().strip(): '{str_value}'")
            
            # Remove common formatting characters except commas and periods
            str_value = str_value.replace('$', '')  # Remove dollar signs
            str_value = str_value.replace('%', '')  # Remove percentage signs
            str_value = str_value.replace('(', '-').replace(')', '')  # Handle negative parentheses
            
            logger.debug(f"  After removing $/%/(): '{str_value}'")
            
            # Handle the conversion: American (7,802.30) → European (7.802,30)
            if ',' in str_value and '.' in str_value:
                # Both comma and period present - typical American format
                # Split by period to get decimal part
                parts = str_value.split('.')
                if len(parts) == 2:
                    integer_part = parts[0].replace(',', '.')  # Replace commas with periods
                    decimal_part = parts[1]
                    european_format = f"{integer_part},{decimal_part}"
                    logger.debug(f"  Both comma & period: '{str_value}' → '{european_format}'")
                    return european_format
            elif ',' in str_value and '.' not in str_value:
                # Only comma present - could be thousands separator
                # Convert comma to period for thousands
                european_format = str_value.replace(',', '.')
                logger.debug(f"  Only comma: '{str_value}' → '{european_format}'")
                return european_format
            elif '.' in str_value and ',' not in str_value:
                # Only period present - convert to comma for decimal
                european_format = str_value.replace('.', ',')
                logger.debug(f"  Only period: '{str_value}' → '{european_format}'")
                return european_format
            else:
                # No special formatting needed
                logger.debug(f"  No special formatting: '{str_value}' → '{str_value}'")
                return str_value
                
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to European number format")
            return None
    
    def convert_total_cost_conditionally(self, total_cost, asset_classification) -> Optional[str]:
        """
        Convert Total Cost conditionally based on Asset Classification.
        Cash, Money Funds and Bank Deposits are already in European format.
        All others need conversion from American to European format.
        
        Args:
            total_cost: Original Total Cost value
            asset_classification: Asset Classification to determine conversion need
            
        Returns:
            Total Cost in European format
        """
        if pd.isna(total_cost) or total_cost == '' or total_cost is None:
            return None
            
        # Cash, Money Funds and Bank Deposits already in European format
        if asset_classification == 'Cash, Money Funds and Bank Deposits':
            logger.debug(f"Keeping Total Cost as-is for Cash/Money Market: {total_cost}")
            return str(total_cost) if total_cost is not None else None
        else:
            # Convert from American to European format
            converted = self.convert_american_to_european_number(total_cost)
            logger.debug(f"Converted Total Cost: {total_cost} → {converted}")
            return converted
    
    def extract_coupon_from_description(self, description: str) -> Optional[float]:
        """
        Extract coupon rate from bond description.
        
        Args:
            description: Bond description containing percentage (e.g., "METLIFE INC 6.350% 03/15/55 ...")
            
        Returns:
            Coupon rate as float, or None if not found
        """
        if pd.isna(description) or not description:
            return None
        
        try:
            # Regex to find percentage values in description
            # Pattern: digits with optional decimal point followed by %
            pattern = r'(\d+\.?\d*)\%'
            match = re.search(pattern, str(description))
            
            if match:
                coupon_str = match.group(1)
                coupon_rate = float(coupon_str)
                logger.debug(f"🎯 EXTRACTED COUPON: '{description}' → {coupon_rate}%")
                return coupon_rate
            else:
                logger.debug(f"⚠️ NO COUPON FOUND: '{description}'")
                return None
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Error extracting coupon from description '{description}': {str(e)}")
            return None

    def calculate_coupon_rate(self, description: str, sub_asset_classification: str) -> Optional[float]:
        """
        Calculate coupon rate for bonds by extracting from description.
        Only for Corporate Bonds, Sovereign Debt, and U.S. Treasury Securities.
        
        Args:
            description: Bond description containing percentage
            sub_asset_classification: Sub-Asset Classification
            
        Returns:
            Coupon rate as percentage, or None if not applicable
        """
        # Check if this is a bond type that should have coupon rate
        bond_types = ['Corporate Bonds', 'Sovereign Debt', 'U.S. Treasury Securities']
        if sub_asset_classification not in bond_types:
            return None
        
        # Extract coupon rate from description
        logger.debug(f"🔍 COUPON EXTRACTION DEBUG:")
        logger.debug(f"  Sub-Asset Classification: {sub_asset_classification}")
        logger.debug(f"  Description: {repr(description)}")
        
        coupon_rate = self.extract_coupon_from_description(description)
        
        if coupon_rate is not None:
            logger.debug(f"  ✅ Extracted coupon rate: {coupon_rate}%")
        else:
            logger.debug(f"  ❌ Could not extract coupon rate")
            
        return coupon_rate
    
    def reclassify_pershing_asset_type(self, asset_type: str) -> str:
        """
        Reclassify Pershing asset type to standard format.
        
        Args:
            asset_type: Original Pershing asset type
            
        Returns:
            Reclassified asset type
        """
        if pd.isna(asset_type):
            return asset_type
        
        asset_type_str = str(asset_type).strip()
        
        # Pershing Asset Type Mapping
        mapping = {
            'Fixed Income': 'Fixed Income',
            'Equity': 'Equity',
            'Cash, Money Funds and Bank Deposits': 'Money Market',
            'Investment Funds': 'Equity'
        }
        
        result = mapping.get(asset_type_str, asset_type_str)
        if result != asset_type_str:
            logger.debug(f"Reclassified asset type: '{asset_type_str}' → '{result}'")
        
        return result
    
    def convert_pershing_bond_price(self, price, sub_asset_classification) -> Optional[str]:
        """
        Convert Pershing bond prices with special logic for bond assets.
        
        Logic:
        - For Corporate Bonds, Sovereign Debt, U.S. Treasury Securities: 
          Remove commas/periods, then:
          - If starts with "1": place comma after 1 (12345 → 1,2345)
          - If starts with any other number: place comma before (89765 → 0,89765)
        - For other assets: convert American → European if needed
        
        Args:
            price: Original price value
            sub_asset_classification: Sub-Asset Classification to determine if bond pricing needed
            
        Returns:
            Converted price as string with European decimal format
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        bond_types = ['Corporate Bonds', 'Sovereign Debt', 'U.S. Treasury Securities']
        
        if sub_asset_classification in bond_types:
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
            # Non-bond: just convert American → European if needed
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
        Transform Pershing securities to standard format.
        
        Args:
            securities_file: Path to Pershing securities Excel file
            
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
        
        # Validate required Pershing columns exist
        pershing_required = list(column_map.values())
        missing_cols = [col for col in pershing_required if col not in df.columns]
        if missing_cols:
            logger.error(f"Pershing securities file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"Pershing securities file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns + supporting columns
        logger.info("Identifying required columns...")
        required_pershing_columns = list(column_map.values())
        # Add supporting columns needed for calculations
        supporting_columns = ['Sub-Asset Classification', 'Estimated Annual Income']
        all_columns = required_pershing_columns + supporting_columns
        existing_columns = [col for col in all_columns if col in df.columns]
        
        logger.info(f"Keeping {len(existing_columns)} required columns: {existing_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[existing_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename Pershing columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {pershing_col: std_col for std_col, pershing_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        logger.info("=== STEP 2: ADD MISSING COLUMNS ===")
        
        # Add missing standard columns with None values
        df_renamed['coupon_rate'] = None
        
        # Handle maturity_date: preserve if exists, otherwise set to None
        if 'maturity_date' not in df_renamed.columns:
            df_renamed['maturity_date'] = None
            logger.info("Added missing column: maturity_date (set to None)")
        else:
            maturity_count = df_renamed['maturity_date'].notna().sum()
            logger.info(f"Preserved existing maturity_date column with {maturity_count} values")
        
        logger.info("Added missing column: coupon_rate (set to None)")
        
        logger.info("=== STEP 3: NUMBER FORMAT CONVERSIONS ===")
        
        # Step 3a: Convert Estimated Annual Income to European format (needed for coupon calculation)
        logger.info("Converting Estimated Annual Income from American to European format...")
        if 'Estimated Annual Income' in df_renamed.columns:
            df_renamed['estimated_annual_income_eur'] = df_renamed['Estimated Annual Income'].apply(
                self.convert_american_to_european_number
            )
            logger.info("Converted Estimated Annual Income to European format")
        
        # Step 3b: Convert Total Cost conditionally based on Asset Classification
        logger.info("Converting Total Cost conditionally based on Asset Classification...")
        df_renamed['cost_basis'] = df_renamed.apply(
            lambda row: self.convert_total_cost_conditionally(
                row['cost_basis'], row['asset_type']
            ), axis=1
        )
        logger.info("Converted Total Cost conditionally")
        
        logger.info("=== STEP 4: COUPON RATE CALCULATION ===")
        
        # Calculate coupon rate for bonds by extracting from description
        logger.info("Calculating coupon rate for bond securities...")
        df_renamed['coupon_rate'] = df_renamed.apply(
            lambda row: self.calculate_coupon_rate(
                row['name'],  # Bond description containing percentage
                row['Sub-Asset Classification']
            ), axis=1
        )
        
        # Count how many got coupon rates
        coupon_count = df_renamed['coupon_rate'].notna().sum()
        logger.info(f"Calculated coupon rates for {coupon_count} bond securities")
        
        logger.info("=== STEP 5: ASSET TYPE RECLASSIFICATION ===")
        
        # Reclassify asset types (using original column before dropping it)
        logger.info("Reclassifying asset types...")
        df_renamed['asset_type'] = df_renamed['asset_type'].apply(self.reclassify_pershing_asset_type)
        
        logger.info("=== STEP 6: BOND PRICE HANDLING ===")
        
        # Handle bond pricing for price column
        logger.info("Converting prices with Pershing bond pricing logic...")
        df_renamed['price'] = df_renamed.apply(
            lambda row: self.convert_pershing_bond_price(
                row['price'], row['Sub-Asset Classification']
            ), axis=1
        )
        
        logger.info("=== STEP 7: TEXT CLEANING ===")
        
        # Clean text columns
        text_columns = ['name', 'ticker', 'cusip']
        for col in text_columns:
            if col in df_renamed.columns:
                logger.info(f"Cleaning text column: {col}")
                df_renamed[col] = df_renamed[col].apply(self.clean_text_value)
        
        logger.info("=== STEP 8: CLEAN UP INTERMEDIATE COLUMNS ===")
        
        # Drop intermediate columns that are no longer needed
        intermediate_cols = ['Sub-Asset Classification', 'Estimated Annual Income', 'estimated_annual_income_eur']
        for col in intermediate_cols:
            if col in df_renamed.columns:
                df_renamed = df_renamed.drop(columns=[col])
                logger.info(f"Dropped intermediate column: {col}")
        
        logger.info("=== STEP 9: FINAL COLUMN ORDERING ===")
        
        # Reorder columns to match target format
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
        Transform Pershing transactions to standard format.
        
        Args:
            transactions_file: Path to Pershing transactions Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file
        logger.info(f"Loading transactions file: {transactions_file}")
        df = pd.read_excel(transactions_file)
        logger.info(f"Loaded {len(df)} transaction records with {len(df.columns)} columns")
        
        # Get column mappings
        column_map = self.get_transactions_column_mappings()
        
        # Validate required Pershing columns exist
        pershing_required = list(column_map.values())
        missing_cols = [col for col in pershing_required if col not in df.columns]
        if missing_cols:
            logger.error(f"Pershing transactions file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"Pershing transactions file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_pershing_columns = list(column_map.values())
        logger.info(f"Keeping {len(required_pershing_columns)} required columns: {required_pershing_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[required_pershing_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename Pershing columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {pershing_col: std_col for std_col, pershing_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        logger.info("=== STEP 2: DATE FORMAT CONVERSION ===")
        
        # Convert date columns to MM/DD/YYYY format
        date_columns = ['date']
        for col in date_columns:
            if col in df_renamed.columns:
                logger.info(f"Converting date column: {col}")
                df_renamed[col] = df_renamed[col].apply(self.clean_date_value)
        
        logger.info("=== STEP 3: TEXT CLEANING ===")
        
        # Clean text columns
        text_columns = ['transaction_type', 'cusip']
        for col in text_columns:
            if col in df_renamed.columns:
                logger.info(f"Cleaning text column: {col}")
                df_renamed[col] = df_renamed[col].apply(self.clean_text_value)
        
        logger.info("=== STEP 4: FINAL COLUMN ORDERING ===")
        
        # Reorder columns to match target format
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
        Process Pershing files and save to output directory.
        
        Args:
            input_dir: Directory containing combined Pershing files
            output_dir: Directory to save processed files
            date_str: Date string for output filenames (DD_MM_YYYY format)
            
        Returns:
            Tuple of (securities_processed, transactions_processed)
        """
        logger.info(f"Processing Pershing files from {input_dir}")
        
        # Expected filenames
        securities_file = os.path.join(input_dir, f"Pershing_securities_{date_str}.xlsx")
        transactions_file = os.path.join(input_dir, f"Pershing_transactions_{date_str}.xlsx")
        
        securities_processed = False
        transactions_processed = False
        
        # Process securities file
        if os.path.exists(securities_file):
            try:
                logger.info(f"Processing Pershing securities: {securities_file}")
                securities_df = self.transform_securities(securities_file)
                
                # Save processed securities
                output_securities = os.path.join(output_dir, f"securities_{date_str}.xlsx")
                securities_df.to_excel(output_securities, index=False)
                logger.info(f"✅ Pershing securities saved: {output_securities}")
                logger.info(f"📊 Securities: {len(securities_df)} rows, {len(securities_df.columns)} columns")
                securities_processed = True
                
            except Exception as e:
                logger.error(f"❌ Error processing Pershing securities: {str(e)}")
                raise
        else:
            logger.warning(f"⚠️ Pershing securities file not found: {securities_file}")
        
        # Process transactions file
        if os.path.exists(transactions_file):
            try:
                logger.info(f"Processing Pershing transactions: {transactions_file}")
                transactions_df = self.transform_transactions(transactions_file)
                
                # Save processed transactions
                output_transactions = os.path.join(output_dir, f"transactions_{date_str}.xlsx")
                transactions_df.to_excel(output_transactions, index=False)
                logger.info(f"✅ Pershing transactions saved: {output_transactions}")
                logger.info(f"📊 Transactions: {len(transactions_df)} rows, {len(transactions_df.columns)} columns")
                transactions_processed = True
                
            except Exception as e:
                logger.error(f"❌ Error processing Pershing transactions: {str(e)}")
                raise
        else:
            logger.warning(f"⚠️ Pershing transactions file not found: {transactions_file}")
        
        return securities_processed, transactions_processed 