"""HSBC Excel file transformer for preprocessing combined bank files.
Inherits from PershingTransformer and overrides HSBC-specific logic."""

import pandas as pd
import logging
import re
import os
from typing import Dict, Optional, Tuple
from .pershing_transformer import PershingTransformer

logger = logging.getLogger(__name__)

class HSBCTransformer(PershingTransformer):
    """Transformer for HSBC Excel files, inheriting from PershingTransformer."""
    
    def __init__(self):
        super().__init__()
        self.bank_code = 'HSBC'
        logger.info(f"Initialized {self.bank_code} transformer (inheriting from Pershing)")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for HSBC securities files (HSBC → Standard).
        
        Key differences from Pershing:
        - bank/client/account are lowercase in HSBC
        - Same structure otherwise
        """
        return {
            # System columns (already lowercase in HSBC)
            'bank': 'bank',
            'client': 'client', 
            'account': 'account',
            # Standard HSBC-specific columns
            'cusip': 'CUSIP',
            'name': 'Description',
            'asset_type': 'Asset Classification',  # Will be reclassified
            'quantity': 'Quantity',                # Already European format
            'price': 'Price',                      # Bond-specific logic applied
            'market_value': 'Market Value',        # No conversion needed
            'cost_basis': 'Total Cost',            # American → European conversion
            'ticker': 'Security ID',
            'maturity_date': 'maturity_date'       # Use extracted maturity dates from enricher
            # coupon_rate will be calculated from description
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for HSBC transactions files (HSBC → Standard).
        
        Key differences from Pershing:
        - bank/client/account are lowercase in HSBC
        - Same structure otherwise
        """
        return {
            'bank': 'bank',
            'client': 'client',
            'account': 'account',
            'cusip': 'Cusip',
            'transaction_type': 'Activity Description',
            'date': 'Settlement Date',        # Convert to MM/DD/YYYY
            'price': 'Price',                 # Already European format
            'quantity': 'Quantity',           # Already European format
            'amount': 'Net Amount'            # Already European format
        }
    
    def reclassify_hsbc_asset_type(self, asset_type: str, description: str) -> str:
        """
        Reclassify HSBC asset type to standard format.
        
        HSBC Logic:
        - Unclassified + "HSBC US TREASURY LIQUIDITY CLASS B" → Money Market
        - Unclassified + (other) → Equity
        - Fixed Income → Fixed Income
        - Other → Equity
        - Alternatives → Alternatives
        
        Args:
            asset_type: Original HSBC asset type
            description: Security description for special cases
            
        Returns:
            Reclassified asset type
        """
        if pd.isna(asset_type):
            return asset_type
        
        asset_type_str = str(asset_type).strip()
        description_str = str(description).strip()
        
        # HSBC Asset Type Mapping
        if asset_type_str == 'Unclassified':
            if 'HSBC US TREASURY LIQUIDITY CLASS B' in description_str:
                result = 'Money Market'
                logger.debug(f"HSBC special case: Unclassified + Treasury → Money Market")
            else:
                result = 'Equity'
                logger.debug(f"HSBC reclassification: Unclassified → Equity")
        elif asset_type_str == 'Fixed Income':
            result = 'Fixed Income'
        elif asset_type_str == 'Other':
            result = 'Equity'
            logger.debug(f"HSBC reclassification: Other → Equity")
        elif asset_type_str == 'Alternatives':
            result = 'Alternatives'
        else:
            result = asset_type_str  # Keep original if unknown
            logger.debug(f"HSBC unknown asset type, keeping original: {asset_type_str}")
        
        if result != asset_type_str:
            logger.debug(f"HSBC asset reclassification: '{asset_type_str}' → '{result}'")
        
        return result
    
    def is_hsbc_bond(self, description: str) -> bool:
        """
        Detect HSBC bonds by checking last 15 characters for MM/DD/YY date pattern.
        
        Logic: If Description last 15 characters contain date in format MM/DD/YY, 
        then the asset is a bond and needs special price handling.
        
        Args:
            description: Security description
            
        Returns:
            True if bond detected, False otherwise
        """
        if pd.isna(description):
            return False
        
        desc_str = str(description).strip()
        last_15 = desc_str[-15:] if len(desc_str) >= 15 else desc_str
        date_pattern = r'\d{1,2}/\d{1,2}/\d{2}'
        is_bond = bool(re.search(date_pattern, last_15))
        
        if is_bond:
            logger.debug(f"HSBC bond detected: '{desc_str[:50]}...' (last 15: '{last_15}')")
        
        return is_bond
    
    def convert_hsbc_bond_price(self, price, description) -> Optional[str]:
        """
        Convert HSBC bond prices with special logic.
        
        Logic for bonds (detected via description date pattern):
        - Remove all commas (already in European format)
        - If starts with "1": place comma after 1 (123456 → 1,23456)
        - If starts with other number: place comma before (89765 → 0,89765)
        
        For non-bonds: keep as-is (already European format)
        
        Args:
            price: Original price value
            description: Security description for bond detection
            
        Returns:
            Converted price as string with European decimal format
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        if self.is_hsbc_bond(description):
            logger.debug(f"Applying HSBC bond pricing logic to: {price}")
            
            # Convert to string and get the numeric value
            price_str = str(price).strip()
            
            if price_str.startswith('1'):
                # 104.85 → 1,0485
                result = '1,' + price_str[1:].replace('.', '')
                logger.debug(f"HSBC bond price (starts with 1): {price} → {result}")
                return result
            else:
                # 99.12 → 0,9912
                result = '0,' + price_str.replace('.', '')
                logger.debug(f"HSBC bond price (other number): {price} → {result}")
                return result
        else:
            # Non-bond: already in European format, return as-is (don't convert to string)
            logger.debug(f"HSBC non-bond price (keep as-is): {price} → {price}")
            return price
    
    def convert_hsbc_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert numbers for HSBC with specific logic.
        
        HSBC Number Conversion Rules:
        - Quantity: NO conversion (already European format)
        - Price: Apply bond-specific logic only
        - Market Value: NO conversion (already correct)
        - Total Cost: American → European conversion
        
        Args:
            df: DataFrame with HSBC data
            
        Returns:
            DataFrame with converted numbers
        """
        logger.info("Converting HSBC numbers with specific rules...")
        
        # Total Cost: American → European conversion
        logger.info("Converting Total Cost from American to European format...")
        df['cost_basis'] = df['cost_basis'].apply(self.convert_american_to_european_number)
        
        # Price: Apply HSBC bond logic (price is already in European format, just modify bond prices)
        logger.info("Applying HSBC bond price logic...")
        df['price'] = df.apply(
            lambda row: self.convert_hsbc_bond_price(row['price'], row['name']), 
            axis=1
        )
        
        # Quantity and Market Value: NO conversion (already correct)
        logger.info("Quantity and Market Value: keeping as-is (already European format)")
        
        return df
    
    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform HSBC securities to standard format.
        Overrides parent method to use HSBC-specific logic.
        
        Args:
            securities_file: Path to HSBC securities Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== HSBC SECURITIES TRANSFORMATION ===")
        logger.info("=== STEP 1: COLUMN MANAGEMENT ===")
        
        # Load the Excel file
        logger.info(f"Loading HSBC securities file: {securities_file}")
        df = pd.read_excel(securities_file)
        logger.info(f"Loaded {len(df)} securities records with {len(df.columns)} columns")
        
        # Get HSBC column mappings
        column_map = self.get_securities_column_mappings()
        
        # Validate required HSBC columns exist
        hsbc_required = list(column_map.values())
        missing_cols = [col for col in hsbc_required if col not in df.columns]
        if missing_cols:
            logger.error(f"HSBC securities file missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df.columns)}")
            raise ValueError(f"HSBC securities file missing required columns: {missing_cols}")
        
        # Step 1a: Identify and keep only required columns
        logger.info("Identifying required columns...")
        required_hsbc_columns = list(column_map.values())
        existing_columns = [col for col in required_hsbc_columns if col in df.columns]
        
        logger.info(f"Keeping {len(existing_columns)} required columns: {existing_columns}")
        
        # Step 1b: Delete unused columns
        df_filtered = df[existing_columns].copy()
        logger.info(f"Filtered dataset: {len(df_filtered)} rows, {len(df_filtered.columns)} columns")
        
        # Step 1c: Rename HSBC columns to standard names
        logger.info("Renaming columns to standard format...")
        reverse_map = {hsbc_col: std_col for std_col, hsbc_col in column_map.items()}
        df_renamed = df_filtered.rename(columns=reverse_map)
        logger.info(f"Renamed columns: {list(df_renamed.columns)}")
        
        logger.info("=== STEP 2: ADD MISSING COLUMNS ===")
        
        # Add missing standard columns with None values
        if 'coupon_rate' not in df_renamed.columns:
            df_renamed['coupon_rate'] = None
            logger.info("Added missing coupon_rate column (set to None)")
        
        # Handle maturity_date - preserve if extracted by enricher, otherwise set to None
        if 'maturity_date' not in df_renamed.columns:
            df_renamed['maturity_date'] = None
            logger.info("Added missing maturity_date column (set to None)")
        else:
            # Maturity dates were extracted by enricher - log the results
            maturity_count = df_renamed['maturity_date'].notna().sum()
            total_rows = len(df_renamed)
            logger.info(f"Using extracted maturity dates from enricher: {maturity_count}/{total_rows} securities have maturity dates")
            
            if maturity_count > 0:
                # Show sample of extracted maturity dates
                sample_dates = df_renamed[df_renamed['maturity_date'].notna()]['maturity_date'].head(3).tolist()
                logger.info(f"Sample maturity dates: {sample_dates}")
        
        logger.info("Column initialization complete")
        
        logger.info("=== STEP 3: HSBC-SPECIFIC NUMBER CONVERSIONS ===")
        
        # Apply HSBC-specific number conversions
        df_renamed = self.convert_hsbc_numbers(df_renamed)
        
        logger.info("=== STEP 4: COUPON RATE CALCULATION ===")
        
        # Calculate coupon rate for bonds by extracting from description
        # Note: Using HSBC bond detection since HSBC doesn't have Sub-Asset Classification patterns
        logger.info("Calculating coupon rate for HSBC bond securities...")
        def calculate_hsbc_coupon_rate(row):
            # Check if this is a bond using HSBC detection
            if self.is_hsbc_bond(row['name']):
                # Extract coupon rate directly from description (same logic as Pershing)
                description = row['name']
                coupon_rate = self.extract_coupon_from_description(description)
                
                if coupon_rate is not None:
                    logger.debug(f"🎯 HSBC EXTRACTED COUPON: '{description}' → {coupon_rate}%")
                else:
                    logger.debug(f"⚠️ HSBC NO COUPON FOUND: '{description}'")
                    
                return coupon_rate
            else:
                return None
        
        df_renamed['coupon_rate'] = df_renamed.apply(calculate_hsbc_coupon_rate, axis=1)
        coupon_rate_count = df_renamed['coupon_rate'].notna().sum()
        logger.info(f"Calculated coupon rate for {coupon_rate_count} bond securities")
        
        logger.info("=== STEP 5: HSBC ASSET TYPE RECLASSIFICATION ===")
        
        # Reclassify HSBC asset types using description
        logger.info("Reclassifying HSBC asset types...")
        df_renamed['asset_type'] = df_renamed.apply(
            lambda row: self.reclassify_hsbc_asset_type(row['asset_type'], row['name']),
            axis=1
        )
        
        # Log asset type distribution
        asset_distribution = df_renamed['asset_type'].value_counts()
        logger.info(f"Asset type distribution after reclassification: {dict(asset_distribution)}")
        
        logger.info("=== STEP 6: TEXT CLEANING ===")
        
        # Clean text values
        text_columns = ['name', 'cusip', 'ticker']
        for col in text_columns:
            if col in df_renamed.columns:
                df_renamed[col] = df_renamed[col].apply(self.clean_text_value)
        logger.info(f"Cleaned text columns: {text_columns}")
        
        logger.info("=== STEP 7: COUPON RATE - KEEPING AS NUMERIC ===")
        
        # Keep coupon rate as numeric float (like quantity and market_value)
        logger.info("Keeping coupon rate as numeric float values (consistent with quantity/market_value)")
        coupon_numeric_count = df_renamed['coupon_rate'].notna().sum()
        logger.info(f"Coupon rate values remain as numeric floats: {coupon_numeric_count} bonds")
        
        logger.info("=== STEP 8: FINAL COLUMN ORDERING ===")
        
        # Define final column order for securities
        final_columns = [
            'bank', 'client', 'account', 'asset_type', 'name', 'cusip', 'ticker',
            'quantity', 'price', 'maturity_date', 'market_value', 'cost_basis', 'coupon_rate'
        ]
        
        # Keep only final columns that exist
        existing_final_columns = [col for col in final_columns if col in df_renamed.columns]
        df_final = df_renamed[existing_final_columns].copy()
        
        logger.info(f"Final dataset: {len(df_final)} rows, {len(df_final.columns)} columns")
        logger.info(f"Final columns: {list(df_final.columns)}")
        
        logger.info("=== HSBC SECURITIES TRANSFORMATION COMPLETE ===")
        
        return df_final
    
    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """
        Transform HSBC transactions to standard format.
        Uses parent method since transactions logic is identical.
        
        Args:
            transactions_file: Path to HSBC transactions Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info("=== HSBC TRANSACTIONS TRANSFORMATION ===")
        logger.info("Using inherited transactions transformation logic...")
        
        # Use parent method with HSBC-specific column mappings
        return super().transform_transactions(transactions_file)
    
    def process_files(self, input_dir: str, output_dir: str, date_str: str) -> Tuple[bool, bool]:
        """
        Process HSBC files and save to output directory.
        Overrides parent method to use HSBC file patterns instead of Pershing.
        
        Args:
            input_dir: Directory containing HSBC files
            output_dir: Directory to save processed files
            date_str: Date string for output filenames (DD_MM_YYYY format)
            
        Returns:
            Tuple of (securities_processed, transactions_processed)
        """
        logger.info(f"Processing HSBC files from {input_dir}")
        
        # Expected HSBC filenames
        securities_file = os.path.join(input_dir, f"HSBC_securities_{date_str}.xlsx")
        transactions_file = os.path.join(input_dir, f"HSBC_transactions_{date_str}.xlsx")
        
        securities_processed = False
        transactions_processed = False
        
        # Process securities file
        if os.path.exists(securities_file):
            try:
                logger.info(f"Processing HSBC securities: {securities_file}")
                securities_df = self.transform_securities(securities_file)
                
                # Save processed securities - follow same pattern as other preprocessors
                output_securities = os.path.join(output_dir, f"securities_{date_str}.xlsx")
                securities_df.to_excel(output_securities, index=False)
                logger.info(f"✅ HSBC securities saved: {output_securities}")
                logger.info(f"📊 Securities: {len(securities_df)} rows, {len(securities_df.columns)} columns")
                securities_processed = True
                
            except Exception as e:
                logger.error(f"❌ Error processing HSBC securities: {str(e)}")
                raise
        else:
            logger.warning(f"⚠️ HSBC securities file not found: {securities_file}")
        
        # Process transactions file
        if os.path.exists(transactions_file):
            try:
                logger.info(f"Processing HSBC transactions: {transactions_file}")
                transactions_df = self.transform_transactions(transactions_file)
                
                # Save processed transactions - follow same pattern as other preprocessors
                output_transactions = os.path.join(output_dir, f"transactions_{date_str}.xlsx")
                transactions_df.to_excel(output_transactions, index=False)
                logger.info(f"✅ HSBC transactions saved: {output_transactions}")
                logger.info(f"📊 Transactions: {len(transactions_df)} rows, {len(transactions_df.columns)} columns")
                transactions_processed = True
                
            except Exception as e:
                logger.error(f"❌ Error processing HSBC transactions: {str(e)}")
                raise
        else:
            logger.warning(f"⚠️ HSBC transactions file not found: {transactions_file}")
        
        return securities_processed, transactions_processed 