"""IDB Excel file transformer for preprocessing raw bank files."""

import pandas as pd
import logging
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
import sys
sys.path.insert(0, str(project_root))

from portfolio.preprocessing.utils.openfigi_client import OpenFIGIClient
from portfolio.preprocessing.bank_detector import BankDetector

logger = logging.getLogger(__name__)

class IDBTransformer:
    """Transformer for IDB Excel files with API integration for asset type detection."""
    
    def __init__(self, api_key: str = None):
        """
        Initialize IDB transformer.
        
        Args:
            api_key: OpenFIGI API key for asset type detection
        """
        self.bank_code = 'IDB'
        self.api_key = api_key or os.getenv('OPENFIGI_API_KEY')
        self.openfigi_client = OpenFIGIClient(self.api_key)
        logger.info(f"Initialized {self.bank_code} transformer with API integration")
        
        # Enhanced asset type mappings based on OpenFIGI API responses
        self.api_asset_type_mapping = {
            # OpenFIGI types that map to Fixed Income (with bond pricing logic)
            'US GOVERNMENT': 'Fixed Income',
            'GLOBAL': 'Fixed Income', 
            'DOMESTIC MTN': 'Fixed Income',
            'Corporate Bond': 'Fixed Income',
            'Government Bond': 'Fixed Income', 
            'Treasury Bill': 'Fixed Income',
            'Treasury Note': 'Fixed Income',
            'Treasury Bond': 'Fixed Income',
            'Municipal Bond': 'Fixed Income',
            
            # OpenFIGI types that map to Equity (standard pricing logic)
            'ETP': 'Equity',    # Exchange Traded Products
            'Equity': 'Equity',
            'Common Stock': 'Equity',
            'Preferred Stock': 'Equity',
            'ETF': 'Equity',
            'REIT': 'Equity'
        }
        
        # IDB-specific hardcoded mappings (highest priority)
        self.idb_specific_mappings = {
            'CAJA': 'Cash',
            'SPDR GOLD SHARES': 'Alternatives',
            'GOLD SHARES': 'Alternatives',
            'GOLD ETF': 'Alternatives',
            'GOLD FUND': 'Alternatives'
        }
        
        # Commodity ETF detection keywords
        self.commodity_keywords = [
            'GOLD', 'SILVER', 'OIL', 'COMMODITY', 'PRECIOUS METAL', 
            'PLATINUM', 'PALLADIUM', 'COPPER', 'NATURAL GAS', 'ENERGY'
        ]
        
        # Fallback asset type patterns for when API fails
        self.fallback_asset_patterns = {
            # Fixed Income patterns
            'TREASURY': 'Fixed Income',
            'TREAS': 'Fixed Income', 
            'BOND': 'Fixed Income',
            'NOTE': 'Fixed Income',
            'BILL': 'Fixed Income',
            'GOVERNMENT': 'Fixed Income',
            
            # Alternatives patterns  
            'GOLD': 'Alternatives',
            'COMMODITY': 'Alternatives',
            'REAL ESTATE': 'Alternatives',
            'REIT': 'Alternatives',
            
            # Equity patterns
            'STOCK': 'Equity',
            'EQUITY': 'Equity',
            'ETF': 'Equity',
            'FUND': 'Equity',
            
            # Cash patterns
            'CASH': 'Cash',
            'MONEY MARKET': 'Cash'
        }
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for IDB securities files (IDB → Standard)."""
        return {
            # System columns (extracted from filename)
            'bank': 'bank',
            'client': 'client', 
            'account': 'account',
            
            # Direct mappings
            'name': 'Name',
            'quantity': 'Quantity',
            'coupon_rate': 'Rate',
            'cusip': 'CUSIP',
            'ticker': 'Ticker',
            'cost_basis': 'Original Cost',
            'market_value': 'Market Value',
            'price': 'Price',
            'maturity_date': 'Maturity',
            
            # Calculated/API fields
            'asset_type': None  # Via OpenFIGI API using CUSIP + fallback patterns
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for IDB transactions files (IDB → Standard)."""
        return {
            # System columns (extracted from filename)
            'bank': 'bank',
            'client': 'client',
            'account': 'account',
            
            # Direct mappings
            'date': 'Fecha',        # Convert DD-MM-YY → DD/MM/20YY
            'transaction_type': 'Description',
            'cusip': 'CUSIP',
            'price': 'Unit cost',
            'amount': 'Amount',
            'quantity': 'Quantity'
        }
    
    def convert_american_to_european_number(self, value) -> Optional[str]:
        """Convert American number format to European format."""
        if pd.isna(value) or value == '' or value is None:
            return None
        
        try:
            # Convert to string and clean
            value_str = str(value).strip()
            
            # Handle negative numbers
            is_negative = value_str.startswith('-')
            if is_negative:
                value_str = value_str[1:]
            
            # Remove commas and convert to float
            clean_value = value_str.replace(',', '')
            float_value = float(clean_value)
            
            # Format with European notation (comma as decimal separator)
            european_str = f"{float_value:.2f}".replace('.', ',')
            
            # Add negative sign back if needed
            if is_negative:
                european_str = f"-{european_str}"
            
            return european_str
            
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to European number format")
            return None
    
    def convert_idb_date_securities(self, date_value) -> Optional[str]:
        """
        Convert IDB securities date to MM/DD/YYYY format.
        Handles multiple input formats: DD-MM-YYYY, YYYY-MM-DD HH:MM:SS, etc.
        
        Args:
            date_value: Date in various formats
            
        Returns:
            Date in MM/DD/YYYY format or None if conversion fails
        """
        if pd.isna(date_value) or date_value == '' or date_value is None:
            return None
        
        try:
            date_str = str(date_value).strip()
            
            # Handle YYYY-MM-DD HH:MM:SS format (pandas datetime)
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                # Extract just the date part
                date_part = date_str.split(' ')[0]
                year, month, day = date_part.split('-')
                return f"{month}/{day}/{year}"
            
            # Handle DD-MM-YYYY format
            elif re.match(r'\d{2}-\d{2}-\d{4}', date_str):
                day, month, year = date_str.split('-')
                return f"{month}/{day}/{year}"
            
            logger.warning(f"Unexpected date format in securities: {date_value}")
            return None
            
        except Exception as e:
            logger.warning(f"Could not convert securities date '{date_value}': {e}")
            return None
    
    def convert_idb_date_transactions(self, date_value) -> Optional[str]:
        """
        Convert IDB transactions date to DD/MM/YYYY format.
        Handles multiple input formats: DD-MM-YY, YYYY-MM-DD HH:MM:SS, etc.
        
        Args:
            date_value: Date in various formats
            
        Returns:
            Date in DD/MM/YYYY format or None if conversion fails
        """
        if pd.isna(date_value) or date_value == '' or date_value is None:
            return None
        
        try:
            date_str = str(date_value).strip()
            
            # Handle YYYY-MM-DD HH:MM:SS format (pandas datetime)
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                # Extract just the date part
                date_part = date_str.split(' ')[0]
                year, month, day = date_part.split('-')
                return f"{day}/{month}/{year}"
            
            # Handle DD-MM-YY format
            elif re.match(r'\d{2}-\d{2}-\d{2}', date_str):
                day, month, year = date_str.split('-')
                # Add 20 prefix to year
                full_year = f"20{year}"
                return f"{day}/{month}/{full_year}"
            
            logger.warning(f"Unexpected date format in transactions: {date_value}")
            return None
            
        except Exception as e:
            logger.warning(f"Could not convert transactions date '{date_value}': {e}")
            return None
    
    def apply_bond_price_logic(self, price_str, is_bond):
        """
        Apply IDB-specific bond price formatting based on API asset type detection.
        - If bond and starts with 1: comma after first digit (12345 → 1,2345)  
        - If bond and other: comma before (89765 → 0,89765)
        - If not bond: standard European formatting
        """
        if pd.isna(price_str):
            return None
            
        if not is_bond:
            return self.convert_american_to_european_number(price_str)
        
        try:
            # Bond-specific logic
            clean_number = str(price_str).replace(',', '').replace('.', '').replace(' ', '')
            
            if clean_number.startswith('1'):
                # Place comma after first digit: 12345 → 1,2345
                if len(clean_number) > 1:
                    return f"1,{clean_number[1:]}"
                else:
                    return "1"
            else:
                # Place comma before: 89765 → 0,89765  
                return f"0,{clean_number}"
                
        except Exception as e:
            logger.warning(f"Could not apply bond price logic to '{price_str}': {e}")
            return self.convert_american_to_european_number(price_str)
    
    def detect_asset_type_idb_enhanced(self, cusip: str, name: str, api_data: dict = None) -> str:
        """
        Enhanced asset type detection for IDB with layered approach.
        
        Priority order:
        1. IDB-specific hardcoded mappings (highest priority)
        2. Enhanced API analysis for commodity ETFs
        3. Existing API mapping
        4. Fallback pattern matching (lowest priority)
        
        Args:
            cusip: CUSIP identifier
            name: Asset name/description
            api_data: Pre-fetched API data (optional)
            
        Returns:
            Asset type classification
        """
        # Layer 1: IDB-specific hardcoded mappings (highest priority)
        if name:
            name_upper = str(name).upper().strip()
            for pattern, asset_type in self.idb_specific_mappings.items():
                if pattern in name_upper:
                    logger.info(f"IDB-specific mapping: '{pattern}' → {asset_type} for asset: {name}")
                    return asset_type
        
        # Layer 2: Enhanced API analysis for commodity ETFs
        if api_data and 'error' not in api_data:
            # Check if this is a commodity ETP
            if api_data.get('security_type') == 'ETP' and name:
                name_upper = str(name).upper()
                for keyword in self.commodity_keywords:
                    if keyword in name_upper:
                        logger.info(f"Commodity ETP detected: '{keyword}' in '{name}' → Alternatives")
                        return 'Alternatives'
            
            # Layer 3: Existing API mapping
            api_security_type = api_data.get('security_type') or api_data.get('security_type2')
            if api_security_type:
                mapped_asset_type = self.api_asset_type_mapping.get(api_security_type, api_security_type)
                logger.info(f"API detected asset type for CUSIP {cusip}: {api_security_type} → {mapped_asset_type}")
                return mapped_asset_type
        
        # Layer 4: Fallback pattern matching (lowest priority)
        fallback_result = self.detect_asset_type_fallback(name)
        logger.info(f"Using fallback classification for '{name}' → {fallback_result}")
        return fallback_result
    
    def detect_asset_type_from_api(self, cusip: str) -> Optional[str]:
        """
        Detect asset type using OpenFIGI API.
        
        Args:
            cusip: CUSIP identifier
            
        Returns:
            Asset type or None if API lookup fails
        """
        if not cusip or pd.isna(cusip):
            return None
        
        try:
            api_data = self.openfigi_client.lookup_by_cusip(str(cusip).strip())
            
            if api_data and 'error' not in api_data:
                api_security_type = api_data.get('security_type') or api_data.get('security_type2')
                if api_security_type:
                    mapped_asset_type = self.api_asset_type_mapping.get(api_security_type, api_security_type)
                    logger.info(f"API detected asset type for CUSIP {cusip}: {api_security_type} → {mapped_asset_type}")
                    return mapped_asset_type
            
            logger.warning(f"API lookup failed for CUSIP {cusip}")
            return None
            
        except Exception as e:
            logger.warning(f"Error in API lookup for CUSIP {cusip}: {e}")
            return None
    
    def detect_asset_type_fallback(self, name: str) -> str:
        """
        Detect asset type using fallback patterns when API fails.
        
        Args:
            name: Asset name/description
            
        Returns:
            Asset type based on name patterns
        """
        if not name or pd.isna(name):
            return 'Alternatives'  # Default fallback
        
        name_upper = str(name).upper()
        
        # Apply fallback patterns
        for pattern, asset_type in self.fallback_asset_patterns.items():
            if pattern in name_upper:
                logger.info(f"Fallback pattern matched '{pattern}' → {asset_type} for asset: {name}")
                return asset_type
        
        logger.info(f"No fallback pattern matched for asset: {name}, defaulting to Alternatives")
        return 'Alternatives'  # Default when no patterns match
    
    def detect_bond_from_asset_type(self, asset_type: str) -> bool:
        """
        Determine if an asset is a bond based on asset type.
        
        Args:
            asset_type: Asset type (from API or fallback)
            
        Returns:
            True if asset is a bond, False otherwise
        """
        return asset_type == 'Fixed Income'
    
    def extract_bank_client_account_from_filename(self, filename: str) -> Tuple[str, str, str]:
        """
        Extract bank, client, account from IDB filename.
        
        Args:
            filename: IDB filename (e.g., 'IDB_JC_Datim_securities_DD_MM_YYYY.xlsx')
            
        Returns:
            Tuple of (bank, client, account)
        """
        extraction = BankDetector.extract_client_account_from_filename(filename)
        if extraction:
            return extraction
        
        # Fallback for IDB-specific pattern
        logger.warning(f"Using fallback extraction for IDB file: {filename}")
        return ('IDB', 'JC', 'Datim')
    
    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform IDB securities file to standard format.
        
        Args:
            securities_file: Path to IDB securities Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info(f"🔄 Transforming IDB securities file: {securities_file}")
        
        try:
            # Extract bank, client, account from filename
            filename = Path(securities_file).name
            bank, client, account = self.extract_bank_client_account_from_filename(filename)
            logger.info(f"Extracted from filename: bank={bank}, client={client}, account={account}")
            
            # Read the securities file
            df = pd.read_excel(securities_file)
            logger.info(f"📊 Loaded {len(df)} securities records with {len(df.columns)} columns")
            logger.info(f"📋 Columns: {list(df.columns)}")
            
            # Get column mappings
            column_map = self.get_securities_column_mappings()
            
            # Validate required columns exist (exclude system columns that we add)
            system_columns = ['bank', 'client', 'account']
            required_idb_columns = [col for col in column_map.values() if col is not None and col not in system_columns]
            missing_cols = [col for col in required_idb_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"IDB securities file missing required columns: {missing_cols}")
                logger.info(f"Available columns: {list(df.columns)}")
                raise ValueError(f"IDB securities file missing required columns: {missing_cols}")
            
            # Step 1: Filter and rename columns
            logger.info("=== STEP 1: COLUMN FILTERING AND RENAMING ===")
            df_filtered = df[required_idb_columns].copy()
            
            # Rename columns to standard format
            reverse_map = {idb_col: std_col for std_col, idb_col in column_map.items() if idb_col is not None}
            df_renamed = df_filtered.rename(columns=reverse_map)
            logger.info(f"Renamed columns: {list(df_renamed.columns)}")
            
            # Step 2: Add system columns
            logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
            df_renamed['bank'] = bank
            df_renamed['client'] = client
            df_renamed['account'] = account
            logger.info(f"Added system columns: bank={bank}, client={client}, account={account}")
            
            # Step 3: Date conversion for maturity dates
            logger.info("=== STEP 3: DATE CONVERSION ===")
            if 'maturity_date' in df_renamed.columns:
                logger.info("Converting maturity dates from DD-MM-YYYY to MM/DD/YYYY format")
                df_renamed['maturity_date'] = df_renamed['maturity_date'].apply(self.convert_idb_date_securities)
            
            # Step 4: Number format conversions
            logger.info("=== STEP 4: NUMBER FORMAT CONVERSIONS ===")
            
            # Convert basic numeric columns from American to European format
            basic_numeric_columns = ['quantity', 'market_value', 'cost_basis', 'coupon_rate']
            for col in basic_numeric_columns:
                if col in df_renamed.columns:
                    logger.info(f"Converting {col} from American to European format")
                    df_renamed[col] = df_renamed[col].apply(self.convert_american_to_european_number)
            
            # Step 5: Asset type detection and bond price handling
            logger.info("=== STEP 5: ASSET TYPE DETECTION AND BOND PRICE HANDLING ===")
            
            # Extract unique CUSIPs for batch API processing
            unique_cusips = df_renamed['cusip'].dropna().unique().tolist()
            total_cusips = len(unique_cusips)
            
            if total_cusips > 0:
                logger.info(f"Found {total_cusips} unique CUSIPs for API enrichment")
                
                # Perform batch lookup
                api_results = self.openfigi_client.batch_lookup(unique_cusips, 'cusip')
                
                # Calculate success rate
                successful_lookups = sum(1 for result in api_results.values() if result is not None and 'error' not in result)
                success_rate = (successful_lookups / total_cusips * 100) if total_cusips > 0 else 0
                
                logger.info(f"API enrichment results: {successful_lookups}/{total_cusips} successful ({success_rate:.1f}%)")
            else:
                logger.warning("No CUSIPs found for API enrichment")
                api_results = {}
            
            # Apply asset type detection and bond price handling
            asset_type_list = []
            price_list = []
            bond_count = 0
            api_success_count = 0
            fallback_count = 0
            
            for idx, row in df_renamed.iterrows():
                cusip = row.get('cusip')
                name = row.get('name', '')
                price = row.get('price')
                
                # Enhanced asset type detection using layered approach
                api_data = None
                if cusip and not pd.isna(cusip):
                    api_data = api_results.get(cusip)
                    if api_data and 'error' not in api_data:
                        api_success_count += 1
                    else:
                        fallback_count += 1
                else:
                    fallback_count += 1
                
                # Use enhanced detection method
                asset_type = self.detect_asset_type_idb_enhanced(cusip, name, api_data)
                asset_type_list.append(asset_type)
                
                # Bond price handling
                is_bond = self.detect_bond_from_asset_type(asset_type)
                if is_bond:
                    bond_count += 1
                
                converted_price = self.apply_bond_price_logic(price, is_bond)
                price_list.append(converted_price)
            
            df_renamed['asset_type'] = asset_type_list
            df_renamed['price'] = price_list
            
            logger.info(f"  ✅ Asset type detection: {api_success_count} via API, {fallback_count} via fallback")
            logger.info(f"  ✅ Bond price handling: {bond_count} bonds detected and processed")
            
            # Step 6: Final column setup and cleanup
            logger.info("=== STEP 6: FINAL COLUMN SETUP ===")
            
            # Ensure all required output columns exist
            output_columns = [
                'bank', 'client', 'account', 'asset_type', 'name', 'ticker', 'cusip',
                'quantity', 'price', 'market_value', 'cost_basis', 'maturity_date', 'coupon_rate'
            ]
            
            for col in output_columns:
                if col not in df_renamed.columns:
                    df_renamed[col] = None
                    logger.warning(f"Added missing column: {col}")
            
            # Reorder columns
            df_final = df_renamed[output_columns]
            
            logger.info(f"✅ IDB securities transformation completed successfully!")
            logger.info(f"📊 Output: {len(df_final)} records with {len(df_final.columns)} columns")
            
            # Show sample of transformed data
            logger.info("📋 Sample of transformed data:")
            for i, row in df_final.head(3).iterrows():
                logger.info(f"  Row {i}: {row['name'][:50]}... | {row['asset_type']} | {row['quantity']} | {row['price']}")
            
            return df_final
            
        except Exception as e:
            logger.error(f"❌ Error transforming IDB securities: {e}")
            raise
    
    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """
        Transform IDB transactions file to standard format.
        
        Args:
            transactions_file: Path to IDB transactions Excel file
            
        Returns:
            Transformed DataFrame in standard format
        """
        logger.info(f"🔄 Transforming IDB transactions file: {transactions_file}")
        
        try:
            # Extract bank, client, account from filename
            filename = Path(transactions_file).name
            bank, client, account = self.extract_bank_client_account_from_filename(filename)
            logger.info(f"Extracted from filename: bank={bank}, client={client}, account={account}")
            
            # Read the transactions file
            df = pd.read_excel(transactions_file)
            logger.info(f"📊 Loaded {len(df)} transaction records with {len(df.columns)} columns")
            logger.info(f"📋 Columns: {list(df.columns)}")
            
            # Get column mappings
            column_map = self.get_transactions_column_mappings()
            
            # Validate required columns exist (exclude system columns that we add)
            system_columns = ['bank', 'client', 'account']
            required_idb_columns = [col for col in column_map.values() if col is not None and col not in system_columns]
            missing_cols = [col for col in required_idb_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"IDB transactions file missing required columns: {missing_cols}")
                logger.info(f"Available columns: {list(df.columns)}")
                raise ValueError(f"IDB transactions file missing required columns: {missing_cols}")
            
            # Step 1: Filter and rename columns
            logger.info("=== STEP 1: COLUMN FILTERING AND RENAMING ===")
            df_filtered = df[required_idb_columns].copy()
            
            # Rename columns to standard format
            reverse_map = {idb_col: std_col for std_col, idb_col in column_map.items() if idb_col is not None}
            df_renamed = df_filtered.rename(columns=reverse_map)
            logger.info(f"Renamed columns: {list(df_renamed.columns)}")
            
            # Step 2: Add system columns
            logger.info("=== STEP 2: ADD SYSTEM COLUMNS ===")
            df_renamed['bank'] = bank
            df_renamed['client'] = client
            df_renamed['account'] = account
            logger.info(f"Added system columns: bank={bank}, client={client}, account={account}")
            
            # Step 3: Date conversion
            logger.info("=== STEP 3: DATE CONVERSION ===")
            if 'date' in df_renamed.columns:
                logger.info("Converting dates from DD-MM-YY to DD/MM/20YY format")
                df_renamed['date'] = df_renamed['date'].apply(self.convert_idb_date_transactions)
            
            # Step 4: Number format conversions
            logger.info("=== STEP 4: NUMBER FORMAT CONVERSIONS ===")
            
            # Convert numeric columns from American to European format
            numeric_columns = ['price', 'quantity', 'amount']
            for col in numeric_columns:
                if col in df_renamed.columns:
                    logger.info(f"Converting {col} from American to European format")
                    df_renamed[col] = df_renamed[col].apply(self.convert_american_to_european_number)
            
            # Step 5: Final column setup and cleanup
            logger.info("=== STEP 5: FINAL COLUMN SETUP ===")
            
            # Ensure all required output columns exist
            output_columns = [
                'bank', 'client', 'account', 'date', 'transaction_type', 'amount',
                'cusip', 'price', 'quantity'
            ]
            
            for col in output_columns:
                if col not in df_renamed.columns:
                    df_renamed[col] = None
                    logger.warning(f"Added missing column: {col}")
            
            # Reorder columns
            df_final = df_renamed[output_columns]
            
            logger.info(f"✅ IDB transactions transformation completed successfully!")
            logger.info(f"📊 Output: {len(df_final)} records with {len(df_final.columns)} columns")
            
            # Show sample of transformed data
            logger.info("📋 Sample of transformed data:")
            for i, row in df_final.head(3).iterrows():
                trans_desc = str(row['transaction_type'])[:50] + "..." if isinstance(row['transaction_type'], str) and len(str(row['transaction_type'])) > 50 else row['transaction_type']
                logger.info(f"  Row {i}: {row['date']} | {trans_desc} | {row['amount']}")
            
            return df_final
            
        except Exception as e:
            logger.error(f"❌ Error transforming IDB transactions: {e}")
            raise 