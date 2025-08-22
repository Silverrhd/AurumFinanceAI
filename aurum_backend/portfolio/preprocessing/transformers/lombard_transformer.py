"""Lombard Excel file transformer for preprocessing raw bank files."""

import pandas as pd
import logging
import os
import re
from typing import Dict, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class LombardTransformer:
    """Transformer for Lombard Excel files following step-by-step approach."""
    
    def __init__(self):
        self.bank_code = 'LO'
        logger.info(f"Initialized {self.bank_code} transformer")
    
    def get_securities_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Lombard securities files (LO → Standard)."""
        return {
            # Standard Column → Lombard Column (from actual file)
            'bank': 'Bank',
            'client': 'Client', 
            'account': 'Account',
            'quantity': 'Quantity',                        # European format as-is
            'name': 'Description',                         # As-is
            'price': 'Last Price (QC)',                    # Needs bond price handling
            'market_value': 'Valuation\n(VC, End)',        # Fixed: Added newline character
            'cusip': 'ISIN',                              # As-is
            'cost_basis': 'Total Purchase Cost (VC, UR)', # European format as-is
            'asset_type': 'Asset Class Code',             # Needs classification mapping
            'ticker': None,                               # Empty/NaN for now
            'coupon_rate': None,                          # Extract from Description
            'maturity_date': None                         # Extract from Description
        }
    
    def get_transactions_column_mappings(self) -> Dict[str, str]:
        """Get column mappings for Lombard transactions files (LO → Standard)."""
        return {
            # Standard Column → Lombard Column (from actual file)
            'bank': 'Bank',
            'client': 'Client',
            'account': 'Account',
            'amount': 'Amount',                           # European format as-is
            'transaction_type': 'Transaction',            # As-is
            'date': 'Accounting date',                   # YYYY-MM-DD 00:00:00 → DD/MM/YYYY
            'price': 'Price',                            # European format as-is
            'quantity': 'Quantity',                      # European format as-is
            'cusip': 'Position'                          # Extract ISIN from position description
        }
    
    def reclassify_lombard_asset_type(self, asset_class_code: str) -> str:
        """
        Reclassify Lombard asset type to standard format.
        
        Args:
            asset_class_code: Original Lombard asset class code
            
        Returns:
            Reclassified asset type
        """
        if pd.isna(asset_class_code):
            return asset_class_code
        
        asset_type_str = str(asset_class_code).strip()
        
        # Lombard Asset Class Code Mapping
        lombard_mapping = {
            'Cash': 'Cash',
            'Currency Forward': 'Money Market',
            'Short-Term Instruments': 'Money Market',
            'Fixed Income': 'Fixed Income',
            'Equities': 'Equity',
            'Structured Products': 'Alternatives',
            'Hedge Funds': 'Alternatives',
            'Gold and other Commodities': 'Alternatives',
            'Other Investments': 'Alternatives'
        }
        
        return lombard_mapping.get(asset_type_str, asset_type_str)
    
    def is_bond_asset(self, asset_type: str, description: str) -> bool:
        """
        Determine if an asset is a bond based on Lombard-specific criteria.
        
        Bond Detection Criteria (ALL must be true):
        1. Asset Type: Must be "Fixed Income" OR "Structured Products"
        2. Description Pattern: Contains fraction (1 7/8, 2 1/4) OR percentage (2.3%, 4.3%, 2%)
        3. Maturity Date: Contains date in DD.MM.YYYY format
        
        Args:
            asset_type: Original asset type from file
            description: Asset description/name
            
        Returns:
            True if asset is a bond, False otherwise
        """
        if pd.isna(asset_type) or pd.isna(description):
            return False
        
        asset_type_str = str(asset_type).strip()
        description_str = str(description).strip()
        
        # Criterion 1: Asset type check
        if asset_type_str == "Structured Products":
            # Structured Products always get bond price handling
            return True
        elif asset_type_str != "Fixed Income":
            # Must be Fixed Income for other cases
            return False
        
        # Criterion 2: Check for fraction or percentage pattern
        # Fraction patterns: "1 7/8", "2 1/4", "1/4"
        fraction_pattern = r'\b(\d+\s+\d+/\d+|\d+/\d+)\b'
        # Percentage patterns: "5.375%", "3.5%", "5.9%" - anchored at start, flexible spacing
        percentage_pattern = r'^(\d+(?:\.\d+)?)%'
        
        has_fraction = bool(re.search(fraction_pattern, description_str))
        has_percentage = bool(re.search(percentage_pattern, description_str))
        
        if not (has_fraction or has_percentage):
            return False
        
        # Criterion 3: Check for maturity date in DD.MM.YYYY format
        date_pattern = r'\b\d{2}\.\d{2}\.\d{4}\b'
        has_maturity_date = bool(re.search(date_pattern, description_str))
        
        return has_maturity_date
    
    def convert_lombard_bond_price(self, price) -> Optional[str]:
        """
        Convert Lombard bond prices with European decimal format.
        
        Bond Price Handling Logic:
        1. Remove all commas and periods from price
        2. If starts with "1": Place comma after first digit (123456 → 1,23456)
        3. If starts with other digit: Place comma before (987654 → 0,987654)
        4. Return European decimal format
        
        Args:
            price: Original price value (European format already)
            
        Returns:
            Converted price as string with European decimal format
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        try:
            # Convert to string and remove all commas and periods
            price_str = str(price).replace(',', '').replace('.', '').strip()
            
            if not price_str:
                return None
            
            # Apply bond price logic
            if price_str.startswith('1'):
                # Starts with 1: place comma after first digit
                return f"1,{price_str[1:]}"
            else:
                # Starts with other number: place comma before
                return f"0,{price_str}"
                
        except Exception as e:
            logger.warning(f"Could not convert bond price '{price}': {e}")
            return None
    
    def extract_coupon_rate(self, description: str) -> Optional[str]:
        """
        Extract coupon rate from Lombard description field.
        
        Handles:
        - Fraction Format: "1 7/8" → Convert to percentage (1 + 7/8 = 1.875 → "1,875")
        - Percentage Format: "2.3%" → Extract number, convert to European ("2,3")
        
        Args:
            description: Asset description containing coupon information
            
        Returns:
            Coupon rate in European decimal format without % symbol
        """
        if pd.isna(description):
            return None
        
        description_str = str(description).strip()
        
        # Try to extract fraction first: "1 7/8", "2 1/4", "1/4"
        fraction_pattern = r'\b(\d+)\s+(\d+)/(\d+)\b'  # "1 7/8"
        simple_fraction_pattern = r'\b(\d+)/(\d+)\b'    # "1/4"
        
        fraction_match = re.search(fraction_pattern, description_str)
        if fraction_match:
            whole_part = int(fraction_match.group(1))
            numerator = int(fraction_match.group(2))
            denominator = int(fraction_match.group(3))
            
            # Convert to decimal: whole + (numerator/denominator)
            decimal_value = whole_part + (numerator / denominator)
            # Convert to European format
            european_format = str(decimal_value).replace('.', ',')
            return european_format
        
        # Try simple fraction: "1/4"
        simple_fraction_match = re.search(simple_fraction_pattern, description_str)
        if simple_fraction_match:
            numerator = int(simple_fraction_match.group(1))
            denominator = int(simple_fraction_match.group(2))
            
            # Convert to decimal
            decimal_value = numerator / denominator
            # Convert to European format
            european_format = str(decimal_value).replace('.', ',')
            return european_format
        
        # Try to extract percentage: "5.375%", "3.5%", "5.9%" - at start of description
        percentage_pattern = r'^(\d+(?:\.\d+)?)%'
        percentage_match = re.search(percentage_pattern, description_str)
        if percentage_match:
            percentage_value = percentage_match.group(1)
            # Convert to European format (replace . with ,)
            european_format = percentage_value.replace('.', ',')
            return european_format
        
        return None
    
    def extract_maturity_date(self, description: str) -> Optional[str]:
        """
        Extract maturity date from Lombard description field.
        
        Conversion: DD.MM.YYYY → DD/MM/YYYY (replace periods with slashes)
        
        Args:
            description: Asset description containing maturity date
            
        Returns:
            Maturity date in DD/MM/YYYY format
        """
        if pd.isna(description):
            return None
        
        description_str = str(description).strip()
        
        # Pattern: DD.MM.YYYY
        date_pattern = r'\b(\d{2})\.(\d{2})\.(\d{4})\b'
        date_match = re.search(date_pattern, description_str)
        
        if date_match:
            day = date_match.group(1)
            month = date_match.group(2)
            year = date_match.group(3)
            
            # Convert to DD/MM/YYYY format
            return f"{day}/{month}/{year}"
        
        return None
    
    def convert_transaction_date(self, date_value) -> Optional[str]:
        """
        Convert Lombard transaction date format.
        
        Input Format: "2025-05-27 00:00:00" or "2025-05-27"
        Output Format: "27/05/2025"
        
        Args:
            date_value: Original date value
            
        Returns:
            Date in DD/MM/YYYY format
        """
        if pd.isna(date_value):
            return None
        
        try:
            date_str = str(date_value).strip()
            
            # Extract YYYY-MM-DD portion (remove time if present)
            if ' ' in date_str:
                date_str = date_str.split(' ')[0]
            
            # Parse YYYY-MM-DD format
            if '-' in date_str:
                parts = date_str.split('-')
                if len(parts) == 3:
                    year, month, day = parts
                    return f"{day}/{month}/{year}"
            
            logger.warning(f"Could not parse date format: {date_value}")
            return None
            
        except Exception as e:
            logger.warning(f"Error converting date '{date_value}': {e}")
            return None
    
    def extract_cusip_from_position(self, position_value) -> Optional[str]:
        """
        Extract CUSIP/ISIN from Lombard Position column.
        
        Args:
            position_value: Position description containing ISIN
            
        Returns:
            Extracted ISIN/CUSIP or None if not found
        """
        if pd.isna(position_value):
            return None
        
        position_str = str(position_value).strip()
        
        # ISIN pattern: 12-character alphanumeric (2 letters + 10 alphanumeric)
        isin_pattern = r'\b[A-Z]{2}[A-Z0-9]{10}\b'
        isin_match = re.search(isin_pattern, position_str)
        
        if isin_match:
            return isin_match.group(0)
        
        # Fallback: look for any 12-character alphanumeric sequence
        fallback_pattern = r'\b[A-Z0-9]{12}\b'
        fallback_match = re.search(fallback_pattern, position_str)
        
        if fallback_match:
            return fallback_match.group(0)
        
        logger.warning(f"Could not extract CUSIP from position: {position_value}")
        return None
    
    def create_position_to_cusip_mapping(self, securities_file: str) -> Dict[str, str]:
        """
        Create Position → ISIN mapping by cross-referencing with securities file.
        
        Args:
            securities_file: Path to securities file for cross-reference
            
        Returns:
            Dictionary mapping Position descriptions to ISIN codes
        """
        try:
            from difflib import SequenceMatcher
            
            # Load securities data
            securities_df = pd.read_excel(securities_file)
            logger.info(f"📋 Loading securities for CUSIP cross-reference: {len(securities_df)} records")
            
            # Create Description → ISIN mapping from securities
            securities_mapping = {}
            for _, row in securities_df.iterrows():
                if pd.notna(row.get('Description')) and pd.notna(row.get('ISIN')):
                    description = str(row['Description']).strip()
                    isin = str(row['ISIN']).strip()
                    securities_mapping[description] = isin
            
            logger.info(f"📋 Created securities mapping: {len(securities_mapping)} Description→ISIN pairs")
            return securities_mapping
            
        except Exception as e:
            logger.error(f"❌ Error creating securities mapping: {e}")
            return {}
    
    def normalize_description(self, description: str) -> str:
        """
        Normalize security description for better matching.
        
        Handles:
        - Removes maturity dates in both formats (DD.MM.YYYY and MMM.YY)
        - Standardizes spaces
        - Removes special characters
        
        Args:
            description: Original description
            
        Returns:
            Normalized description
        """
        if pd.isna(description):
            return ""
            
        desc = str(description).strip()
        
        # Remove full date format (DD.MM.YYYY)
        desc = re.sub(r'\b\d{2}\.\d{2}\.\d{4}\b', '', desc)
        
        # Remove short date format (FEB35, MAR24, etc)
        desc = re.sub(r'\b[A-Z]{3}\d{2}\b', '', desc)
        
        # Standardize spaces (multiple spaces to single space)
        desc = re.sub(r'\s+', ' ', desc)
        
        # Remove special characters but keep % and numbers
        desc = re.sub(r'[^a-zA-Z0-9%\s]', '', desc)
        
        return desc.strip()

    def find_matching_cusip(self, position: str, securities_mapping: Dict[str, str], similarity_threshold: float = 0.6) -> Optional[str]:
        """
        Find matching CUSIP for a transaction position using fuzzy matching.
        
        Args:
            position: Position description from transaction
            securities_mapping: Dictionary of Description → ISIN from securities
            similarity_threshold: Minimum similarity for fuzzy matching (default: 0.6)
            
        Returns:
            Matching ISIN or None if no match found
        """
        if pd.isna(position):
            return None
        
        from difflib import SequenceMatcher
        
        # Normalize the position description
        position_normalized = self.normalize_description(position)
        
        # Strategy 1: Exact substring matching on normalized descriptions
        for description, isin in securities_mapping.items():
            desc_normalized = self.normalize_description(description)
            if position_normalized.lower() in desc_normalized.lower():
                logger.debug(f"🔍 Exact substring match: '{position_normalized}' → '{desc_normalized}' → {isin}")
                return isin
        
        # Strategy 2: Fuzzy similarity matching on normalized descriptions
        best_match = None
        best_similarity = 0.0
        
        for description, isin in securities_mapping.items():
            desc_normalized = self.normalize_description(description)
            similarity = SequenceMatcher(None, position_normalized.lower(), desc_normalized.lower()).ratio()
            
            # Log similarity scores for debugging
            logger.debug(f"Similarity {similarity:.2f}: '{position_normalized}' vs '{desc_normalized}'")
            
            if similarity > best_similarity and similarity >= similarity_threshold:
                best_similarity = similarity
                best_match = (description, isin)
        
        if best_match:
            description, isin = best_match
            logger.debug(f"🔍 Fuzzy match ({best_similarity:.2f}): '{position_normalized}' → '{self.normalize_description(description)}' → {isin}")
            return isin
        
        # No match found
        logger.warning(f"⚠️ Could not match Position '{position_normalized}' to any securities (threshold: {similarity_threshold})")
        return None

    def load_processed_securities_for_cusip_lookup(self, securities_file: str) -> pd.DataFrame:
        """
        Load processed securities file for CUSIP lookup.
        
        Args:
            securities_file: Path to raw securities file (used to determine date)
            
        Returns:
            DataFrame with processed LO securities containing name and cusip columns
        """
        try:
            # Extract date from securities file path to find processed securities file
            # securities_file format: /path/to/LO_securities_DD_MM_YYYY.xlsx
            import re
            date_match = re.search(r'(\d{2}_\d{2}_\d{4})', securities_file)
            if not date_match:
                raise ValueError(f"Could not extract date from securities file path: {securities_file}")
            
            date_str = date_match.group(1)
            
            # Construct path to processed securities file
            # Assuming it's in data/excel/securities_DD_MM_YYYY.xlsx
            # securities_file is like: /path/to/data/excel/input_files/LO_securities_DD_MM_YYYY.xlsx
            # We want: /path/to/data/excel/securities_DD_MM_YYYY.xlsx
            base_dir = os.path.dirname(os.path.dirname(securities_file))  # Go up from input_files to excel
            processed_securities_path = os.path.join(base_dir, f'securities_{date_str}.xlsx')
            
            if not os.path.exists(processed_securities_path):
                raise FileNotFoundError(f"Processed securities file not found: {processed_securities_path}")
            
            # Load processed securities file
            df = pd.read_excel(processed_securities_path)
            
            # Filter for LO bank only and securities with valid CUSIPs
            lo_securities = df[
                (df['bank'] == 'LO') & 
                (df['cusip'].notna()) & 
                (df['cusip'] != '') & 
                (df['name'].notna()) & 
                (df['name'] != '')
            ].copy()
            
            logger.info(f"📋 Loaded {len(lo_securities)} LO securities with valid CUSIPs from {processed_securities_path}")
            
            return lo_securities[['name', 'cusip']]
            
        except Exception as e:
            logger.error(f"❌ Error loading processed securities file: {e}")
            return pd.DataFrame(columns=['name', 'cusip'])

    def match_position_to_cusip(self, position: str, processed_securities_df: pd.DataFrame) -> Optional[str]:
        """
        Match transaction position to CUSIP using processed securities data.
        
        Args:
            position: Position description from transaction
            processed_securities_df: DataFrame with name and cusip columns
            
        Returns:
            Matched CUSIP or None if no match found
        """
        if pd.isna(position) or processed_securities_df.empty:
            return None
        
        position_normalized = self.normalize_description(position)
        
        # First try exact substring matching
        for _, row in processed_securities_df.iterrows():
            name = row['name']
            cusip = row['cusip']
            
            if pd.isna(name) or pd.isna(cusip):
                continue
                
            name_normalized = self.normalize_description(name)
            
            # Check if position is contained in name (position is usually shorter)
            if position_normalized in name_normalized:
                logger.debug(f"Substring match found: '{position}' → '{name}' → {cusip}")
                return cusip
        
        # Fallback to fuzzy matching with lower threshold for processed securities
        from difflib import SequenceMatcher
        best_match = None
        best_score = 0.0
        similarity_threshold = 0.6
        
        for _, row in processed_securities_df.iterrows():
            name = row['name']
            cusip = row['cusip']
            
            if pd.isna(name) or pd.isna(cusip):
                continue
                
            name_normalized = self.normalize_description(name)
            similarity = SequenceMatcher(None, position_normalized, name_normalized).ratio()
            
            if similarity > best_score and similarity >= similarity_threshold:
                best_score = similarity
                best_match = cusip
                logger.debug(f"Fuzzy match: '{position}' → '{name}' (score: {similarity:.3f}) → {cusip}")
        
        if best_match:
            logger.debug(f"Best match selected: '{position}' → {best_match} (score: {best_score:.3f})")
        else:
            logger.debug(f"No match found for: '{position}' (threshold: {similarity_threshold})")
        
        return best_match
    
    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform Lombard securities file to standard format.
        
        Args:
            securities_file: Path to Lombard securities Excel file
            
        Returns:
            Transformed securities DataFrame
        """
        logger.info(f"🔄 Transforming Lombard securities file: {securities_file}")
        
        try:
            # Load securities data
            df = pd.read_excel(securities_file)
            logger.info(f"📊 Loaded {len(df)} securities records")
            
            # Get column mappings
            column_mappings = self.get_securities_column_mappings()
            
            # Initialize result DataFrame with standard columns
            result_columns = ['bank', 'client', 'account', 'quantity', 'name', 'price', 
                            'market_value', 'cusip', 'cost_basis', 'asset_type', 
                            'ticker', 'coupon_rate', 'maturity_date']
            
            result_df = pd.DataFrame(columns=result_columns)
            
            # Step 1: Basic column mappings (direct transfers)
            logger.info("🔄 Step 1: Applying basic column mappings...")
            for target_col, source_col in column_mappings.items():
                if source_col and source_col in df.columns:
                    result_df[target_col] = df[source_col]
                    logger.debug(f"  Mapped '{source_col}' → '{target_col}'")
            
            # Step 2: Asset type reclassification
            logger.info("🔄 Step 2: Reclassifying asset types...")
            result_df['asset_type'] = result_df['asset_type'].apply(self.reclassify_lombard_asset_type)
            
            # Step 3: Bond detection and advanced processing
            logger.info("🔄 Step 3: Processing bond assets...")
            bond_count = 0
            coupon_extracted = 0
            maturity_extracted = 0
            bond_price_converted = 0
            
            for idx, row in result_df.iterrows():
                is_bond = self.is_bond_asset(df.loc[idx, 'Asset Class Code'], 
                                           df.loc[idx, 'Description'])
                
                if is_bond:
                    bond_count += 1
                    
                    # Extract coupon rate
                    coupon_rate = self.extract_coupon_rate(df.loc[idx, 'Description'])
                    if coupon_rate:
                        result_df.loc[idx, 'coupon_rate'] = coupon_rate
                        coupon_extracted += 1
                    
                    # Extract maturity date
                    maturity_date = self.extract_maturity_date(df.loc[idx, 'Description'])
                    if maturity_date:
                        result_df.loc[idx, 'maturity_date'] = maturity_date
                        maturity_extracted += 1
                    
                    # Convert bond price
                    bond_price = self.convert_lombard_bond_price(df.loc[idx, 'Last Price (QC)'])
                    if bond_price:
                        result_df.loc[idx, 'price'] = bond_price
                        bond_price_converted += 1
            
            logger.info(f"  📈 Bond processing: {bond_count} bonds detected")
            logger.info(f"  📈 Coupon rates extracted: {coupon_extracted}")
            logger.info(f"  📈 Maturity dates extracted: {maturity_extracted}")
            logger.info(f"  📈 Bond prices converted: {bond_price_converted}")
            
            # Step 4: Set empty ticker column
            result_df['ticker'] = None
            
            # Step 5: Filter out negative quantities
            logger.info("🔄 Step 5: Filtering negative quantities...")
            initial_count = len(result_df)
            
            # Convert quantity to numeric for filtering
            result_df['quantity'] = pd.to_numeric(result_df['quantity'], errors='coerce')
            
            # Filter out negative quantities
            result_df = result_df[result_df['quantity'] >= 0].copy()
            
            filtered_count = initial_count - len(result_df)
            if filtered_count > 0:
                logger.info(f"  📉 Filtered out {filtered_count} assets with negative quantities")
            
            # Step 6: Final data cleaning and validation
            logger.info("🔄 Step 6: Final data validation...")
            
            # Ensure all required columns are present
            for col in result_columns:
                if col not in result_df.columns:
                    result_df[col] = None
            
            # Reorder columns to match standard format
            result_df = result_df[result_columns]
            
            logger.info(f"✅ Securities transformation completed: {len(result_df)} records")
            return result_df
            
        except Exception as e:
            logger.error(f"❌ Error transforming securities file: {e}")
            raise
    
    def transform_transactions(self, transactions_file: str, securities_file: str = None, securities_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Transform Lombard transactions file to standard format.
        
        Args:
            transactions_file: Path to Lombard transactions Excel file
            
        Returns:
            Transformed transactions DataFrame
        """
        logger.info(f"🔄 Transforming Lombard transactions file: {transactions_file}")
        
        try:
            # Load transactions data
            df = pd.read_excel(transactions_file)
            logger.info(f"📊 Loaded {len(df)} transaction records")
            
            # Get column mappings
            column_mappings = self.get_transactions_column_mappings()
            
            # Initialize result DataFrame with standard columns
            result_columns = ['bank', 'client', 'account', 'amount', 'transaction_type', 
                            'date', 'price', 'quantity', 'cusip']
            
            result_df = pd.DataFrame(columns=result_columns)
            
            # Step 1: Basic column mappings (direct transfers)
            logger.info("🔄 Step 1: Applying basic column mappings...")
            for target_col, source_col in column_mappings.items():
                if source_col and source_col in df.columns:
                    if target_col != 'cusip' and target_col != 'date':  # Handle these separately
                        result_df[target_col] = df[source_col]
                        logger.debug(f"  Mapped '{source_col}' → '{target_col}'")
            
            # Step 2: Date conversion
            logger.info("🔄 Step 2: Converting transaction dates...")
            date_converted = 0
            for idx, row in df.iterrows():
                converted_date = self.convert_transaction_date(row['Accounting date'])
                if converted_date:
                    result_df.loc[idx, 'date'] = converted_date
                    date_converted += 1
            
            logger.info(f"  📅 Dates converted: {date_converted}/{len(df)}")
            
            # Step 3: CUSIP extraction via processed securities lookup (NEW APPROACH)
            logger.info("🔄 Step 3: Extracting CUSIPs via processed securities lookup...")
            cusip_extracted = 0
            
            processed_securities_df = pd.DataFrame()
            
            # Try to use provided securities DataFrame first (for multi-bank processing)
            if securities_df is not None and not securities_df.empty:
                logger.info("📋 Using provided securities DataFrame for CUSIP lookup")
                # Filter for LO securities with valid CUSIPs
                processed_securities_df = securities_df[
                    (securities_df['cusip'].notna()) & 
                    (securities_df['cusip'] != '') & 
                    (securities_df['name'].notna()) & 
                    (securities_df['name'] != '')
                ][['name', 'cusip']].copy()
                logger.info(f"📋 Filtered to {len(processed_securities_df)} securities with valid CUSIPs")
            
            # Fallback to loading from processed securities file (for single-bank processing)
            elif securities_file and os.path.exists(securities_file):
                logger.info("📋 Loading processed securities file for CUSIP lookup")
                processed_securities_df = self.load_processed_securities_for_cusip_lookup(securities_file)
            
            if not processed_securities_df.empty:
                # Match each transaction position to processed securities
                for idx, row in df.iterrows():
                    position = row.get('Position')
                    if pd.notna(position):
                        matched_cusip = self.match_position_to_cusip(position, processed_securities_df)
                        if matched_cusip:
                            result_df.loc[idx, 'cusip'] = matched_cusip
                            cusip_extracted += 1
            else:
                logger.warning("⚠️ No processed securities available - CUSIP extraction skipped")
            
            logger.info(f"  🔍 CUSIPs extracted: {cusip_extracted}/{len(df)}")
            
            # Step 4: Final data cleaning and validation
            logger.info("🔄 Step 4: Final data validation...")
            
            # Ensure all required columns are present
            for col in result_columns:
                if col not in result_df.columns:
                    result_df[col] = None
            
            # Reorder columns to match standard format
            result_df = result_df[result_columns]
            
            logger.info(f"✅ Transactions transformation completed: {len(result_df)} records")
            return result_df
            
        except Exception as e:
            logger.error(f"❌ Error transforming transactions file: {e}")
            raise
    
    def process_files(self, input_dir: str, output_dir: str, date_str: str) -> Tuple[bool, bool]:
        """
        Process Lombard securities and transactions files.
        
        Args:
            input_dir: Directory containing input files
            output_dir: Directory for output files
            date_str: Date string in DD_MM_YYYY format
            
        Returns:
            Tuple of (securities_success, transactions_success)
        """
        logger.info(f"🚀 Processing Lombard files for date: {date_str}")
        
        securities_success = False
        transactions_success = False
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Process securities file
        securities_file = os.path.join(input_dir, f"LO_securities_{date_str}.xlsx")
        if os.path.exists(securities_file):
            try:
                logger.info(f"📄 Processing securities file: {securities_file}")
                securities_df = self.transform_securities(securities_file)
                
                # Save transformed securities
                output_securities = os.path.join(output_dir, f"LO_securities_{date_str}_processed.xlsx")
                securities_df.to_excel(output_securities, index=False)
                logger.info(f"✅ Securities saved to: {output_securities}")
                securities_success = True
                
            except Exception as e:
                logger.error(f"❌ Failed to process securities file: {e}")
        else:
            logger.warning(f"⚠️ Securities file not found: {securities_file}")
        
        # Process transactions file
        transactions_file = os.path.join(input_dir, f"LO_transactions_{date_str}.xlsx")
        if os.path.exists(transactions_file):
            try:
                logger.info(f"📄 Processing transactions file: {transactions_file}")
                
                # Pass securities file for CUSIP cross-reference
                securities_file_for_ref = os.path.join(input_dir, f"LO_securities_{date_str}.xlsx")
                transactions_df = self.transform_transactions(transactions_file, securities_file_for_ref)
                
                # Save transformed transactions
                output_transactions = os.path.join(output_dir, f"LO_transactions_{date_str}_processed.xlsx")
                transactions_df.to_excel(output_transactions, index=False)
                logger.info(f"✅ Transactions saved to: {output_transactions}")
                transactions_success = True
                
            except Exception as e:
                logger.error(f"❌ Failed to process transactions file: {e}")
        else:
            logger.warning(f"⚠️ Transactions file not found: {transactions_file}")
        
        logger.info(f"🏁 Processing completed - Securities: {securities_success}, Transactions: {transactions_success}")
        return securities_success, transactions_success