#!/usr/bin/env python3
"""
Pictet Bank Data Transformer

Transforms Pictet bank data files into standardized format for AurumFinance.
Handles securities and transactions data with Pictet-specific logic.
"""

import logging
import pandas as pd
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class PictetTransformer:
    """Transformer for Pictet bank data files."""
    
    def __init__(self):
        """Initialize Pictet transformer."""
        self.bank_name = 'Pictet'
        logger.info(f"🏦 Initialized {self.bank_name} transformer")
    
    
    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform Pictet securities data to standard format.
        
        Args:
            securities_file: Path to combined Pictet securities Excel file
            
        Returns:
            DataFrame with transformed securities data
        """
        logger.info(f"🔄 Transforming Pictet securities file: {securities_file}")
        
        try:
            # Read the combined securities file
            df = pd.read_excel(securities_file)
            
            if df.empty:
                logger.warning("⚠️ Securities file is empty")
                return self._create_empty_securities_dataframe()
            
            logger.info(f"📊 Loaded {len(df)} securities with {len(df.columns)} columns")
            logger.info(f"📋 Available columns: {list(df.columns)}")
            
            # Create output DataFrame with standard columns
            output_columns = ['bank', 'client', 'account', 'asset_type', 'name', 
                            'cost_basis', 'market_value', 'quantity', 'price', 
                            'ticker', 'cusip', 'coupon_rate', 'maturity_date']
            result_df = pd.DataFrame()
            
            # Step 1: Direct column mappings
            logger.info("📋 Step 1: Direct column mappings")
            result_df['bank'] = df['bank']
            result_df['client'] = df['client'] 
            result_df['account'] = df['account']
            result_df['name'] = df['name']
            result_df['market_value'] = df['Valuation\n(USD)']
            result_df['quantity'] = df['Quantity']
            result_df['ticker'] = df['Pictet\ncode']
            result_df['coupon_rate'] = df['Coupon']
            
            # Step 2: Asset type detection using Bloomberg + name logic
            logger.info("📋 Step 2: Asset type detection from Bloomberg codes")
            result_df['asset_type'] = df.apply(
                lambda row: self._detect_asset_type(row['Bloomberg'], row['name']), axis=1
            )
            
            # Log asset type distribution
            asset_counts = result_df['asset_type'].value_counts()
            for asset_type, count in asset_counts.items():
                logger.info(f"  📊 {asset_type}: {count} securities")
            
            # Step 3: Bond detection and price processing
            logger.info("📋 Step 3: Bond detection and price processing")
            bond_count = 0
            for idx, row in df.iterrows():
                is_bond = self._is_bond(row['Bloomberg'])
                if is_bond:
                    bond_count += 1
                    # Apply bond price logic
                    bond_price = self._convert_bond_price(row['Market \nprice'])
                    result_df.at[idx, 'price'] = bond_price
                else:
                    # Keep original price for non-bonds (preserves European formatting)
                    result_df.at[idx, 'price'] = row['Market \nprice']
            
            logger.info(f"  📈 Bonds detected: {bond_count}")
            logger.info(f"  📈 Bond prices converted: {bond_count}")
            logger.info(f"  📈 Non-bond prices preserved: {len(df) - bond_count}")
            
            # Step 4: CUSIP/ISIN priority logic
            logger.info("📋 Step 4: CUSIP/ISIN priority processing")
            if 'CUSIP' in df.columns:
                result_df['cusip'] = df.apply(
                    lambda row: self._get_cusip_value(row.get('CUSIP'), row.get('ISIN')), axis=1
                )
                
                # Count actual CUSIP vs ISIN usage
                cusip_used = 0
                isin_used = 0
                none_used = 0
                
                for idx, row in df.iterrows():
                    cusip_val = row.get('CUSIP')
                    isin_val = row.get('ISIN')
                    
                    # Check what actually got used
                    if (pd.notna(cusip_val) and str(cusip_val).strip() != '' and str(cusip_val).strip() != '-'):
                        cusip_used += 1
                    elif (pd.notna(isin_val) and str(isin_val).strip() != '' and str(isin_val).strip() != '-'):
                        isin_used += 1
                    else:
                        none_used += 1
                
                total_assigned = result_df['cusip'].notna().sum()
                logger.info(f"  📊 CUSIP values used: {cusip_used}")
                logger.info(f"  📊 ISIN values used (fallback): {isin_used}")
                logger.info(f"  📊 None/empty values: {none_used}")
                logger.info(f"  📊 Total values assigned: {total_assigned}")
            else:
                result_df['cusip'] = df['ISIN']
                isin_count = result_df['cusip'].notna().sum()
                logger.info(f"  📊 ISIN values assigned: {isin_count} (no CUSIP column found)")
            
            # Step 5: Maturity date conversion
            logger.info("📋 Step 5: Maturity date conversion")
            result_df['maturity_date'] = df['Probable\nmaturity'].apply(self._convert_maturity_date)
            date_count = result_df['maturity_date'].notna().sum()
            logger.info(f"  📅 Maturity dates converted: {date_count}")
            
            # Step 6: Set cost_basis to None (not provided by Pictet)
            result_df['cost_basis'] = None
            
            # Step 7: Ensure column order and final validation
            result_df = result_df[output_columns]
            
            logger.info(f"✅ Transformation completed: {len(result_df)} securities")
            logger.info(f"📊 Output columns: {list(result_df.columns)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"❌ Error transforming securities file {securities_file}: {str(e)}")
            return self._create_empty_securities_dataframe()
    
    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """
        Transform Pictet transactions data to standard format.
        
        Args:
            transactions_file: Path to combined Pictet transactions Excel file
            
        Returns:
            DataFrame with transformed transactions data in standard format
        """
        logger.info(f"🔄 Transforming Pictet transactions file: {transactions_file}")
        
        try:
            # Read the combined transactions file
            df = pd.read_excel(transactions_file)
            
            if df.empty:
                logger.warning("⚠️ Transactions file is empty")
                return pd.DataFrame(columns=['bank', 'client', 'account', 'date', 'transaction_type', 
                                           'amount', 'price', 'quantity', 'cusip'])
            
            logger.info(f"📊 Loaded {len(df)} transactions with {len(df.columns)} columns")
            
            # Create output DataFrame with standard columns
            output_columns = ['bank', 'client', 'account', 'date', 'transaction_type', 
                            'amount', 'price', 'quantity', 'cusip']
            result_df = pd.DataFrame()
            
            # Step 1: Direct mappings (transfer as-is)
            logger.debug("📋 Step 1: Direct column mappings")
            result_df['bank'] = df['bank']
            result_df['client'] = df['client']
            result_df['account'] = df['account']
            result_df['transaction_type'] = df['Order type']
            result_df['amount'] = df['Amount\n(net)']  # European formatting preserved as-is
            
            # Step 2: Date transformation (Timestamp → MM/DD/YYYY)
            logger.debug("📅 Step 2: Date format transformation")
            result_df['date'] = df['Value date'].dt.strftime('%m/%d/%Y')

            # Step 3: Empty columns (system handles these)
            logger.debug("⭕ Step 3: Setting empty columns")
            result_df['price'] = None
            result_df['quantity'] = None

            # Step 4: CUSIP matching logic
            logger.info("📋 Step 4: Matching transactions to securities for CUSIP lookup")

            # Load transformed securities file to get CUSIP mappings
            securities_file = transactions_file.replace('_transactions_', '_securities_')
            if Path(securities_file).exists():
                logger.info(f"  📂 Loading securities file: {Path(securities_file).name}")
                securities_df = pd.read_excel(securities_file)

                cusips = []
                matched_count = 0
                unmatched_transactions = []

                for idx, row in df.iterrows():
                    description = row.get('Description', '')
                    order_type = row.get('Order type', '')

                    # Extract asset name from description
                    extracted_name = self._extract_asset_name_from_description(description)

                    if extracted_name:
                        # Match to security and get CUSIP
                        cusip = self._match_transaction_to_security(extracted_name, securities_df)
                        if cusip:
                            matched_count += 1
                        else:
                            unmatched_transactions.append((order_type, description))
                        cusips.append(cusip)
                    else:
                        # No extraction (fees, external flows, etc.)
                        cusips.append(None)

                result_df['cusip'] = cusips
                logger.info(f"  ✅ Matched {matched_count}/{len(df)} transactions to securities")

                if unmatched_transactions:
                    logger.warning(f"  ⚠️ {len(unmatched_transactions)} transactions could not be matched:")
                    for order_type, desc in unmatched_transactions[:5]:  # Show first 5
                        logger.warning(f"    • {order_type}: {desc[:80]}")
                    if len(unmatched_transactions) > 5:
                        logger.warning(f"    ... and {len(unmatched_transactions) - 5} more")
            else:
                logger.warning(f"⚠️ Securities file not found: {securities_file}")
                logger.warning("⚠️ Cannot populate transaction CUSIPs - all will be None")
                result_df['cusip'] = None

            # Step 5: Ensure column order
            result_df = result_df[output_columns]
            
            logger.info(f"✅ Transformation completed: {len(result_df)} transactions")
            logger.info(f"📊 Output columns: {list(result_df.columns)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"❌ Error transforming transactions file {transactions_file}: {str(e)}")
            # Return empty DataFrame with correct structure on error
            return pd.DataFrame(columns=['bank', 'client', 'account', 'date', 'transaction_type',
                                       'amount', 'price', 'quantity', 'cusip'])

    def _extract_asset_name_from_description(self, description: str) -> Optional[str]:
        """
        Extract asset identifier from Pictet transaction description.

        Examples:
        - "Ordinary interest 140000 5.60% HYUNDAI CAP. 23/28 SR S" → "5.60% HYUNDAI CAP. 23/28"
        - "Purchase 18000 TBI USA 150126 SR" → "TBI USA 150126"
        - "Interest 102000 9.7% JPM (SPX/SX5E/NKY) 25/26" → "9.7% JPM (SPX/SX5E/NKY) 25/26"
        - "Management fees 3rd quarter 2025" → None

        Args:
            description: Transaction description from Pictet file

        Returns:
            Extracted asset name or None if not a security transaction
        """
        if pd.isna(description) or not description:
            return None

        # Pattern: Extract after quantity, before optional "SR S" suffix
        patterns = [
            # Pattern for interest/redemptions: "Interest 140000 5.60% HYUNDAI CAP. 23/28 SR S"
            r'(?:Ordinary interest|Interest|Early redemption)\s+\d+\s+([\d.]+%\s+[\w\s\.\-\(\)/]+?)(?:\s+SR\s*S?)?$',
            # Pattern for purchases/sales: "Purchase 18000 TBI USA 150126 SR"
            r'(?:Purchase|Sale)\s+\d+\s+([\w\s\.\-\(\)/]+?)(?:\s+SR\s*S?)?$',
            # Pattern for redemptions: "Redemption 500 LOFS-SHO.-T.MONEY MKT(USD)M USD-ACC"
            r'Redemption\s+\d+\s+([\w\s\.\-\(\)/]+?)$',
        ]

        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                extracted = match.group(1).strip()
                # Remove trailing "SR S" or "SR" if still present
                extracted = re.sub(r'\s+SR\s*S?$', '', extracted, flags=re.IGNORECASE)
                logger.debug(f"Extracted asset name: '{description}' → '{extracted}'")
                return extracted

        logger.debug(f"No asset name extracted from: '{description}'")
        return None

    def _match_transaction_to_security(self, extracted_name: str, securities_df: pd.DataFrame) -> Optional[str]:
        """
        Match extracted transaction name to security and return CUSIP.
        Uses case-insensitive fuzzy matching to handle minor format differences.

        Args:
            extracted_name: Asset name extracted from transaction description
            securities_df: DataFrame of transformed securities with 'name' and 'cusip'/'CUSIP' columns

        Returns:
            CUSIP/ISIN value or None if no match found
        """
        if not extracted_name or securities_df.empty:
            return None

        # Normalize for comparison
        extracted_upper = extracted_name.upper().strip()

        # Try to find match in securities
        for idx, row in securities_df.iterrows():
            sec_name = str(row.get('name', '')).upper().strip()

            # Check if either contains the other (handles minor format differences)
            # e.g., "5.60% HYUNDAI CAP. 23/28" matches "5.60% Hyundai Cap. 23/28 Sr S"
            if extracted_upper in sec_name or sec_name in extracted_upper:
                # Try both lowercase and uppercase CUSIP column names
                cusip = row.get('cusip') if 'cusip' in securities_df.columns else row.get('CUSIP')
                if pd.notna(cusip) and str(cusip).strip() != '' and str(cusip).strip() != '-':
                    logger.debug(f"✅ Matched '{extracted_name}' → '{row['name']}' (CUSIP: {cusip})")
                    return str(cusip).strip()

        logger.debug(f"⚠️ No security match found for '{extracted_name}'")
        return None

    def _standardize_date_format(self, date_str: str) -> Optional[str]:
        """
        Standardize date format for database insertion.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            Standardized date string in YYYY-MM-DD format or None
        """
        if pd.isna(date_str) or not date_str:
            return None
        
        # TODO: Add Pictet-specific date format handling
        # Common formats might include: DD/MM/YYYY, DD-MM-YYYY, etc.
        
        try:
            # Placeholder - will be customized based on Pictet date formats
            if isinstance(date_str, str):
                # Handle common European date formats
                if '/' in date_str:
                    parsed_date = datetime.strptime(date_str, '%d/%m/%Y')
                elif '-' in date_str:
                    parsed_date = datetime.strptime(date_str, '%d-%m-%Y')
                else:
                    return None
                
                return parsed_date.strftime('%Y-%m-%d')
            
            return None
            
        except ValueError:
            logger.warning(f"⚠️ Could not parse date: {date_str}")
            return None
    
    def _clean_numeric_value(self, value: Any) -> Optional[float]:
        """
        Clean and standardize numeric values.
        
        Args:
            value: Raw numeric value (could be string, float, etc.)
            
        Returns:
            Cleaned float value or None
        """
        if pd.isna(value) or value == '':
            return None
        
        try:
            # Handle string representations of numbers
            if isinstance(value, str):
                # Remove common formatting characters
                cleaned = value.replace(',', '').replace(' ', '')
                # Handle negative values in parentheses (accounting format)
                if cleaned.startswith('(') and cleaned.endswith(')'):
                    cleaned = '-' + cleaned[1:-1]
                
                return float(cleaned)
            
            return float(value)
            
        except (ValueError, TypeError):
            logger.warning(f"⚠️ Could not parse numeric value: {value}")
            return None
    
    def _create_empty_securities_dataframe(self) -> pd.DataFrame:
        """
        Create empty securities dataframe with correct structure.
        
        Returns:
            Empty DataFrame with standard securities columns
        """
        columns = ['bank', 'client', 'account', 'asset_type', 'name', 
                  'cost_basis', 'market_value', 'quantity', 'price', 
                  'ticker', 'cusip', 'coupon_rate', 'maturity_date']
        return pd.DataFrame(columns=columns)
    
    def _detect_asset_type(self, bloomberg_code: str, name: str) -> str:
        """
        Detect asset type from Bloomberg code and name.
        
        Logic:
        - Bloomberg ends with "Equity" → "Equity"
        - Bloomberg ends with "Corp" → "Fixed Income" 
        - Bloomberg ends with "Govt" → "Fixed Income"
        - Bloomberg is "-" AND name is "Usd Common" → "Cash"
        - Bloomberg is "-" AND name is NOT "Usd Common" → "Alternatives"
        
        Args:
            bloomberg_code: Bloomberg identifier code
            name: Asset name
            
        Returns:
            Asset type string
        """
        bloomberg_str = str(bloomberg_code).strip() if pd.notna(bloomberg_code) else ""
        name_str = str(name).strip() if pd.notna(name) else ""
        
        if bloomberg_str.endswith('Equity'):
            return 'Equity'
        elif bloomberg_str.endswith('Corp') or bloomberg_str.endswith('Govt'):
            return 'Fixed Income'
        elif bloomberg_str == '-':
            if name_str == 'Usd Common':
                return 'Cash'
            else:
                return 'Alternatives'
        else:
            logger.debug(f"⚠️ Unknown Bloomberg pattern: '{bloomberg_str}' for '{name_str}'")
            return 'Unknown'
    
    def _is_bond(self, bloomberg_code: str) -> bool:
        """
        Detect if asset is a bond based on Bloomberg code.
        Bonds end with 'Corp' or 'Govt'.
        
        Args:
            bloomberg_code: Bloomberg identifier code
            
        Returns:
            True if asset is a bond
        """
        bloomberg_str = str(bloomberg_code).strip() if pd.notna(bloomberg_code) else ""
        return bloomberg_str.endswith('Corp') or bloomberg_str.endswith('Govt')
    
    def _convert_bond_price(self, price) -> Optional[str]:
        """
        Apply Pictet bond price logic.
        
        Examples:
        - 103,6279346 → 1,036279346 (starts with 1: comma after 1)  
        - 90,14069571 → 0,9014069571 (other: comma before number)
        
        Args:
            price: Original price value
            
        Returns:
            Converted price as string with European decimal format
        """
        if pd.isna(price) or price == '' or price is None:
            return None
        
        try:
            # Convert to string and clean
            price_str = str(price).strip()
            
            # Remove existing commas and periods for processing
            price_clean = price_str.replace(',', '').replace('.', '')
            
            if price_clean.startswith('1'):
                result = f"1,{price_clean[1:]}"
                logger.debug(f"Bond price (starts with 1): {price} → {result}")
                return result
            else:
                result = f"0,{price_clean}"
                logger.debug(f"Bond price (other number): {price} → {result}")
                return result
                
        except Exception as e:
            logger.warning(f"⚠️ Could not convert bond price '{price}': {e}")
            return str(price)  # Return original on error
    
    def _get_cusip_value(self, cusip_val, isin_val) -> Optional[str]:
        """
        Get CUSIP value with priority logic.
        Priority: CUSIP first, then ISIN.
        Treats "-" as "no value" for both CUSIP and ISIN.
        
        Args:
            cusip_val: CUSIP value
            isin_val: ISIN value
            
        Returns:
            CUSIP/ISIN value or None
        """
        # Check if CUSIP has a real value (not NaN, not empty, not "-")
        if (pd.notna(cusip_val) and 
            str(cusip_val).strip() != '' and 
            str(cusip_val).strip() != '-'):
            return str(cusip_val).strip()
        # Fall back to ISIN (also exclude "-")
        elif (pd.notna(isin_val) and 
              str(isin_val).strip() != '' and 
              str(isin_val).strip() != '-'):
            return str(isin_val).strip()
        else:
            return None
    
    def _convert_maturity_date(self, date_str) -> Optional[str]:
        """
        Convert maturity date from 2034-11-12 00:00:00 to 11/12/2034.
        Remove timestamp and convert YYYY-MM-DD to MM/DD/YYYY.
        
        Args:
            date_str: Date string with or without timestamp
            
        Returns:
            Date in MM/DD/YYYY format or None
        """
        if pd.isna(date_str) or str(date_str).strip() == '':
            return None
            
        try:
            from datetime import datetime
            # Handle both with and without timestamp
            date_clean = str(date_str).split(' ')[0]  # Remove timestamp
            date_obj = datetime.strptime(date_clean, '%Y-%m-%d')
            return date_obj.strftime('%m/%d/%Y')
        except Exception as e:
            logger.debug(f"⚠️ Could not convert maturity date '{date_str}': {e}")
            return None