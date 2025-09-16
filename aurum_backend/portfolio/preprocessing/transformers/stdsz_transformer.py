"""
Santander Switzerland (STDSZ) Transformer

Handles complex STDSZ files with multiple asset sections separated by empty rows.
The securities file has a unique structure with:
- Summary section (rows 0-14)
- Multiple asset class sections (Fixed Income, Equities, Short Term, Alternatives)
- Each section has sub-sections with their own headers
- Empty rows separate sections

Step 1: Parse and group assets by type (Bonds, Equities, Cash)
Step 2: Map columns to standard Aurum format (TODO)
Step 3: Generate final output files (TODO)

Expected Files:
- STDSZ_EI_Mazal_securities_DD_MM_YYYY.xlsx
- STDSZ_EI_Mazal_transactions_DD_MM_YYYY.xlsx

Author: Generated for Project Aurum
Date: 2025-01-15
"""

import pandas as pd
import logging
import os
from typing import Dict, List, Tuple, Optional

class STDSZSecuritiesParser:
    """Step 1: Parse STDSZ securities file into grouped DataFrames"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("🏦 STDSZ Securities Parser initialized")
        
    def parse_securities_file(self, input_file: str) -> Dict[str, pd.DataFrame]:
        """
        Main parsing function that returns 3 clean DataFrames grouped by asset type.
        
        Returns:
            Dict with keys: 'bonds', 'equities', 'cash'
            Each value is a DataFrame with that asset type's data
        """
        try:
            # Step 1: Read raw file
            df_raw = pd.read_excel(input_file, header=None)
            self.logger.info(f"📖 Read STDSZ file: {len(df_raw)} rows x {len(df_raw.columns)} columns")
            
            # Step 2: Find where asset sections begin
            summary_end = self._find_summary_end(df_raw)
            self.logger.info(f"📍 Asset sections start at row {summary_end}")
            
            # Step 3: Extract and group assets by type
            grouped_data = self._extract_asset_groups(df_raw, summary_end)
            self.logger.info(f"📊 Extracted: {len(grouped_data['BONDS'])} bonds, "
                            f"{len(grouped_data['EQUITIES'])} equities, "
                            f"{len(grouped_data['CASH'])} cash accounts")
            
            # Step 4: Create clean DataFrames
            dataframes = self._create_dataframes(grouped_data)
            
            # Step 5: Validate results
            total_assets = sum(len(df) for df in dataframes.values())
            self.logger.info(f"✅ Created {len(dataframes)} DataFrames with {total_assets} total assets")
            
            return dataframes
            
        except Exception as e:
            self.logger.error(f"❌ STDSZ parsing failed: {e}")
            raise
    
    def _find_summary_end(self, df: pd.DataFrame) -> int:
        """
        Dynamically find where the summary section ends by looking for the first 
        main asset section pattern.
        """
        for i in range(len(df)):
            row_data = df.iloc[i].values
            non_null = [str(x) for x in row_data if pd.notna(x) and str(x).strip() != '']
            
            # Look for pattern: ["FIXED INCOME", "31", "BALANCE IN REFERENCE CURRENCY", ...]
            if (len(non_null) >= 4 and 
                'FIXED INCOME' in str(non_null[0]).upper() and
                'BALANCE IN REFERENCE CURRENCY' in ' '.join(str(x) for x in non_null) and
                len(non_null) > 1 and non_null[1].replace('.','').isdigit()):
                self.logger.info(f"🎯 Summary end detected at row {i}")
                return i
        
        # Fallback to row 15 if pattern not found
        self.logger.warning("⚠️ Could not detect summary end dynamically, using fallback row 15")
        return 15
    
    def _is_main_section(self, cells: List[str]) -> bool:
        """Check if row is a main asset class section header"""
        return (len(cells) >= 4 and
                'BALANCE IN REFERENCE CURRENCY' in ' '.join(str(x) for x in cells) and
                len(cells) > 1 and cells[1].replace('.','').isdigit())
    
    def _is_subsection_with_headers(self, cells: List[str]) -> bool:
        """Check if row is a subsection header with column names"""
        return (len(cells) >= 2 and 
                'ISIN' in str(cells[1]).upper() and
                any(keyword in str(cells[0]).upper() for keyword in 
                    ['BONDS', 'STOCKS', 'FUNDS', 'GOVERNMENTS', 'INVESTMENT', 'HIGH YIELD', 
                     'EMERGING', 'US EQUITIES', 'OTHER', 'ALTERNATIVE', 'COMMERCIAL PAPER']))
    
    def _is_cash_headers(self, cells: List[str]) -> bool:
        """Check if row contains cash/liquidity headers"""
        return (len(cells) >= 3 and 
                any(cash_col in ' '.join(str(x) for x in cells).upper() 
                    for cash_col in ['ACCOUNT NUMBER', 'MARKET VALUE', 'LIQUIDITY']))
    
    def _determine_asset_type(self, headers: List[str]) -> str:
        """
        Determine asset type based on column headers
        """
        headers_str = ' '.join(str(h) for h in headers).upper()
        
        # BONDS: Have bond-specific columns
        if any(bond_col in headers_str for bond_col in 
               ['FREQUENCY', 'RATING', 'MATURITY DATE', 'NOMINAL']):
            return 'BONDS'
        
        # EQUITIES: Have equity-specific columns  
        elif any(equity_col in headers_str for equity_col in
                 ['QUANTITY', 'UNIT COST', 'NAV DATE']):
            return 'EQUITIES'
            
        # CASH: Have cash-specific columns
        elif any(cash_col in headers_str for cash_col in
                 ['ACCOUNT NUMBER', 'MARKET VALUE', 'LIQUIDITY']):
            return 'CASH'
            
        else:
            return 'UNKNOWN'
    
    def _extract_asset_groups(self, df: pd.DataFrame, start_row: int) -> Dict[str, List]:
        """
        Parse file using state machine to group assets correctly
        """
        groups = {'BONDS': [], 'EQUITIES': [], 'CASH': []}
        
        current_main_section = None
        current_subsection = None  
        current_headers = None
        current_asset_type = None
        
        for i in range(start_row, len(df)):
            row = df.iloc[i].values
            cells = [str(x) for x in row if pd.notna(x) and str(x).strip() != '']
            
            if not cells:
                continue
                
            # STATE 1: Main section detection
            if self._is_main_section(cells):
                current_main_section = cells[0]  # "FIXED INCOME", "EQUITIES", etc.
                self.logger.debug(f"Found main section: {current_main_section}")
                
            # STATE 2: Subsection with headers detection  
            elif self._is_subsection_with_headers(cells):
                current_subsection = cells[0]   # "GOVERNMENTS FIXED INCOME BONDS"
                current_headers = cells[1:]     # ["ISIN", "FREQUENCY", ...]
                current_asset_type = self._determine_asset_type(current_headers)
                self.logger.debug(f"Found subsection: {current_subsection} -> {current_asset_type}")
                
            # STATE 3: Cash headers detection (different pattern)
            elif self._is_cash_headers(cells):
                current_subsection = cells[0]   # "LIQUIDITY" 
                current_headers = cells[1:]     # ["ACCOUNT NUMBER", ...]
                current_asset_type = 'CASH'
                current_main_section = current_main_section or 'SHORT TERM'  # Default main section
                self.logger.debug(f"Found cash headers: {current_subsection} -> CASH")
                
            # STATE 4: Data row - add to appropriate group
            elif current_headers and current_asset_type in groups:
                asset_data = {
                    'row_number': i,
                    'main_section': current_main_section,
                    'subsection': current_subsection,
                    'data_cells': cells,
                    'headers': current_headers
                }
                groups[current_asset_type].append(asset_data)
        
        return groups
    
    def _create_dataframes(self, grouped_data: Dict) -> Dict[str, pd.DataFrame]:
        """
        Convert grouped data into clean DataFrames with proper columns
        """
        dataframes = {}
        
        for asset_type, assets in grouped_data.items():
            if not assets:
                dataframes[asset_type.lower()] = pd.DataFrame()
                self.logger.info(f"📝 Created empty DataFrame for {asset_type}")
                continue
                
            # Get standard headers for this asset type
            # The headers from subsection include the subsection name first, then actual column headers
            raw_headers = assets[0]['headers']
            
            # The actual data columns start with Security Name, then the subsection headers (minus the subsection name)
            if asset_type == 'CASH':
                # Cash has different structure: ["ACCOUNT NUMBER", "MARKET VALUE", ...]
                standard_headers = ['Security_Name'] + raw_headers
            else:
                # Bonds/Equities: subsection headers are ["ISIN", "FREQUENCY", ...] 
                standard_headers = ['Security_Name'] + raw_headers
                
            self.logger.info(f"📋 {asset_type} headers: {standard_headers[:4]}...")
            
            # Create data matrix
            data_matrix = []
            for asset in assets:
                # Pad data to match header count
                padded_data = list(asset['data_cells'])
                while len(padded_data) < len(standard_headers):
                    padded_data.append('')
                
                # Add metadata
                row_data = padded_data[:len(standard_headers)] + [
                    asset['main_section'],      # Asset_Class
                    asset['subsection'],        # Sub_Class  
                    'STDSZ',                   # Bank_Code
                    asset['row_number']        # Source_Row
                ]
                data_matrix.append(row_data)
            
            # Create DataFrame
            all_columns = list(standard_headers) + ['Asset_Class', 'Sub_Class', 'Bank_Code', 'Source_Row']
            df = pd.DataFrame(data_matrix, columns=all_columns)
            
            # Apply post-processing fixes for specific asset types
            if asset_type == 'BONDS':
                df = self._fix_commercial_paper_market_value(df)
            
            dataframes[asset_type.lower()] = df
            self.logger.info(f"📊 Created {asset_type} DataFrame: {len(df)} rows x {len(df.columns)} columns")
            
        return dataframes
    
    def _fix_commercial_paper_market_value(self, bonds_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fix Commercial Paper market values that get misaligned during parsing.
        
        Issue: Commercial Paper data has market value at position 13 in raw data,
        but our parser maps it to ACCRUED INTEREST instead of MARKET VALUE column.
        
        Solution: Move the value from ACCRUED INTEREST to MARKET VALUE for Commercial Paper only.
        """
        cp_mask = bonds_df['Sub_Class'] == 'COMMERCIAL PAPER'
        
        if cp_mask.any():
            # Get the correct market value from ACCRUED INTEREST column
            correct_market_values = bonds_df.loc[cp_mask, 'ACCRUED INTEREST']
            
            # Move it to MARKET VALUE column
            bonds_df.loc[cp_mask, 'MARKET VALUE'] = correct_market_values
            
            self.logger.info(f"🔧 Fixed {cp_mask.sum()} Commercial Paper market values")
            
            # Log the fix for transparency
            for idx in bonds_df[cp_mask].index:
                security_name = bonds_df.loc[idx, 'Security_Name']
                market_value = bonds_df.loc[idx, 'MARKET VALUE']
                self.logger.info(f"   📈 {security_name}: Market Value = {market_value}")
        
        return bonds_df


class STDSZUnifiedMapper:
    """Step 2: Map different asset types to unified schema"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("🔄 STDSZ Unified Mapper initialized")
        
        # Define unified mapping schema
        self.mapping_schema = {
            'Description': {
                'BONDS': 'Sub_Class',
                'EQUITIES': 'Sub_Class', 
                'CASH': 'Sub_Class'
            },
            'Security_Name': {
                'BONDS': 'Security_Name',
                'EQUITIES': 'Security_Name',
                'CASH': 'Security_Name'
            },
            'ISIN': {
                'BONDS': 'ISIN',
                'EQUITIES': 'ISIN',
                'CASH': 'ACCOUNT NUMBER'
            },
            'Frequency': {
                'BONDS': 'FREQUENCY',
                'EQUITIES': '',
                'CASH': ''
            },
            'Maturity_Date': {
                'BONDS': 'MATURITY DATE',
                'EQUITIES': '',
                'CASH': ''
            },
            'Quantity': {
                'BONDS': 'NOMINAL',
                'EQUITIES': 'QUANTITY',
                'CASH': ''
            },
            'Unit_Cost': {
                'BONDS': 'COST',
                'EQUITIES': 'UNIT COST',
                'CASH': ''
            },
            'Current_Price': {
                'BONDS': 'CURRENT PRICE',
                'EQUITIES': 'CURRENT PRICE',
                'CASH': ''
            },
            'Market_Value': {
                'BONDS': 'MARKET VALUE',
                'EQUITIES': 'MARKET VALUE',
                'CASH': 'MARKET VALUE'
            },
            'Unrealized_Gains_Losses': {
                'BONDS': 'UNREALIZED GAINS / LOSSES',
                'EQUITIES': 'UNREALIZED GAINS / LOSSES',
                'CASH': ''
            },
            'Currency': {
                'BONDS': 'CURRENCY',
                'EQUITIES': 'CURRENCY',
                'CASH': 'CURRENCY'
            },
            'Asset_Class': {
                'BONDS': 'Asset_Class',
                'EQUITIES': 'Asset_Class',
                'CASH': 'Asset_Class'
            },
            'Bank_Code': {
                'BONDS': 'Bank_Code',
                'EQUITIES': 'Bank_Code',
                'CASH': 'Bank_Code'
            }
        }
    
    def _find_similar_column(self, target_col: str, available_cols: List[str]) -> Optional[str]:
        """Find a similar column name when exact match fails"""
        target_upper = target_col.upper()
        
        # Look for columns that start with the target name
        for col in available_cols:
            if col.upper().startswith(target_upper):
                return col
        
        # Look for columns that contain the target name
        for col in available_cols:
            if target_upper in col.upper():
                return col
                
        return None
    
    def map_to_unified_schema(self, grouped_dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Combine all asset type DataFrames into a single unified DataFrame
        """
        try:
            unified_data = []
            
            for asset_type, df in grouped_dataframes.items():
                if df.empty:
                    continue
                    
                asset_type_key = asset_type.upper()
                self.logger.info(f"🔄 Mapping {asset_type} assets to unified schema ({len(df)} rows)")
                
                # Create unified rows for this asset type
                for _, row in df.iterrows():
                    unified_row = {}
                    
                    # Map each target column
                    for target_col, source_mapping in self.mapping_schema.items():
                        source_col = source_mapping.get(asset_type_key, '')
                        
                        if source_col == '':
                            # Empty for this asset type
                            unified_row[target_col] = ''
                        elif source_col in df.columns:
                            # Direct mapping - ensure we get scalar value
                            value = row[source_col]
                            # Handle pandas Series (multiple columns issue)
                            if hasattr(value, 'iloc'):
                                unified_row[target_col] = value.iloc[0] if len(value) > 0 else ''
                            else:
                                unified_row[target_col] = value
                        else:
                            # Column missing - try to find a similar column or set empty
                            similar_col = self._find_similar_column(source_col, df.columns)
                            if similar_col:
                                value = row[similar_col]
                                if hasattr(value, 'iloc'):
                                    unified_row[target_col] = value.iloc[0] if len(value) > 0 else ''
                                else:
                                    unified_row[target_col] = value
                                self.logger.debug(f"Used similar column '{similar_col}' for '{source_col}' in {asset_type}")
                            else:
                                self.logger.warning(f"Column '{source_col}' not found in {asset_type} data")
                                unified_row[target_col] = ''
                    
                    unified_data.append(unified_row)
            
            # Create unified DataFrame
            unified_df = pd.DataFrame(unified_data)
            self.logger.info(f"✅ Created unified DataFrame: {len(unified_df)} rows x {len(unified_df.columns)} columns")
            
            return unified_df
            
        except Exception as e:
            self.logger.error(f"❌ Unified mapping failed: {e}")
            raise


class STDSZCostBasisCalculator:
    """Step 3: Calculate cost basis using Market Value - Unrealized Gains/Losses"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("💰 STDSZ Cost Basis Calculator initialized")
    
    def calculate_cost_basis(self, unified_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add Cost_Basis column to unified DataFrame
        
        Formula: Cost_Basis = Market_Value - Unrealized_Gains_Losses
        
        For assets without Unrealized_Gains_Losses (like cash):
        Cost_Basis = Market_Value
        
        Args:
            unified_df: DataFrame with Market_Value and Unrealized_Gains_Losses columns
            
        Returns:
            DataFrame with added Cost_Basis column
        """
        try:
            self.logger.info("💰 Step 3: Calculating cost basis...")
            
            # Create a copy to avoid modifying original
            result_df = unified_df.copy()
            
            # Initialize Cost_Basis column
            result_df['Cost_Basis'] = 0.0
            
            for idx, row in result_df.iterrows():
                market_value = row.get('Market_Value', 0)
                unrealized_gl = row.get('Unrealized_Gains_Losses', None)
                
                # Calculate cost basis
                if pd.notna(unrealized_gl) and unrealized_gl != '':
                    # Cost Basis = Market Value - Unrealized Gains/Losses
                    cost_basis = float(market_value) - float(unrealized_gl)
                    self.logger.debug(f"📊 {row.get('Security_Name', 'Unknown')}: "
                                    f"Market Value {market_value} - Unrealized G/L {unrealized_gl} = Cost Basis {cost_basis}")
                else:
                    # If no unrealized G/L, cost basis = market value (typically cash/liquidity)
                    cost_basis = float(market_value)
                    self.logger.debug(f"💵 {row.get('Security_Name', 'Unknown')}: "
                                    f"Cost Basis = Market Value {cost_basis} (no unrealized G/L)")
                
                result_df.loc[idx, 'Cost_Basis'] = cost_basis
            
            # Log summary
            bonds_count = len(result_df[result_df['Asset_Class'] == 'BONDS'])
            equities_count = len(result_df[result_df['Asset_Class'] == 'EQUITIES'])
            cash_count = len(result_df[result_df['Asset_Class'] == 'CASH'])
            
            self.logger.info(f"✅ Cost basis calculated for {len(result_df)} assets:")
            self.logger.info(f"   📈 {bonds_count} bonds")
            self.logger.info(f"   📊 {equities_count} equities")
            self.logger.info(f"   💵 {cash_count} cash/liquidity")
            
            # Validate the example from user
            treasury_rows = result_df[result_df['Security_Name'].str.contains('US TREASURY', na=False)]
            if len(treasury_rows) > 0:
                treasury = treasury_rows.iloc[0]
                self.logger.info(f"🔍 Validation - US TREASURY:")
                self.logger.info(f"   Market Value: {treasury['Market_Value']}")
                self.logger.info(f"   Unrealized G/L: {treasury['Unrealized_Gains_Losses']}")
                self.logger.info(f"   Cost Basis: {treasury['Cost_Basis']}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"❌ Cost basis calculation failed: {e}")
            raise


class STDSZTransformer:
    """Main STDSZ Transformer - orchestrates all transformation steps"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.parser = STDSZSecuritiesParser()
        self.mapper = STDSZUnifiedMapper()
        self.cost_calculator = STDSZCostBasisCalculator()
        self.logger.info("🏦 STDSZ Transformer initialized")
        
    def transform_securities(self, securities_file: str) -> pd.DataFrame:
        """
        Transform STDSZ securities file to standard format.
        
        Input: STDSZ enriched/combined securities file with columns:
               ['Client', 'Account', 'Description', 'Security_Name', 'ISIN', 'Frequency', 
                'Maturity_Date', 'Quantity', 'Unit_Cost', 'Current_Price', 'Market_Value', 
                'Unrealized_Gains_Losses', 'Currency', 'Asset_Class', 'Bank_Code', 'Cost_Basis']
                
        Output: Standardized DataFrame with columns:
                ['bank', 'client', 'account', 'asset_type', 'ticker', 'name', 'cusip', 
                 'quantity', 'price', 'maturity_date', 'coupon_rate', 'market_value', 'cost_basis']
        """
        try:
            self.logger.info(f"🔄 Transforming STDSZ securities file: {securities_file}")
            
            # Step 1: Read input file
            df = pd.read_excel(securities_file)
            self.logger.info(f"📊 Loaded {len(df)} securities records with {len(df.columns)} columns")
            
            # Step 2: Initialize output DataFrame with required columns
            output_columns = ['bank', 'client', 'account', 'asset_type', 'ticker', 'name', 'cusip', 
                             'quantity', 'price', 'maturity_date', 'coupon_rate', 'market_value', 'cost_basis']
            result_df = pd.DataFrame()
            
            # Step 3: Direct column mappings
            self.logger.info("📋 Step 1: Basic column mappings...")
            result_df['client'] = df['Client']
            result_df['account'] = df['Account']
            result_df['name'] = df['Security_Name']
            result_df['cusip'] = df['ISIN']
            result_df['quantity'] = df['Quantity']
            result_df['market_value'] = df['Market_Value']
            result_df['cost_basis'] = df['Cost_Basis']
            result_df['bank'] = 'STDSZ'  # Hardcoded bank code
            result_df['ticker'] = ''  # Leave empty as specified
            
            # Step 4: Coupon rate extraction (Frequency: "4.3750 SEMIANNUAL" → "4,3750")
            self.logger.info("📋 Step 2: Extracting coupon rates...")
            result_df['coupon_rate'] = df['Frequency'].apply(self._extract_coupon_rate)
            
            # Step 5: Maturity date conversion (2027-11-15 → 11/15/2027)
            self.logger.info("📋 Step 3: Converting maturity dates...")
            result_df['maturity_date'] = df['Maturity_Date'].apply(self._convert_maturity_date)
            
            # Step 6: Asset type mapping (complex logic with SHORT TERM split)
            self.logger.info("📋 Step 4: Mapping asset types...")
            result_df['asset_type'] = df.apply(lambda row: self._map_asset_type(
                row['Asset_Class'], 
                pd.notna(row['Maturity_Date'])
            ), axis=1)
            
            # Step 7: Bond detection and price handling
            self.logger.info("📋 Step 5: Applying bond price logic...")
            bond_count = 0
            for idx, row in df.iterrows():
                is_bond = self._is_bond_by_description(row['Description'])
                if is_bond:
                    bond_count += 1
                
                # Apply price transformation with bond logic
                formatted_price = self._apply_stdsz_bond_price_logic(row['Current_Price'], is_bond)
                result_df.at[idx, 'price'] = formatted_price
            
            self.logger.info(f"  ✅ Identified {bond_count} bonds requiring special price handling")
            
            # Step 8: Apply European formatting to other numeric columns
            self.logger.info("📋 Step 6: Applying European number formatting...")
            numeric_columns = ['quantity', 'market_value', 'cost_basis']
            for col in numeric_columns:
                result_df[col] = result_df[col].apply(self._format_european_number)
            
            # Step 9: Reorder columns and validate
            result_df = result_df[output_columns]
            
            # Step 10: Log transformation summary
            self.logger.info(f"✅ STDSZ securities transformation completed:")
            self.logger.info(f"   📈 Input records: {len(df)}")
            self.logger.info(f"   📊 Output records: {len(result_df)}")
            self.logger.info(f"   📋 Output columns: {list(result_df.columns)}")
            
            # Asset type distribution
            asset_type_counts = result_df['asset_type'].value_counts()
            for asset_type, count in asset_type_counts.items():
                self.logger.info(f"   🏷️ {asset_type}: {count} records")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"❌ STDSZ securities transformation failed: {e}")
            # Return empty DataFrame with correct columns on error
            return pd.DataFrame(columns=['bank', 'client', 'account', 'asset_type', 'ticker', 'name', 'cusip', 
                                       'quantity', 'price', 'maturity_date', 'coupon_rate', 'market_value', 'cost_basis'])
        
    def transform_transactions(self, transactions_file: str) -> pd.DataFrame:
        """
        Transform STDSZ transactions file to standard format.
        
        Input: STDSZ enriched/combined transactions file with columns:
               ['Client', 'Account', 'PORTFOLIO', 'DETAIL', 'ISIN/IDENTIFIER', 
                'VALUE DATE', 'BOOKING DATE', 'CREDITS/DEBITS', 'CURRENCY', 'BALANCE', 'CURRENCY.1']
                
        Output: Standardized DataFrame with columns:
                ['bank', 'client', 'account', 'date', 'transaction_type', 'cusip', 'price', 'quantity', 'amount']
        """
        try:
            self.logger.info(f"🔄 Transforming STDSZ transactions file: {transactions_file}")
            
            # Step 1: Read input file
            df = pd.read_excel(transactions_file)
            self.logger.info(f"📊 Loaded {len(df)} transaction records with {len(df.columns)} columns")
            
            # Step 2: Initialize output DataFrame with required columns
            output_columns = ['bank', 'client', 'account', 'date', 'transaction_type', 'cusip', 'price', 'quantity', 'amount']
            result_df = pd.DataFrame()
            
            # Step 3: Direct column mappings
            self.logger.info("📋 Step 1: Copying basic identification columns...")
            result_df['client'] = df['Client']
            result_df['account'] = df['Account'] 
            result_df['transaction_type'] = df['DETAIL']
            result_df['cusip'] = df['ISIN/IDENTIFIER']
            result_df['amount'] = df['CREDITS/DEBITS']
            result_df['bank'] = 'STDSZ'  # Hardcoded bank code (must be after other columns)
            
            # Step 4: Date conversion (BOOKING DATE: "02 JUL 2025" → "07/02/2025")
            self.logger.info("📋 Step 2: Converting BOOKING DATE from 'DD MON YYYY' to 'MM/DD/YYYY'...")
            result_df['date'] = df['BOOKING DATE'].apply(self._convert_stdsz_date)
            
            converted_count = result_df['date'].dropna().shape[0]
            self.logger.info(f"  ✅ Converted {converted_count} dates to MM/DD/YYYY format")
            
            # Step 5: Empty columns as specified
            self.logger.info("📋 Step 3: Setting empty columns...")
            result_df['price'] = ''
            result_df['quantity'] = ''
            
            # Step 6: Reorder columns and validate
            result_df = result_df[output_columns]
            
            self.logger.info(f"✅ STDSZ transactions transformation completed:")
            self.logger.info(f"   📈 Input records: {len(df)}")
            self.logger.info(f"   📊 Output records: {len(result_df)}")
            self.logger.info(f"   📋 Output columns: {list(result_df.columns)}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"❌ STDSZ transactions transformation failed: {e}")
            # Return empty DataFrame with correct columns on error
            return pd.DataFrame(columns=['bank', 'client', 'account', 'date', 'transaction_type', 'cusip', 'price', 'quantity', 'amount'])
    
    def _convert_stdsz_date(self, date_str):
        """
        Convert STDSZ date format "DD MON YYYY" to MM/DD/YYYY
        Example: "02 JUL 2025" → "07/02/2025"
        """
        if pd.isna(date_str) or str(date_str).strip() == '':
            return None
            
        try:
            date_str = str(date_str).strip()
            
            # Split date string: "02 JUL 2025" → ["02", "JUL", "2025"]
            parts = date_str.split()
            if len(parts) != 3:
                self.logger.warning(f"Date format unexpected: '{date_str}' (expected 3 parts)")
                return None
                
            day, month_abbr, year = parts
            
            # Month abbreviation to number mapping
            month_mapping = {
                'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08', 
                'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
            }
            
            month_num = month_mapping.get(month_abbr.upper())
            if not month_num:
                self.logger.warning(f"Unknown month abbreviation: '{month_abbr}' in date '{date_str}'")
                return None
                
            # Format as MM/DD/YYYY
            formatted_date = f"{month_num}/{day.zfill(2)}/{year}"
            return formatted_date
            
        except Exception as e:
            self.logger.warning(f"Could not convert date '{date_str}': {e}")
            return None
    
    def _extract_coupon_rate(self, frequency_str):
        """
        Extract coupon rate from frequency string and convert to European format.
        Example: "4.3750 SEMIANNUAL" → "4,3750"
        """
        if pd.isna(frequency_str) or str(frequency_str).strip() == '':
            return ''
        
        try:
            # Split by space and take first part
            parts = str(frequency_str).strip().split()
            if parts:
                number_str = parts[0]
                # Convert decimal point to comma for European format
                european_number = number_str.replace('.', ',')
                return european_number
            return ''
        except Exception as e:
            self.logger.warning(f"Could not extract coupon rate from '{frequency_str}': {e}")
            return ''
    
    def _convert_maturity_date(self, date_str):
        """
        Convert STDSZ maturity date format from YYYY-MM-DD to MM/DD/YYYY.
        Example: "2027-11-15" → "11/15/2027"
        """
        if pd.isna(date_str) or str(date_str).strip() == '':
            return ''
        
        try:
            from datetime import datetime
            # Parse YYYY-MM-DD format
            date_obj = datetime.strptime(str(date_str), '%Y-%m-%d')
            # Format as MM/DD/YYYY
            return date_obj.strftime('%m/%d/%Y')
        except Exception as e:
            self.logger.warning(f"Could not convert maturity date '{date_str}': {e}")
            return ''
    
    def _map_asset_type(self, asset_class, has_maturity_date):
        """
        Map STDSZ Asset_Class to standard asset_type with special SHORT TERM logic.
        
        Rules:
        - FIXED INCOME → Fixed Income
        - SHORT TERM + has maturity → Fixed Income
        - SHORT TERM + no maturity → Cash
        - EQUITIES → Equity
        - ALTERNATIVE INVESTMENTS → Alternatives
        """
        if pd.isna(asset_class):
            return 'Unknown'
        
        asset_class_str = str(asset_class).strip()
        
        if asset_class_str == 'FIXED INCOME':
            return 'Fixed Income'
        elif asset_class_str == 'SHORT TERM':
            if has_maturity_date:
                return 'Fixed Income'  # SHORT TERM + maturity = Fixed Income
            else:
                return 'Cash'  # SHORT TERM + no maturity = Cash
        elif asset_class_str == 'EQUITIES':
            return 'Equity'
        elif asset_class_str == 'ALTERNATIVE INVESTMENTS':
            return 'Alternatives'
        else:
            self.logger.warning(f"Unknown asset class: '{asset_class_str}'")
            return 'Unknown'
    
    def _is_bond_by_description(self, description):
        """
        Determine if asset is a bond based on Description column.
        
        Bond descriptions that require special price handling:
        - GOVERNMENTS FIXED INCOME BONDS
        - INVESTMENT GRADE FIXED INCOME BONDS
        - HIGH YIELD FIXED INCOME BONDS
        - COMMERCIAL PAPER
        """
        if pd.isna(description):
            return False
        
        description_str = str(description).strip()
        bond_descriptions = [
            'GOVERNMENTS FIXED INCOME BONDS',
            'INVESTMENT GRADE FIXED INCOME BONDS', 
            'HIGH YIELD FIXED INCOME BONDS',
            'COMMERCIAL PAPER',
            'EMERGING FIXED INCOME BONDS'
        ]
        
        return description_str in bond_descriptions
    
    def _apply_stdsz_bond_price_logic(self, price_str, is_bond):
        """
        Apply STDSZ-specific price formatting.
        
        For non-bonds: American → European formatting
        For bonds: American → European → Bond price logic:
        - If starts with 1: 101,234 → 1,01234
        - If other number: 99,023 → 0,99023
        """
        if pd.isna(price_str) or str(price_str).strip() == '':
            return ''
        
        try:
            # Step 1: Convert to European format (replace . with ,)
            european_price = str(price_str).replace('.', ',')
            
            # Step 2: If not a bond, return European format
            if not is_bond:
                return european_price
            
            # Step 3: Bond-specific price logic
            # Split by comma to get integer and decimal parts
            if ',' in european_price:
                parts = european_price.split(',')
                integer_part = parts[0]
                decimal_part = parts[1] if len(parts) > 1 else ''
                
                if integer_part.startswith('1') and len(integer_part) > 1:
                    # 101,234 → 1,01234 (move digits after 1 to decimal)
                    remaining_digits = integer_part[1:]  # Remove the leading 1
                    return f"1,{remaining_digits}{decimal_part}"
                elif integer_part == '1':
                    # Edge case: exactly 1,xxx → 1,xxx (no change needed)
                    return european_price
                else:
                    # 99,023 → 0,99023 (put 0 before all digits)
                    return f"0,{integer_part}{decimal_part}"
            else:
                # No decimal part, handle integer only
                if price_str.startswith('1') and len(str(price_str)) > 1:
                    # 123 → 1,23
                    remaining = str(price_str)[1:]
                    return f"1,{remaining}"
                else:
                    # 99 → 0,99
                    return f"0,{price_str}"
                    
        except Exception as e:
            self.logger.warning(f"Could not apply bond price logic to '{price_str}': {e}")
            # Fallback to European formatting
            return str(price_str).replace('.', ',')
    
    def _format_european_number(self, value):
        """
        Convert number to European format (comma as decimal separator).
        """
        if pd.isna(value) or str(value).strip() == '':
            return ''
        
        try:
            # Convert to string and replace decimal point with comma
            return str(value).replace('.', ',')
        except Exception as e:
            self.logger.warning(f"Could not format number '{value}': {e}")
            return str(value)