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


class STDSZTransformer:
    """Main STDSZ Transformer - orchestrates all transformation steps"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.parser = STDSZSecuritiesParser()
        self.mapper = STDSZUnifiedMapper()
        self.logger.info("🏦 STDSZ Transformer initialized")
        
    def transform_securities(self, input_file: str, output_file: str, mappings: dict = None):
        """
        Complete STDSZ Securities transformation:
        Step 1: Parse file into grouped DataFrames
        Step 2: Map to unified schema  
        Step 3: Generate final output file
        """
        try:
            self.logger.info(f"🚀 Starting STDSZ Securities transformation: {input_file}")
            
            # Step 1: Parse file into grouped DataFrames
            self.logger.info("📋 Step 1: Parsing securities file...")
            parsed_dataframes = self.parser.parse_securities_file(input_file)
            
            # Step 2: Map to unified schema
            self.logger.info("🔄 Step 2: Mapping to unified schema...")
            unified_df = self.mapper.map_to_unified_schema(parsed_dataframes)
            
            # Step 3: Generate final output file
            self.logger.info("💾 Step 3: Generating final output...")
            unified_df.to_excel(output_file, index=False)
            self.logger.info(f"✅ Final unified file saved: {output_file}")
            
            # Also save intermediate files for debugging
            base_name = os.path.splitext(output_file)[0]
            for asset_type, df in parsed_dataframes.items():
                if not df.empty:
                    temp_output = f"{base_name}_{asset_type}_step1.xlsx"
                    df.to_excel(temp_output, index=False)
            
            self.logger.info(f"🎉 STDSZ Securities transformation completed successfully!")
            self.logger.info(f"📊 Final result: {len(unified_df)} assets in unified format")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ STDSZ Securities transformation failed: {e}")
            return False
        
    def transform_transactions(self, input_file: str, output_file: str, mappings: dict = None):
        """
        Transform STDSZ transactions file to standard format.
        TODO: Implement transactions transformation logic.
        """
        self.logger.info(f"🏦 STDSZ Transactions Transformer called: {input_file}")
        self.logger.warning("⚠️  STDSZ transactions transformation not implemented yet")
        
        # TODO: Implement actual transformation
        return True