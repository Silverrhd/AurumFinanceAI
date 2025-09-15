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
            standard_headers = assets[0]['headers']
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
            
            dataframes[asset_type.lower()] = df
            self.logger.info(f"📊 Created {asset_type} DataFrame: {len(df)} rows x {len(df.columns)} columns")
            
        return dataframes


class STDSZTransformer:
    """Main STDSZ Transformer - orchestrates all transformation steps"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.parser = STDSZSecuritiesParser()
        self.logger.info("🏦 STDSZ Transformer initialized")
        
    def transform_securities(self, input_file: str, output_file: str, mappings: dict = None):
        """
        Step 1: Parse STDSZ securities file into grouped DataFrames.
        Steps 2-3 (column mapping and final output) to be implemented.
        """
        try:
            self.logger.info(f"🚀 Starting STDSZ Securities transformation: {input_file}")
            
            # Step 1: Parse file into grouped DataFrames
            parsed_dataframes = self.parser.parse_securities_file(input_file)
            
            # For now, save the parsed DataFrames to see the results
            base_name = os.path.splitext(output_file)[0]
            
            for asset_type, df in parsed_dataframes.items():
                if not df.empty:
                    temp_output = f"{base_name}_{asset_type}_parsed.xlsx"
                    df.to_excel(temp_output, index=False)
                    self.logger.info(f"💾 Saved {asset_type} data to: {temp_output}")
            
            self.logger.info(f"✅ STDSZ Securities Step 1 completed successfully")
            
            # TODO: Step 2 - Column mapping to Aurum standard format
            # TODO: Step 3 - Generate final unified output file
            
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