#!/usr/bin/env python3
"""
Santander Switzerland (STDSZ) File Enricher

Simple pass-through enricher that takes raw STDSZ files and enriches them with complex parsing logic.
Adds client and account columns extracted from filename and prepares for combination.

Currently handles single client (EI_Mazal) but designed to be future-ready for multiple clients.

Input: nonenriched_santander_switzerland/STDSZ_EI_Mazal_securities_DD_MM_YYYY.xlsx
Output: santander_switzerland/STDSZ_EI_Mazal_securities_DD_MM_YYYY.xlsx (with enriched data)

Author: Generated for Project Aurum
Date: 2025-01-15
"""

import os
import sys
import logging
import re
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.bank_detector import BankDetector

logger = logging.getLogger(__name__)


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


class STDSZEnricher:
    """Combines enriched STDSZ securities and transactions files."""
    
    def __init__(self):
        """Initialize the STDSZ enricher."""
        self.bank_code = 'STDSZ'
        self.parser = STDSZSecuritiesParser()
        self.mapper = STDSZUnifiedMapper()
        self.cost_calculator = STDSZCostBasisCalculator()
        logger.info(f"🏦 Initialized {self.bank_code} data enricher")
    
    def discover_raw_files(self, input_dir: Path, date: str) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        Discover raw STDSZ files for a specific date and group by client/account.
        
        Args:
            input_dir: Directory containing raw STDSZ files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping client_account to file paths
        """
        try:
            discovered_files = {}
            
            if not input_dir.exists():
                logger.warning(f"📁 Input directory does not exist: {input_dir}")
                return discovered_files
            
            # Pattern for raw STDSZ files: STDSZ_EI_Mazal_securities_31_07_2025.xlsx
            pattern = re.compile(r'^STDSZ_([A-Za-z0-9]+)_([A-Za-z0-9]+)_(securities|transactions)_(\d{2}_\d{2}_\d{4})\.xlsx?$')
            
            for file_path in input_dir.glob("*.xlsx"):
                match = pattern.match(file_path.name)
                if match:
                    client, account, file_type, file_date = match.groups()
                    
                    if file_date == date:
                        client_account = f"{client}_{account}"
                        
                        if client_account not in discovered_files:
                            discovered_files[client_account] = {
                                'securities': None,
                                'transactions': None,
                                'client': client,
                                'account': account
                            }
                        
                        discovered_files[client_account][file_type] = file_path
                        logger.info(f"📁 Found {file_type} file for {client_account}: {file_path.name}")
            
            logger.info(f"📊 Discovered {len(discovered_files)} client/account combinations for date {date}")
            return discovered_files
            
        except Exception as e:
            logger.error(f"❌ Error discovering raw STDSZ files: {e}")
            return {}
    
    def enrich_securities_file(self, securities_file: Path, client: str, account: str, output_dir: Path):
        """
        Enrich a single STDSZ securities file with complex parsing and cost basis calculation.
        
        Args:
            securities_file: Path to securities file
            client: Client name
            account: Account name
            output_dir: Output directory for enriched file
            
        Returns:
            Path to enriched output file
        """
        try:
            logger.info(f"🔄 Enriching STDSZ securities file: {securities_file.name}")
            logger.info(f"   Client: {client}, Account: {account}")
            
            # Step 1: Parse complex STDSZ file structure
            logger.info("📋 Step 1: Parsing complex STDSZ file structure...")
            grouped_data = self.parser.parse_securities_file(str(securities_file))
            
            # Step 2: Map to unified schema
            logger.info("🔄 Step 2: Mapping to unified schema...")
            unified_df = self.mapper.map_to_unified_schema(grouped_data)
            
            # Step 3: Calculate cost basis
            logger.info("💰 Step 3: Calculating cost basis...")
            enriched_df = self.cost_calculator.calculate_cost_basis(unified_df)
            
            # Step 4: Add client and account columns
            enriched_df['Client'] = client
            enriched_df['Account'] = account
            
            # Reorder columns to put Client and Account first
            cols = ['Client', 'Account'] + [col for col in enriched_df.columns if col not in ['Client', 'Account']]
            enriched_df = enriched_df[cols]
            
            # Step 5: Save enriched file
            output_filename = securities_file.name  # Keep original filename
            output_path = output_dir / output_filename
            enriched_df.to_excel(output_path, index=False, engine='openpyxl')
            
            logger.info(f"✅ Enriched securities saved: {output_path}")
            logger.info(f"   Total rows: {len(enriched_df)}")
            logger.info(f"   Client: {client}, Account: {account}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Error enriching securities file {securities_file}: {e}")
            raise
    
    def enrich_stdsz_files(self, input_dir: Path, date: str) -> Dict[str, Path]:
        """
        Main enrichment function for STDSZ files.
        
        Args:
            input_dir: Directory containing raw STDSZ files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping file types to output paths
        """
        try:
            logger.info(f"🚀 Starting STDSZ enrichment for date: {date}")
            
            # Set up output directory (parent directory)
            output_dir = input_dir.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Discover raw files
            discovered_files = self.discover_raw_files(input_dir, date)
            
            if not discovered_files:
                logger.warning(f"⚠️ No raw STDSZ files found for date {date}")
                return {}
            
            # Enrich each client's files
            result = {}
            processed_count = 0
            
            for client_account, files in discovered_files.items():
                client = files['client']
                account = files['account']
                
                # Enrich securities file
                if files['securities'] and files['securities'].exists():
                    enriched_securities_path = self.enrich_securities_file(
                        files['securities'], 
                        client, 
                        account, 
                        output_dir
                    )
                    result[f'securities_{client_account}'] = enriched_securities_path
                    processed_count += 1
                
                # TODO: Handle transactions file when available
                if files['transactions'] and files['transactions'].exists():
                    logger.info(f"   ℹ️ Transactions file found but enrichment not implemented yet")
            
            logger.info(f"🎉 STDSZ enrichment completed! Processed {processed_count} files")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ STDSZ enrichment failed: {e}")
            return {}


def main():
    """Main function for running STDSZ enrichment from command line."""
    parser = argparse.ArgumentParser(description='STDSZ File Enricher')
    parser.add_argument('--date', required=True, help='Date in DD_MM_YYYY format')
    parser.add_argument('--input-dir', required=True, help='Input directory containing raw STDSZ files')
    parser.add_argument('--output-dir', required=True, help='Output directory for enriched files')
    
    args = parser.parse_args()
    
    date = args.date
    input_dir = Path(args.input_dir)
    # Note: output_dir is handled by the enricher logic (parent of input_dir)
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run enrichment
    enricher = STDSZEnricher()
    result = enricher.enrich_stdsz_files(input_dir, date)
    
    if result:
        print(f"✅ STDSZ enrichment completed successfully!")
        for file_type, path in result.items():
            print(f"   {file_type}: {path}")
    else:
        print(f"❌ STDSZ enrichment failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()