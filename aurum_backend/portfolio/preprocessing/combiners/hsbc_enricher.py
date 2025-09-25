#!/usr/bin/env python3
"""
HSBC Data Enricher

Enriches HSBC securities files with unit cost data and adds bank/client/account columns.
Merges 'Total Cost' from unitcost files into securities files.
Note: No longer extracts Estimated Annual Income (using description-based coupon extraction instead).
Uses account mappings from Mappings.xlsx to map account numbers to account names.
"""

import os
import sys
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Add the project root to Python path  
aurum_backend_root = Path(__file__).parent.parent.parent  # Gets to aurum_backend
sys.path.insert(0, str(aurum_backend_root))

from portfolio.services.mappings_encryption_service import MappingsEncryptionService
from preprocessing.bank_detector import BankDetector
from preprocessing.combiners.header_detector import HeaderDetector

logger = logging.getLogger(__name__)


class HSBCEnricher:
    """Enriches HSBC securities files with unit cost data and adds bank/client/account columns."""
    
    def __init__(self):
        """Initialize the HSBC enricher."""
        self.bank_code = 'HSBC'
        logger.info(f"🏦 Initialized {self.bank_code} data enricher")
    
    def load_account_mappings(self, mappings_file: str) -> Dict[str, str]:
        """
        Load HSBC account mappings from Excel file.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            
        Returns:
            Dict mapping account numbers to account names
        """
        logger.info(f"Loading HSBC account mappings from {mappings_file}")
        
        try:
            encryption_service = MappingsEncryptionService()
            df = encryption_service.read_encrypted_excel(mappings_file + '.encrypted', sheet_name='HSBC')
            
            # Validate required columns
            required_cols = ['Account Number', 'client', 'account']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"HSBC mappings missing required columns: {missing_cols}")
            
            # Create mapping dictionary: account_number -> account_name
            mappings = {}
            
            for _, row in df.iterrows():
                account_num = str(row['Account Number']).strip()
                account_name = str(row['account']).strip()
                
                # Skip rows with missing data
                if pd.isna(row['Account Number']) or pd.isna(row['account']):
                    logger.warning(f"Skipping mapping for account {account_num} - missing data")
                    continue
                
                mappings[account_num] = account_name
            
            logger.info(f"Loaded {len(mappings)} HSBC account mappings: {mappings}")
            return mappings
            
        except Exception as e:
            logger.error(f"Error loading HSBC account mappings: {e}")
            raise
    
    def discover_hsbc_files(self, input_dir: Path, date: str) -> Dict[str, Optional[Path]]:
        """
        Discover HSBC files for a specific date.
        
        Args:
            input_dir: Directory containing HSBC files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping file types to paths:
            {
                'securities': Path(...),
                'unitcost': Path(...),
                'transactions': Path(...) or None
            }
        """
        logger.info(f"🔍 Scanning for HSBC files in: {input_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not input_dir.exists():
            logger.error(f"❌ Input directory does not exist: {input_dir}")
            return {}
        
        files = {
            'securities': None,
            'unitcost': None,
            'transactions': None
        }
        
        # Scan for HSBC files (support both .xlsx and .xls)
        for pattern in ["*.xlsx", "*.xls"]:
            for file in input_dir.glob(pattern):
                # Check if file matches HSBC pattern and date
                if not file.name.startswith('HSBC_'):
                    logger.debug(f"  Skipping non-HSBC file: {file.name}")
                    continue
                
                # Extract date from filename
                file_date = BankDetector.extract_date_from_filename(file.name)
                if file_date != date:
                    logger.debug(f"  Skipping file with different date: {file.name} (date: {file_date})")
                    continue
                
                # Determine file type
                if 'securities' in file.name.lower():
                    file_type = 'securities'
                elif 'unitcost' in file.name.lower():
                    file_type = 'unitcost'
                elif 'transactions' in file.name.lower():
                    file_type = 'transactions'
                else:
                    logger.warning(f"⚠️ Unknown file type: {file.name}")
                    continue
                
                files[file_type] = file
                logger.info(f"  ✅ Found {file_type} file: {file.name}")
        
        # Log summary and validate required files
        logger.info(f"📊 Discovery summary:")
        securities_status = "✅" if files['securities'] else "❌"
        unitcost_status = "✅" if files['unitcost'] else "❌"
        transactions_status = "✅" if files['transactions'] else "⚠️"
        
        logger.info(f"  📄 Securities: {securities_status}")
        logger.info(f"  📊 Unitcost: {unitcost_status}")
        logger.info(f"  💰 Transactions: {transactions_status}")
        
        # Check if we have required files (securities + unitcost)
        if not files['securities'] or not files['unitcost']:
            logger.error(f"❌ Missing required files (securities and/or unitcost)")
        
        return files
    
    def read_hsbc_file(self, file_path: Path, file_type: str) -> pd.DataFrame:
        """
        Read HSBC file with appropriate header detection.
        For unitcost files, uses outline-aware reading to only get summary rows.
        
        Args:
            file_path: Path to file
            file_type: Type of file ('securities', 'unitcost', 'transactions')
            
        Returns:
            DataFrame with file data
        """
        logger.debug(f"  📖 Reading {file_type} file: {file_path.name}")
        
        try:
            # Use appropriate header detection method
            if file_type == 'securities':
                header_row = HeaderDetector.find_hsbc_securities_header_row(file_path)
                expected_columns = HeaderDetector.HSBC_SECURITIES_KEY_COLUMNS
                use_outline_awareness = False
            elif file_type == 'unitcost':
                header_row = HeaderDetector.find_hsbc_unitcost_header_row(file_path)
                expected_columns = HeaderDetector.HSBC_UNITCOST_KEY_COLUMNS
                use_outline_awareness = True  # Use outline-aware reading for unitcost
            elif file_type == 'transactions':
                header_row = HeaderDetector.find_hsbc_transactions_header_row(file_path)
                expected_columns = HeaderDetector.HSBC_TRANSACTIONS_KEY_COLUMNS
                use_outline_awareness = False
            else:
                raise ValueError(f"Unknown file type: {file_type}")
            
            # Read the file with appropriate method
            if use_outline_awareness:
                df = HeaderDetector.read_excel_with_outline_awareness(
                    file_path, header_row, filter_summary_only=True
                )
            else:
                df = HeaderDetector.read_excel_with_fallback(file_path, header_row)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty {file_type} file: {file_path.name}")
                return pd.DataFrame()
            
            # Validate columns
            if HeaderDetector.validate_columns(df, expected_columns, min_match_ratio=0.5):
                logger.info(f"    ✅ Loaded {len(df)} {file_type} records (header row {header_row + 1})")
                if use_outline_awareness:
                    logger.info(f"    📋 Used outline-aware reading to get summary rows only")
                return df
            else:
                logger.warning(f"    ⚠️ Column validation failed for {file_type} file")
                return df  # Return anyway, let caller decide
                
        except Exception as e:
            logger.error(f"    ❌ Error reading {file_type} file {file_path.name}: {str(e)}")
            raise
    
    def merge_unit_cost_data(self, securities_df: pd.DataFrame, unitcost_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge unit cost data into securities DataFrame.
        Uses multiple matching strategies:
        1. Primary: Security ID <-> Security
        2. Fallback: Description <-> Symbol Description (normalized)
        3. Final fallback: Market Value = Total Cost
        
        Args:
            securities_df: Securities DataFrame
            unitcost_df: Unit cost DataFrame
            
        Returns:
            Enriched securities DataFrame with unit cost data
        """
        logger.info(f"  🔗 Merging unit cost data...")
        logger.info(f"    📊 Securities records: {len(securities_df)}")
        logger.info(f"    📊 Unit cost records: {len(unitcost_df)}")
        
        # Check required columns exist
        if 'Security ID' not in securities_df.columns:
            raise ValueError("Securities file missing 'Security ID' column")
        if 'Security' not in unitcost_df.columns:
            raise ValueError("Unit cost file missing 'Security' column")
        if 'Total Cost' not in unitcost_df.columns:
            raise ValueError("Unit cost file missing 'Total Cost' column")
        
        # Prepare unit cost data for merging
        merge_columns = ['Security', 'Total Cost']
        # Removed: Estimated Annual Income extraction (now using description-based coupon extraction)
        
        unitcost_subset = unitcost_df[merge_columns].copy()
        
        # STRATEGY 1: Primary merge on Security ID <-> Security
        logger.info(f"    🎯 Strategy 1: Matching on Security ID <-> Security")
        merged_df = securities_df.merge(
            unitcost_subset,
            left_on='Security ID',
            right_on='Security',
            how='left',
            suffixes=('', '_unitcost')
        )
        
        # Check initial matches
        primary_matched_mask = ~merged_df['Total Cost'].isna()
        primary_matches = primary_matched_mask.sum()
        logger.info(f"    ✅ Primary matches: {primary_matches}")
        
        # STRATEGY 2: Fallback merge on Description <-> Symbol Description (for unmatched)
        unmatched_mask = merged_df['Total Cost'].isna()
        unmatched_count = unmatched_mask.sum()
        
        if unmatched_count > 0 and 'Description' in securities_df.columns and 'Symbol Description' in unitcost_df.columns:
            logger.info(f"    🎯 Strategy 2: Fallback matching on Description for {unmatched_count} unmatched securities")
            
            # Get unmatched securities
            unmatched_securities = merged_df[unmatched_mask].copy()
            
            # Normalize descriptions for matching (strip whitespace, convert to uppercase)
            unitcost_descriptions = unitcost_df.copy()
            unitcost_descriptions['normalized_description'] = unitcost_descriptions['Symbol Description'].str.strip().str.upper()
            
            fallback_matches = 0
            for idx in unmatched_securities.index:
                sec_desc = str(merged_df.loc[idx, 'Description']).strip().upper()
                
                # Find matching unitcost by description
                matching_unitcost = unitcost_descriptions[
                    unitcost_descriptions['normalized_description'] == sec_desc
                ]
                
                if len(matching_unitcost) > 0:
                    # Take first match if multiple
                    match = matching_unitcost.iloc[0]
                    merged_df.loc[idx, 'Total Cost'] = match['Total Cost']
                    # Note: No longer extracting Estimated Annual Income
                    fallback_matches += 1
                    logger.debug(f"    📍 Description match: '{sec_desc[:50]}...' -> Security: {match['Security']}")
            
            logger.info(f"    ✅ Fallback matches: {fallback_matches}")
        
        # STRATEGY 3: Final fallback - use Market Value as Total Cost
        final_unmatched_mask = merged_df['Total Cost'].isna()
        final_unmatched_count = final_unmatched_mask.sum()
        
        if final_unmatched_count > 0:
            logger.info(f"    🎯 Strategy 3: Using Market Value as Total Cost for {final_unmatched_count} remaining unmatched securities")
            
            if 'Market Value' in merged_df.columns:
                merged_df.loc[final_unmatched_mask, 'Total Cost'] = merged_df.loc[final_unmatched_mask, 'Market Value']
                logger.info(f"    💡 Applied Market Value fallback for final unmatched securities")
            else:
                logger.warning(f"    ⚠️ No 'Market Value' column found for final fallback")
        
        # Remove the duplicate 'Security' column from unitcost
        if 'Security' in merged_df.columns:
            merged_df = merged_df.drop('Security', axis=1)
        
        # Summary
        total_matched = len(merged_df) - final_unmatched_count
        logger.info(f"    📊 Matching summary:")
        logger.info(f"      • Primary (Security ID): {primary_matches}")
        logger.info(f"      • Fallback (Description): {fallback_matches if unmatched_count > 0 and 'Description' in securities_df.columns else 0}")
        logger.info(f"      • Market Value fallback: {unmatched_count - (fallback_matches if unmatched_count > 0 and 'Description' in securities_df.columns else 0)}")
        logger.info(f"    ✅ Total matched: {total_matched} / {len(merged_df)}")
        logger.info(f"    📊 Final enriched records: {len(merged_df)}")
        
        return merged_df
    
    def extract_maturity_date_from_description(self, description: str) -> Optional[str]:
        """
        Extract the FURTHEST date (maturity) from HSBC bond description.
        
        Logic: 
        - Find ALL MM/DD/YY dates in description
        - Convert to comparable format (assume 20YY)  
        - Return the MAXIMUM/FURTHEST date as maturity
        
        Args:
            description: Bond description containing dates
            
        Returns:
            Maturity date in MM/DD/YYYY format, or None if not found
        """
        if pd.isna(description) or not description:
            return None
        
        # Regex to find all MM/DD/YY dates (flexible pattern)
        date_pattern = r'\d{1,2}/\d{1,2}/\d{2}'
        matches = re.findall(date_pattern, str(description).strip())
        
        if len(matches) == 0:
            return None
        
        # Convert all dates to comparable format and find the maximum
        parsed_dates = []
        for date_str in matches:
            parts = date_str.split('/')
            if len(parts) == 3:
                month, day, year = parts
                # Convert YY to 20YY for comparison
                full_year = f"20{year.zfill(2)}"
                # Create comparable date tuple (year, month, day) for easy comparison
                comparable_date = (int(full_year), int(month), int(day))
                formatted_date = f"{month.zfill(2)}/{day.zfill(2)}/{full_year}"
                parsed_dates.append((comparable_date, formatted_date))
        
        if not parsed_dates:
            return None
        
        # Find the maximum date (furthest in the future)
        max_date = max(parsed_dates, key=lambda x: x[0])
        return max_date[1]  # Return the formatted date MM/DD/YYYY
    
    def is_hsbc_bond(self, description: str) -> bool:
        """
        Check if asset is an HSBC bond based on Description pattern.
        
        HSBC Logic: Bonds have MM/DD/YY date patterns in description
        
        Args:
            description: Description from securities file
            
        Returns:
            True if bond, False otherwise  
        """
        if pd.isna(description) or not description:
            return False
        
        # Check for date pattern MM/DD/YY in description (same logic as HSBC transformer)
        date_pattern = r'\d{1,2}/\d{1,2}/\d{2}'
        return bool(re.search(date_pattern, str(description).strip()))
    
    def add_maturity_dates(self, securities_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add maturity dates to securities DataFrame for bond assets.
        
        Args:
            securities_df: Securities DataFrame with Description column
            
        Returns:
            DataFrame with maturity_date column populated for bonds
        """
        logger.info(f"  📅 Adding maturity dates for bond securities...")
        
        # Initialize maturity_date column
        securities_df = securities_df.copy()
        securities_df['maturity_date'] = None
        
        bond_count = 0
        maturity_extracted_count = 0
        
        for idx, row in securities_df.iterrows():
            # Check if this is a bond using HSBC logic (date pattern in description)
            if self.is_hsbc_bond(row.get('Description')):
                bond_count += 1
                # Extract maturity date from description
                maturity_date = self.extract_maturity_date_from_description(row.get('Description'))
                if maturity_date:
                    securities_df.loc[idx, 'maturity_date'] = maturity_date
                    maturity_extracted_count += 1
                    logger.debug(f"    📅 Extracted maturity: {row.get('Description')[:50]}... → {maturity_date}")
        
        logger.info(f"    ✅ Found {bond_count} bonds, extracted {maturity_extracted_count} maturity dates")
        return securities_df
    
    def add_bank_client_account_columns(self, df: pd.DataFrame, account_column: str, mappings: Dict[str, str]) -> pd.DataFrame:
        """
        Add bank, client, and account columns to DataFrame using account mappings.
        
        Args:
            df: DataFrame to enrich
            account_column: Name of the account column ('Account Number' or 'Account')
            mappings: Dict mapping account numbers to account names
            
        Returns:
            DataFrame with added bank/client/account columns
        """
        logger.info(f"  🏷️ Adding bank/client/account columns...")
        
        if account_column not in df.columns:
            raise ValueError(f"DataFrame missing '{account_column}' column")
        
        # Add bank and client columns (constant for HSBC)
        df = df.copy()
        df['bank'] = 'HSBC'
        df['client'] = 'BK'
        
        # Map account numbers to account names
        def map_account(account_number):
            account_str = str(account_number).strip()
            if account_str in mappings:
                return mappings[account_str]
            else:
                logger.warning(f"    ⚠️ No mapping found for account: {account_str}")
                return account_str  # Use original if no mapping found
        
        df['account'] = df[account_column].apply(map_account)
        
        # Count mapped accounts
        mapped_accounts = df['account'].value_counts()
        logger.info(f"    ✅ Mapped accounts: {dict(mapped_accounts)}")
        
        return df
    
    def enrich_hsbc_files(self, input_dir: Path, output_dir: Path, date: str, 
                         mappings_file: str, dry_run: bool = False) -> bool:
        """
        Main method to enrich HSBC files for a specific date.
        
        Args:
            input_dir: Directory containing raw HSBC files
            output_dir: Directory for enriched output files
            date: Date string in DD_MM_YYYY format
            mappings_file: Path to Mappings.xlsx file
            dry_run: If True, show what would be processed without actually processing
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting HSBC data enrichment for date: {date}")
        logger.info(f"📁 Input directory: {input_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        logger.info(f"🗂️ Mappings file: {mappings_file}")
        
        # Load account mappings
        try:
            mappings = self.load_account_mappings(mappings_file)
        except Exception as e:
            logger.error(f"❌ Failed to load account mappings: {e}")
            return False
        
        # Discover files
        files = self.discover_hsbc_files(input_dir, date)
        
        if not files or not files['securities'] or not files['unitcost']:
            logger.error(f"❌ Missing required HSBC files for date {date}")
            return False
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be processed")
            logger.info("📋 Would process the following files:")
            for file_type, file_path in files.items():
                if file_path:
                    logger.info(f"  {file_type}: {file_path.name}")
                else:
                    logger.info(f"  {file_type}: None")
            logger.info(f"📋 Account mappings: {mappings}")
            return True
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Read securities file
            logger.info(f"🔄 Processing securities file...")
            securities_df = self.read_hsbc_file(files['securities'], 'securities')
            if securities_df.empty:
                logger.error(f"❌ Empty or invalid securities file")
                return False
            
            # Read unit cost file
            logger.info(f"🔄 Processing unitcost file...")
            unitcost_df = self.read_hsbc_file(files['unitcost'], 'unitcost')
            if unitcost_df.empty:
                logger.error(f"❌ Empty or invalid unitcost file")
                return False
            
            # Merge unit cost data into securities
            logger.info(f"🔄 Enriching securities with unit cost data...")
            enriched_securities = self.merge_unit_cost_data(securities_df, unitcost_df)
            
            # Extract maturity dates for bonds
            enriched_securities = self.add_maturity_dates(enriched_securities)
            
            # Add bank/client/account columns to securities
            logger.info(f"🔄 Adding bank/client/account columns to securities...")
            enriched_securities = self.add_bank_client_account_columns(
                enriched_securities, 'Account Number', mappings
            )
            
            # Save enriched securities file
            securities_output = output_dir / f"HSBC_securities_{date}.xlsx"
            enriched_securities.to_excel(securities_output, index=False)
            logger.info(f"💾 Saved enriched securities: {securities_output.name}")
            logger.info(f"  📊 Total records: {len(enriched_securities)}")
            
            # Process transactions file if it exists
            if files['transactions']:
                logger.info(f"🔄 Processing transactions file...")
                transactions_df = self.read_hsbc_file(files['transactions'], 'transactions')
                
                if not transactions_df.empty:
                    # Add bank/client/account columns to transactions
                    logger.info(f"🔄 Adding bank/client/account columns to transactions...")
                    enriched_transactions = self.add_bank_client_account_columns(
                        transactions_df, 'Account', mappings
                    )
                    
                    # Save enriched transactions file
                    transactions_output = output_dir / f"HSBC_transactions_{date}.xlsx"
                    enriched_transactions.to_excel(transactions_output, index=False)
                    logger.info(f"💾 Saved enriched transactions: {transactions_output.name}")
                    logger.info(f"  💰 Total records: {len(enriched_transactions)}")
                else:
                    logger.warning(f"⚠️ Empty transactions file")
            else:
                logger.info(f"📋 No transactions file found")
            
            # Final summary
            logger.info(f"🎉 HSBC data enrichment completed successfully!")
            logger.info(f"  📁 Output files saved in: {output_dir}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during HSBC enrichment: {str(e)}")
            return False 