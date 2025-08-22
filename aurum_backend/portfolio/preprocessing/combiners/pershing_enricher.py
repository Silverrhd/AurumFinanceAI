#!/usr/bin/env python3
"""
Pershing Data Enricher

Enriches Pershing securities files with unit cost data.
Merges 'Total Cost' from unitcost files into securities files.
Note: No longer extracts Estimated Annual Income (using description-based coupon extraction instead).
"""

import os
import sys
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.bank_detector import BankDetector
from preprocessing.combiners.header_detector import HeaderDetector

logger = logging.getLogger(__name__)


class PershingEnricher:
    """Enriches Pershing securities files with unit cost data."""
    
    def __init__(self):
        """Initialize the Pershing enricher."""
        self.bank_code = 'Pershing'
        logger.info(f"🏦 Initialized {self.bank_code} data enricher")
    
    def discover_pershing_files(self, input_dir: Path, date: str) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        Discover Pershing files for a specific date and group by client/account.
        
        Args:
            input_dir: Directory containing individual Pershing files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping client_account to file paths:
            {
                'LP_MT': {
                    'securities': Path(...),
                    'unitcost': Path(...),
                    'transactions': Path(...) or None
                }
            }
        """
        logger.info(f"🔍 Scanning for Pershing files in: {input_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not input_dir.exists():
            logger.error(f"❌ Input directory does not exist: {input_dir}")
            return {}
        
        client_files = {}
        
        # Scan for Pershing files (support both .xlsx and .xls)
        for pattern in ["*.xlsx", "*.xls"]:
            for file in input_dir.glob(pattern):
                # Check if file matches Pershing pattern and date
                if not file.name.startswith('Pershing_'):
                    logger.debug(f"  Skipping non-Pershing file: {file.name}")
                    continue
                
                # Extract date from filename
                file_date = BankDetector.extract_date_from_filename(file.name)
                if file_date != date:
                    logger.debug(f"  Skipping file with different date: {file.name} (date: {file_date})")
                    continue
                
                # Extract bank, client, account
                extraction = BankDetector.extract_client_account_from_filename(file.name)
                if not extraction:
                    logger.warning(f"⚠️ Could not extract client/account from: {file.name}")
                    continue
                
                bank, client, account = extraction
                client_key = f"{client}_{account}"
                
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
                
                # Initialize client entry if not exists
                if client_key not in client_files:
                    client_files[client_key] = {
                        'securities': None,
                        'unitcost': None,
                        'transactions': None
                    }
                
                client_files[client_key][file_type] = file
                logger.info(f"  ✅ Found {file_type} file: {client_key} -> {file.name}")
        
        # Log summary and validate required files
        logger.info(f"📊 Discovery summary:")
        valid_clients = 0
        
        for client_key, files in client_files.items():
            securities_status = "✅" if files['securities'] else "❌"
            unitcost_status = "✅" if files['unitcost'] else "❌"
            transactions_status = "✅" if files['transactions'] else "⚠️"
            
            logger.info(f"  👤 {client_key}: securities {securities_status}, unitcost {unitcost_status}, transactions {transactions_status}")
            
            # Check if we have required files (securities + unitcost)
            if files['securities'] and files['unitcost']:
                valid_clients += 1
            else:
                logger.warning(f"  ⚠️ {client_key} missing required files (securities and/or unitcost)")
        
        logger.info(f"  📊 Valid client sets: {valid_clients}/{len(client_files)}")
        
        return client_files
    
    def read_pershing_file(self, file_path: Path, file_type: str) -> pd.DataFrame:
        """
        Read Pershing file with appropriate header detection.
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
                header_row = HeaderDetector.find_pershing_securities_header_row(file_path)
                expected_columns = HeaderDetector.PERSHING_SECURITIES_KEY_COLUMNS
                use_outline_awareness = False
            elif file_type == 'unitcost':
                header_row = HeaderDetector.find_pershing_unitcost_header_row(file_path)
                expected_columns = HeaderDetector.PERSHING_UNITCOST_KEY_COLUMNS
                use_outline_awareness = True  # Use outline-aware reading for unitcost
            elif file_type == 'transactions':
                header_row = HeaderDetector.find_pershing_transactions_header_row(file_path)
                expected_columns = HeaderDetector.PERSHING_TRANSACTIONS_KEY_COLUMNS
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
        
        # Perform left join to keep all securities
        merged_df = securities_df.merge(
            unitcost_subset,
            left_on='Security ID',
            right_on='Security',
            how='left',
            suffixes=('', '_unitcost')
        )
        
        # Handle unmatched securities (cash, etc.)
        unmatched_mask = merged_df['Total Cost'].isna()
        unmatched_count = unmatched_mask.sum()
        
        if unmatched_count > 0:
            logger.info(f"    ⚠️ {unmatched_count} securities without unit cost data")
            
            # Set Total Cost = Market Value for unmatched securities
            if 'Market Value' in merged_df.columns:
                merged_df.loc[unmatched_mask, 'Total Cost'] = merged_df.loc[unmatched_mask, 'Market Value']
                logger.info(f"    💡 Using Market Value as Total Cost for unmatched securities")
            else:
                logger.warning(f"    ⚠️ No 'Market Value' column found for fallback")
            
            # Note: No longer extracting Estimated Annual Income (using description-based extraction instead)
        
        # Remove the duplicate 'Security' column from unitcost
        if 'Security' in merged_df.columns:
            merged_df = merged_df.drop('Security', axis=1)
        
        matched_count = len(merged_df) - unmatched_count
        logger.info(f"    ✅ Matched {matched_count} securities with unit cost data")
        logger.info(f"    📊 Final enriched records: {len(merged_df)}")
        
        return merged_df
    
    def extract_maturity_date_from_description(self, description: str) -> Optional[str]:
        """
        Extract the FURTHEST date (maturity) from bond description.
        
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
        matches = re.findall(date_pattern, str(description))
        
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
    
    def is_pershing_bond(self, sub_asset_classification: str) -> bool:
        """
        Check if asset is a Pershing bond based on Sub-Asset Classification.
        
        Args:
            sub_asset_classification: Sub-Asset Classification from securities file
            
        Returns:
            True if bond, False otherwise  
        """
        if pd.isna(sub_asset_classification):
            return False
        
        bond_types = ['Corporate Bonds', 'Sovereign Debt', 'U.S. Treasury Securities']
        return str(sub_asset_classification).strip() in bond_types
    
    def add_maturity_dates(self, securities_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add maturity dates to securities DataFrame for bond assets.
        
        Args:
            securities_df: Securities DataFrame with Description and Sub-Asset Classification
            
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
            # Check if this is a bond using Pershing logic
            if self.is_pershing_bond(row.get('Sub-Asset Classification')):
                bond_count += 1
                # Extract maturity date from description
                maturity_date = self.extract_maturity_date_from_description(row.get('Description'))
                if maturity_date:
                    securities_df.loc[idx, 'maturity_date'] = maturity_date
                    maturity_extracted_count += 1
                    logger.debug(f"    📅 Extracted maturity: {row.get('Description')[:50]}... → {maturity_date}")
        
        logger.info(f"    ✅ Found {bond_count} bonds, extracted {maturity_extracted_count} maturity dates")
        return securities_df
    
    def process_client_files(self, client_key: str, files: Dict[str, Optional[Path]], 
                           output_dir: Path, date: str) -> bool:
        """
        Process files for a single client/account.
        
        Args:
            client_key: Client identifier (e.g., 'LP_MT')
            files: Dict of file paths
            output_dir: Output directory
            date: Date string
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🔄 Processing client: {client_key}")
        
        try:
            # Check required files
            if not files['securities'] or not files['unitcost']:
                logger.error(f"  ❌ Missing required files for {client_key}")
                return False
            
            # Read securities file
            securities_df = self.read_pershing_file(files['securities'], 'securities')
            if securities_df.empty:
                logger.error(f"  ❌ Empty or invalid securities file for {client_key}")
                return False
            
            # Read unit cost file
            unitcost_df = self.read_pershing_file(files['unitcost'], 'unitcost')
            if unitcost_df.empty:
                logger.error(f"  ❌ Empty or invalid unit cost file for {client_key}")
                return False
            
            # Merge unit cost data into securities
            enriched_securities = self.merge_unit_cost_data(securities_df, unitcost_df)
            
            # Extract maturity dates for bonds
            enriched_securities = self.add_maturity_dates(enriched_securities)
            
            # Save enriched securities file
            securities_output = output_dir / f"Pershing_{client_key}_securities_{date}.xlsx"
            enriched_securities.to_excel(securities_output, index=False)
            logger.info(f"  💾 Saved enriched securities: {securities_output.name}")
            
            # Copy transactions file if it exists
            if files['transactions']:
                transactions_output = output_dir / f"Pershing_{client_key}_transactions_{date}.xlsx"
                
                # Read and save transactions (passthrough)
                transactions_df = self.read_pershing_file(files['transactions'], 'transactions')
                if not transactions_df.empty:
                    transactions_df.to_excel(transactions_output, index=False)
                    logger.info(f"  📋 Copied transactions: {transactions_output.name}")
                else:
                    logger.warning(f"  ⚠️ Empty transactions file for {client_key}")
            else:
                logger.info(f"  📋 No transactions file for {client_key}")
            
            logger.info(f"  ✅ Successfully processed {client_key}")
            return True
            
        except Exception as e:
            logger.error(f"  ❌ Error processing {client_key}: {str(e)}")
            return False
    
    def enrich_all_clients(self, input_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Main method to enrich all Pershing client files for a specific date.
        
        Args:
            input_dir: Directory containing raw Pershing files
            output_dir: Directory for enriched output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without actually processing
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting Pershing data enrichment for date: {date}")
        logger.info(f"📁 Input directory: {input_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        
        # Discover files
        client_files = self.discover_pershing_files(input_dir, date)
        
        if not client_files:
            logger.error(f"❌ No Pershing files found for date {date}")
            return False
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be processed")
            logger.info("📋 Would process the following clients:")
            for client_key, files in client_files.items():
                if files['securities'] and files['unitcost']:
                    status = "✅ Ready"
                else:
                    status = "❌ Missing required files"
                logger.info(f"  👤 {client_key}: {status}")
            return True
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each client
        successful_clients = 0
        failed_clients = 0
        
        for client_key, files in client_files.items():
            if files['securities'] and files['unitcost']:
                if self.process_client_files(client_key, files, output_dir, date):
                    successful_clients += 1
                else:
                    failed_clients += 1
            else:
                logger.warning(f"⚠️ Skipping {client_key} - missing required files")
                failed_clients += 1
        
        # Final summary
        logger.info(f"🎉 Pershing data enrichment completed!")
        logger.info(f"  ✅ Successful clients: {successful_clients}")
        if failed_clients > 0:
            logger.warning(f"  ❌ Failed clients: {failed_clients}")
        logger.info(f"  📁 Output files saved in: {output_dir}")
        
        return successful_clients > 0 