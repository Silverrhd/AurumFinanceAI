#!/usr/bin/env python3
"""
CS (Credit Suisse) File Combiner

Combines individual CS client files with dynamic header detection for securities files and fixed header for transactions files.
Adds bank, client, and account columns extracted from filenames.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.bank_detector import BankDetector
from preprocessing.combiners.header_detector import HeaderDetector

logger = logging.getLogger(__name__)


class CSCombiner:
    """Combines individual CS client files into unified bank files."""
    
    def __init__(self):
        """Initialize the CS combiner."""
        self.bank_code = 'CS'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
    
    def discover_cs_files(self, cs_dir: Path, date: str) -> Dict[str, List[Dict]]:
        """
        Discover CS files for a specific date.
        
        Args:
            cs_dir: Directory containing individual CS files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict with 'securities' and 'transactions' lists containing file info
        """
        logger.info(f"🔍 Scanning for CS files in: {cs_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not cs_dir.exists():
            logger.error(f"❌ CS directory does not exist: {cs_dir}")
            return {'securities': [], 'transactions': []}
        
        discovered_files = {'securities': [], 'transactions': []}
        clients_found = set()
        
        # Scan for CS files (support both .xlsx and .xls)
        for pattern in ["*.xlsx", "*.xls"]:
            for file in cs_dir.glob(pattern):
                # Check if file matches CS pattern and date
                if not file.name.startswith('CS_'):
                    logger.debug(f"  Skipping non-CS file: {file.name}")
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
                clients_found.add(f"{client}_{account}")
                
                # Determine file type
                if 'securities' in file.name.lower():
                    file_type = 'securities'
                elif 'transactions' in file.name.lower():
                    file_type = 'transactions'
                else:
                    logger.warning(f"⚠️ Unknown file type: {file.name}")
                    continue
                
                file_info = {
                    'file': file,
                    'bank': bank,
                    'client': client,
                    'account': account,
                    'type': file_type
                }
                
                discovered_files[file_type].append(file_info)
                logger.info(f"  ✅ Found {file_type} file: {client}_{account} -> {file.name}")
        
        # Log summary
        securities_count = len(discovered_files['securities'])
        transactions_count = len(discovered_files['transactions'])
        logger.info(f"📊 Discovery summary:")
        logger.info(f"  📄 Securities files: {securities_count}")
        logger.info(f"  💰 Transactions files: {transactions_count}")
        logger.info(f"  👥 Unique clients/accounts: {len(clients_found)}")
        
        # Check for missing transactions files
        securities_clients = {f"{f['client']}_{f['account']}" for f in discovered_files['securities']}
        transactions_clients = {f"{f['client']}_{f['account']}" for f in discovered_files['transactions']}
        
        missing_transactions = securities_clients - transactions_clients
        if missing_transactions:
            logger.warning(f"⚠️ Clients missing transactions files: {', '.join(sorted(missing_transactions))}")
        
        missing_securities = transactions_clients - securities_clients
        if missing_securities:
            logger.warning(f"⚠️ Clients missing securities files: {', '.join(sorted(missing_securities))}")
        
        return discovered_files
    
    def process_securities_file_with_fallback(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process securities file with multiple fallback strategies.
        
        Args:
            file_path: Path to securities file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
            
        Raises:
            ValueError: If all strategies fail
        """
        logger.info(f"  📄 Processing securities: {client}_{account} -> {file_path.name}")
        
        # Define fallback strategies
        strategies = [
            # Strategy 1: Dynamic header detection
            lambda: HeaderDetector.find_securities_header_row(file_path),
            
            # Strategy 2: Client-specific defaults
            lambda: 7 if client in ['HS', 'VLP', 'LP'] else 8,  # Row 8 or 9 (0-indexed = 7 or 8)
            
            # Strategy 3: Common positions
            lambda: 0,  # Standard header
            lambda: 1,  # One row offset
            lambda: 2,  # Two row offset
        ]
        
        strategy_names = [
            "Dynamic detection",
            "Client default",
            "Standard header (row 1)",
            "Row 2 header",
            "Row 3 header"
        ]
        
        for i, (strategy, name) in enumerate(zip(strategies, strategy_names)):
            try:
                logger.debug(f"    🔄 Trying strategy {i+1}: {name}")
                header_row = strategy()
                
                # Read the file with detected header row
                df = HeaderDetector.read_excel_with_fallback(file_path, header_row)
                
                if df.empty:
                    logger.debug(f"    ⚠️ Strategy {i+1} returned empty DataFrame")
                    continue
                
                # Filter out disclaimer rows before validation
                df = self.filter_securities_data(df)
                
                if df.empty:
                    logger.debug(f"    ⚠️ Strategy {i+1} returned empty DataFrame after filtering")
                    continue
                
                # Basic validation - check if we have reasonable number of columns
                if len(df.columns) < 3:
                    logger.debug(f"    ⚠️ Strategy {i+1} returned too few columns ({len(df.columns)})")
                    continue
                
                # Additional validation for securities files
                if HeaderDetector.validate_columns(df, HeaderDetector.SECURITIES_KEY_COLUMNS, min_match_ratio=0.3):
                    logger.info(f"    ✅ Strategy {i+1} succeeded: {name} (header row {header_row + 1})")
                    logger.info(f"    📊 Loaded {len(df)} records with {len(df.columns)} columns")
                    return df
                else:
                    logger.debug(f"    ⚠️ Strategy {i+1} failed column validation")
                    continue
                    
            except Exception as e:
                logger.debug(f"    ❌ Strategy {i+1} failed: {str(e)}")
                continue
        
        # If we get here, all strategies failed
        raise ValueError(f"All strategies failed for securities file: {file_path}")
    
    def process_transactions_file(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process transactions file with dynamic header detection.
        
        Args:
            file_path: Path to transactions file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
        """
        logger.info(f"  💰 Processing transactions: {client}_{account} -> {file_path.name}")
        
        try:
            # Use dynamic header detection for CS transactions files
            header_row = HeaderDetector.find_transactions_header_row(file_path)
            df = HeaderDetector.read_excel_with_fallback(file_path, header_row=header_row)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty transactions file: {file_path.name}")
                return pd.DataFrame()
            
            logger.info(f"    ✅ Loaded {len(df)} transactions with {len(df.columns)} columns (header row: {header_row})")
            return df
            
        except Exception as e:
            logger.error(f"    ❌ Error processing transactions file {file_path.name}: {str(e)}")
            raise
    
    def combine_securities_files(self, files: List[Dict], output_path: Path) -> bool:
        """
        Combine individual securities files into unified file.
        
        Args:
            files: List of file info dictionaries
            output_path: Path for output file
            
        Returns:
            True if successful, False otherwise
        """
        if not files:
            logger.warning("⚠️ No securities files to combine")
            return False
        
        logger.info(f"🔗 Combining {len(files)} securities files...")
        
        combined_data = []
        successful_files = 0
        failed_files = 0
        
        for file_info in files:
            file_path = file_info['file']
            bank = file_info['bank']
            client = file_info['client']
            account = file_info['account']
            
            try:
                # Process with fallback strategies
                df = self.process_securities_file_with_fallback(file_path, client, account)
                
                if df.empty:
                    logger.warning(f"  ⚠️ Empty file: {file_path.name}")
                    continue
                
                # Add bank, client, account columns at the beginning
                df.insert(0, 'bank', bank)
                df.insert(1, 'client', client)
                df.insert(2, 'account', account)
                
                combined_data.append(df)
                successful_files += 1
                logger.info(f"  ✅ Added {len(df)} records from {client}_{account}")
                
            except Exception as e:
                logger.error(f"  ❌ Error processing {file_path.name}: {str(e)}")
                logger.error(f"  💡 Skipping corrupted file and continuing...")
                failed_files += 1
                continue
        
        if not combined_data:
            logger.error("❌ No valid securities data to combine")
            return False
        
        try:
            # Combine all DataFrames
            final_df = pd.concat(combined_data, ignore_index=True)
            
            # Save to output file
            final_df.to_excel(output_path, index=False)
            
            logger.info(f"✅ Securities combination completed!")
            logger.info(f"  📊 Total records: {len(final_df)}")
            logger.info(f"  ✅ Successful files: {successful_files}")
            if failed_files > 0:
                logger.warning(f"  ❌ Failed files: {failed_files}")
            logger.info(f"  💾 Saved to: {output_path.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving combined securities file: {str(e)}")
            return False
    
    def combine_transactions_files(self, files: List[Dict], output_path: Path) -> bool:
        """
        Combine individual transactions files into unified file.
        
        Args:
            files: List of file info dictionaries
            output_path: Path for output file
            
        Returns:
            True if successful, False otherwise
        """
        if not files:
            logger.warning("⚠️ No transactions files to combine")
            return False
        
        logger.info(f"🔗 Combining {len(files)} transactions files...")
        
        combined_data = []
        successful_files = 0
        failed_files = 0
        
        for file_info in files:
            file_path = file_info['file']
            bank = file_info['bank']
            client = file_info['client']
            account = file_info['account']
            
            try:
                # Process transactions file
                df = self.process_transactions_file(file_path, client, account)
                
                if df.empty:
                    logger.warning(f"  ⚠️ Empty file: {file_path.name}")
                    continue
                
                # Add bank, client, account columns at the beginning
                df.insert(0, 'bank', bank)
                df.insert(1, 'client', client)
                df.insert(2, 'account', account)
                
                combined_data.append(df)
                successful_files += 1
                logger.info(f"  ✅ Added {len(df)} records from {client}_{account}")
                
            except Exception as e:
                logger.error(f"  ❌ Error processing {file_path.name}: {str(e)}")
                logger.error(f"  💡 Skipping corrupted file and continuing...")
                failed_files += 1
                continue
        
        if not combined_data:
            logger.error("❌ No valid transactions data to combine")
            return False
        
        try:
            # Combine all DataFrames
            final_df = pd.concat(combined_data, ignore_index=True)
            
            # Save to output file
            final_df.to_excel(output_path, index=False)
            
            logger.info(f"✅ Transactions combination completed!")
            logger.info(f"  📊 Total records: {len(final_df)}")
            logger.info(f"  ✅ Successful files: {successful_files}")
            if failed_files > 0:
                logger.warning(f"  ❌ Failed files: {failed_files}")
            logger.info(f"  💾 Saved to: {output_path.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving combined transactions file: {str(e)}")
            return False
    
    def combine_all_files(self, cs_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Main method to combine all CS files for a specific date.
        
        Args:
            cs_dir: Directory containing individual CS files
            output_dir: Directory for output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without actually processing
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting CS file combination for date: {date}")
        logger.info(f"📁 Input directory: {cs_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        
        # Discover files
        discovered_files = self.discover_cs_files(cs_dir, date)
        
        if not discovered_files['securities'] and not discovered_files['transactions']:
            logger.error(f"❌ No CS files found for date {date}")
            return False
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be combined")
            logger.info("📋 Would process the following:")
            for file_info in discovered_files['securities']:
                logger.info(f"  📄 Securities: {file_info['client']}_{file_info['account']}")
            for file_info in discovered_files['transactions']:
                logger.info(f"  💰 Transactions: {file_info['client']}_{file_info['account']}")
            return True
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define output file paths
        securities_output = output_dir / f"CS_securities_{date}.xlsx"
        transactions_output = output_dir / f"CS_transactions_{date}.xlsx"
        
        success = True
        
        # Combine securities files
        if discovered_files['securities']:
            if not self.combine_securities_files(discovered_files['securities'], securities_output):
                success = False
        else:
            logger.warning("⚠️ No securities files found to combine")
        
        # Combine transactions files
        if discovered_files['transactions']:
            if not self.combine_transactions_files(discovered_files['transactions'], transactions_output):
                success = False
        else:
            logger.warning("⚠️ No transactions files found to combine")
        
        if success:
            logger.info("🎉 CS file combination completed successfully!")
        else:
            logger.error("❌ CS file combination completed with errors")
        
        return success
    
    def should_exclude_row(self, row_data: pd.Series, row_index: int, total_rows: int) -> bool:
        """
        Determine if a row should be excluded from the combined file.
        
        Args:
            row_data: Pandas Series representing a row
            row_index: Index of the row (0-based)
            total_rows: Total number of rows in the DataFrame
            
        Returns:
            True if row should be excluded, False otherwise
        """
        # Skip completely empty rows
        if row_data.isna().all():
            return True
        
        # Check for disclaimer text patterns
        if self._is_disclaimer_row(row_data):
            return True
        
        # Check for merged cell patterns (likely disclaimers)
        if self._is_merged_row(row_data):
            return True
        
        # Check if row is near the end and looks suspicious
        if row_index >= total_rows * 0.9:  # Last 10% of rows
            # More aggressive filtering near the end
            row_text = ' '.join(str(val).lower() for val in row_data if pd.notna(val) and str(val).strip())
            if len(row_text) > 50 and any(word in row_text for word in ['notice', 'information', 'data', 'availability', 'report', 'disclaimer']):
                return True
        
        return False
    
    def _is_disclaimer_row(self, row_data: pd.Series) -> bool:
        """
        Detect if a row contains disclaimer text.
        
        Args:
            row_data: Pandas Series representing a row
            
        Returns:
            True if row appears to be a disclaimer, False otherwise
        """
        # Convert all values to string and join them (case-insensitive)
        row_text = ' '.join(str(val).lower() for val in row_data if pd.notna(val) and str(val).strip())
        
        # Check for disclaimer keywords (case-insensitive)
        disclaimer_keywords = [
            'real time data is shown upon availability',
            'transactions might not be booked real time',
            'cash accounts',
            'disclaimer',
            'important notice',
            'this report',
            'information contained',
            'accuracy of data',
            'data is provided',
            'not be liable',
            'terms and conditions'
        ]
        
        # If row contains disclaimer keywords, it's likely a disclaimer
        return any(keyword in row_text for keyword in disclaimer_keywords)
    
    def _is_merged_row(self, row_data: pd.Series) -> bool:
        """
        Detect if a row appears to be a merged cell (disclaimer spanning multiple columns).
        
        Args:
            row_data: Pandas Series representing a row
            
        Returns:
            True if row appears to be merged/spanning, False otherwise
        """
        # Count non-empty cells with actual content
        non_empty_cells = sum(1 for val in row_data if pd.notna(val) and str(val).strip())
        
        # If only 1-2 cells have content but row has many columns, likely merged
        # Also check if the content is unusually long (typical of disclaimers)
        if non_empty_cells <= 2 and len(row_data) > 5:
            # Check if any cell has unusually long text (likely disclaimer)
            for val in row_data:
                if pd.notna(val) and len(str(val).strip()) > 100:
                    return True
        
        return False
    
    def filter_securities_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out disclaimer and footer rows from securities data.
        
        Args:
            df: Raw securities DataFrame
            
        Returns:
            Filtered DataFrame without disclaimer rows
        """
        if df.empty:
            return df
        
        logger.debug(f"  🧹 Filtering disclaimer rows from {len(df)} total rows...")
        
        try:
            # Track which rows to keep
            rows_to_keep = []
            excluded_rows = []
            
            for idx, row in df.iterrows():
                if self.should_exclude_row(row, idx, len(df)):
                    excluded_rows.append(idx)
                    # Log the first 50 characters of the excluded row for debugging
                    first_non_empty = next((str(val)[:50] for val in row if pd.notna(val) and str(val).strip()), "Empty row")
                    logger.debug(f"    ❌ Excluding row {idx + 1}: {first_non_empty}...")
                else:
                    rows_to_keep.append(idx)
            
            # Filter the DataFrame
            filtered_df = df.loc[rows_to_keep].reset_index(drop=True)
            
            # Log summary
            excluded_count = len(excluded_rows)
            if excluded_count > 0:
                logger.info(f"    🧹 Filtered out {excluded_count} disclaimer/footer rows")
                logger.info(f"    📊 Kept {len(filtered_df)} data rows")
            else:
                logger.debug(f"    ✅ No disclaimer rows found to filter")
            
            return filtered_df
            
        except Exception as e:
            logger.warning(f"    ⚠️ Error during disclaimer filtering: {str(e)}")
            logger.warning(f"    💡 Falling back to include all rows")
            return df 