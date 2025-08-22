#!/usr/bin/env python3
"""
JB (Julius Baer) File Combiner

Combines individual JB client files into unified bank files.
Supports two format types:
- GZ format: Full column set, securities header=4, transactions header=3 + skip 2 empty rows
- HS format: Limited columns, securities header=0, no transactions
Footer detection: Ignore anything after 2+ consecutive empty rows in securities files
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

logger = logging.getLogger(__name__)


class JBCombiner:
    """Combines individual JB client files into unified bank files."""
    
    def __init__(self):
        """Initialize the JB combiner."""
        self.bank_code = 'JB'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
    
    def _is_empty_row(self, row: pd.Series) -> bool:
        """
        Check if row is effectively empty (NaN, empty string, or whitespace only).
        
        Args:
            row: Pandas Series representing a row
            
        Returns:
            True if row is effectively empty, False otherwise
        """
        for value in row:
            if pd.notna(value) and str(value).strip():
                return False
        return True
    
    def _reorder_columns_with_identifiers(self, df: pd.DataFrame, bank_code: str, client: str, account: str) -> pd.DataFrame:
        """
        Place bank, client, account columns at the beginning of the DataFrame.
        
        Args:
            df: DataFrame to reorder
            bank_code: Bank code to add
            client: Client code to add
            account: Account code to add
            
        Returns:
            DataFrame with bank, client, account as first columns
        """
        # Add identifier columns
        df['bank'] = bank_code
        df['client'] = client
        df['account'] = account
        
        # Get all other columns
        other_columns = [col for col in df.columns if col not in ['bank', 'client', 'account']]
        
        # Reorder: bank, client, account first, then others
        df = df[['bank', 'client', 'account'] + other_columns]
        
        return df
    
    def discover_jb_files(self, jb_dir: Path, date: str) -> Dict[str, List[Dict]]:
        """
        Discover JB files for a specific date.
        
        Args:
            jb_dir: Directory containing individual JB files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict with 'securities' and 'transactions' lists containing file info
        """
        logger.info(f"🔍 Scanning for JB files in: {jb_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not jb_dir.exists():
            logger.error(f"❌ JB directory does not exist: {jb_dir}")
            return {'securities': [], 'transactions': []}
        
        discovered_files = {'securities': [], 'transactions': []}
        clients_found = set()
        
        # Scan for JB files (support both .xlsx and .xls)
        for pattern in ["*.xlsx", "*.xls"]:
            for file in jb_dir.glob(pattern):
                # Skip temp files
                if file.name.startswith('~$'):
                    logger.debug(f"  Skipping temp file: {file.name}")
                    continue
                
                # Check if file matches JB pattern and date
                if not file.name.startswith('JB_'):
                    logger.debug(f"  Skipping non-JB file: {file.name}")
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
                if 'securities' in file.name.lower() or 'secturities' in file.name.lower():  # Handle typo
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
        
        # Check for missing files
        securities_clients = {f"{f['client']}_{f['account']}" for f in discovered_files['securities']}
        transactions_clients = {f"{f['client']}_{f['account']}" for f in discovered_files['transactions']}
        
        missing_transactions = securities_clients - transactions_clients
        if missing_transactions:
            logger.warning(f"⚠️ Clients missing transactions files: {', '.join(sorted(missing_transactions))}")
        
        missing_securities = transactions_clients - securities_clients
        if missing_securities:
            logger.warning(f"⚠️ Clients missing securities files: {', '.join(sorted(missing_securities))}")
        
        return discovered_files
    
    def detect_file_format(self, client: str, account: str) -> str:
        """
        Detect if file follows GZ or HS format.
        
        Args:
            client: Client code
            account: Account code
            
        Returns:
            'GZ' for full format, 'HS' for limited format
        """
        if client == 'GZ':
            return 'GZ'
        elif client == 'HS':
            return 'HS'
        else:
            # Default to GZ format for unknown patterns
            logger.warning(f"Unknown client format: {client}, defaulting to GZ")
            return 'GZ'
    
    def _detect_footer_start(self, df: pd.DataFrame) -> int:
        """
        Detect where footer content starts in securities file.
        Look for 2+ consecutive empty rows, footer starts after that.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Index where footer starts, or len(df) if no footer detected
        """
        empty_rows = df.isnull().all(axis=1)
        empty_row_indices = df.index[empty_rows].tolist()
        
        if not empty_row_indices:
            return len(df)
        
        # Find 2+ consecutive empty rows
        consecutive_count = 1
        for i in range(1, len(empty_row_indices)):
            if empty_row_indices[i] - empty_row_indices[i-1] == 1:
                consecutive_count += 1
                if consecutive_count >= 2:
                    # Found 2+ consecutive empty rows, footer starts after
                    footer_start = empty_row_indices[i-1]
                    logger.info(f"    📋 Detected footer starting at row {footer_start}")
                    return footer_start
            else:
                consecutive_count = 1
        
        # No footer detected
        return len(df)
    
    def _process_gz_securities(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process GZ format securities file with header=4 and footer detection.
        
        Args:
            file_path: Path to securities file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
        """
        logger.info(f"  📄 Processing GZ securities: {client}_{account} -> {file_path.name}")
        
        try:
            # GZ securities files have column titles in row 5 (0-indexed = 4)
            df = pd.read_excel(file_path, header=4)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty securities file: {file_path.name}")
                return pd.DataFrame()
            
            initial_rows = len(df)
            logger.info(f"    📊 Loaded {initial_rows} rows with {len(df.columns)} columns")
            
            # Detect and remove footer content
            footer_start = self._detect_footer_start(df)
            if footer_start < len(df):
                df = df.iloc[:footer_start]
                footer_rows_removed = initial_rows - len(df)
                logger.info(f"    🚮 Removed {footer_rows_removed} footer rows")
            
            final_rows = len(df)
            logger.info(f"    ✅ Final dataset: {final_rows} rows")
            
            return df
            
        except Exception as e:
            logger.error(f"    ❌ Error processing GZ securities file {file_path.name}: {str(e)}")
            raise
    
    def _process_hs_securities(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process HS format securities file with header=0 and limited columns.
        
        Args:
            file_path: Path to securities file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data and filled missing columns
        """
        logger.info(f"  📄 Processing HS securities: {client}_{account} -> {file_path.name}")
        
        try:
            # HS securities files have column titles in row 1 (0-indexed = 0)
            df = pd.read_excel(file_path, header=0)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty securities file: {file_path.name}")
                return pd.DataFrame()
            
            initial_rows = len(df)
            logger.info(f"    📊 Loaded {initial_rows} rows with {len(df.columns)} columns (HS format)")
            
            # HS format has limited columns, add missing ones with None
            # Standard columns that GZ format has but HS might be missing
            standard_columns = [
                'Asset Class', 'Instrument Name', 'Instrument', 'Quantity', 'Region', 
                'Currency', 'Price', 'Price Curr', 'Quote Date', 'Maturity Date', 
                'Exchange Rate', 'Market Value', 'Ref, Curr', 'MV%', 'Cost Price', 
                'Ref, Curr.1', 'Exchange Rate.1', 'Net Cost Value', 'Ref, Curr.2', 
                'Capital P%L', 'Currency P&L', 'P&L(%)', 'Indicative LTV (%)'
            ]
            
            for col in standard_columns:
                if col not in df.columns:
                    df[col] = None
                    logger.debug(f"    ➕ Added missing column: {col}")
            
            # Reorder columns to match GZ format
            df = df.reindex(columns=standard_columns, fill_value=None)
            
            logger.info(f"    ✅ Final HS dataset: {len(df)} rows with standardized columns")
            
            return df
            
        except Exception as e:
            logger.error(f"    ❌ Error processing HS securities file {file_path.name}: {str(e)}")
            raise
    
    def process_securities_file(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process securities file based on format type.
        
        Args:
            file_path: Path to securities file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
        """
        format_type = self.detect_file_format(client, account)
        logger.info(f"  🔍 Detected format: {format_type}")
        
        if format_type == 'GZ':
            return self._process_gz_securities(file_path, client, account)
        else:  # HS
            return self._process_hs_securities(file_path, client, account)
    
    def _process_gz_transactions(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process GZ format transactions file with header=3 and empty row skipping.
        
        Args:
            file_path: Path to transactions file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
        """
        logger.info(f"  💰 Processing GZ transactions: {client}_{account} -> {file_path.name}")
        
        try:
            # GZ transactions files have column titles in row 4 (0-indexed = 3)
            df = pd.read_excel(file_path, header=3)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty transactions file: {file_path.name}")
                return pd.DataFrame()
            
            initial_rows = len(df)
            logger.info(f"    📊 Loaded {initial_rows} rows with {len(df.columns)} columns")
            
            # Remove empty rows using robust filtering (handles NaN, empty strings, whitespace)
            df = df[~df.apply(self._is_empty_row, axis=1)]
            final_rows = len(df)
            
            empty_rows_removed = initial_rows - final_rows
            if empty_rows_removed > 0:
                logger.info(f"    🚮 Removed {empty_rows_removed} empty rows")
            
            logger.info(f"    ✅ Final transactions dataset: {final_rows} rows")
            
            return df
            
        except Exception as e:
            logger.error(f"    ❌ Error processing GZ transactions file {file_path.name}: {str(e)}")
            raise
    
    def process_transactions_file(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process transactions file based on format type.
        
        Args:
            file_path: Path to transactions file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
        """
        format_type = self.detect_file_format(client, account)
        
        if format_type == 'GZ':
            return self._process_gz_transactions(file_path, client, account)
        else:  # HS
            # HS format typically doesn't have transactions files
            logger.info(f"  💰 HS format - no transactions file expected for: {client}_{account}")
            return pd.DataFrame()
    
    def combine_securities_files(self, files: List[Dict], output_path: Path) -> bool:
        """
        Combine multiple securities files into one.
        
        Args:
            files: List of file info dictionaries
            output_path: Path for combined output file
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"📄 Combining {len(files)} securities files")
        
        if not files:
            logger.warning("⚠️ No securities files to combine")
            return False
        
        combined_data = []
        
        for file_info in files:
            file_path = file_info['file']
            client = file_info['client']
            account = file_info['account']
            
            try:
                # Process the securities file
                df = self.process_securities_file(file_path, client, account)
                
                if df.empty:
                    logger.warning(f"    ⚠️ Skipping empty file: {file_path.name}")
                    continue
                
                # Add bank, client, account columns at the beginning
                df = self._reorder_columns_with_identifiers(df, self.bank_code, client, account)
                
                combined_data.append(df)
                logger.info(f"    ✅ Added {len(df)} securities from {client}_{account}")
                
            except Exception as e:
                logger.error(f"    ❌ Failed to process {file_path.name}: {str(e)}")
                continue
        
        if not combined_data:
            logger.error("❌ No valid securities data to combine")
            return False
        
        # Combine all dataframes
        final_df = pd.concat(combined_data, ignore_index=True)
        
        # Save to output file
        try:
            final_df.to_excel(output_path, index=False)
            logger.info(f"✅ Combined securities saved: {output_path}")
            logger.info(f"📊 Total securities: {len(final_df)} rows, {len(final_df.columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save combined securities file: {str(e)}")
            return False
    
    def combine_transactions_files(self, files: List[Dict], output_path: Path) -> bool:
        """
        Combine multiple transactions files into one.
        
        Args:
            files: List of file info dictionaries
            output_path: Path for combined output file
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"💰 Combining {len(files)} transactions files")
        
        if not files:
            logger.warning("⚠️ No transactions files to combine")
            return False
        
        combined_data = []
        
        for file_info in files:
            file_path = file_info['file']
            client = file_info['client']
            account = file_info['account']
            
            try:
                # Process the transactions file
                df = self.process_transactions_file(file_path, client, account)
                
                if df.empty:
                    logger.warning(f"    ⚠️ Skipping empty file: {file_path.name}")
                    continue
                
                # Add bank, client, account columns at the beginning
                df = self._reorder_columns_with_identifiers(df, self.bank_code, client, account)
                
                combined_data.append(df)
                logger.info(f"    ✅ Added {len(df)} transactions from {client}_{account}")
                
            except Exception as e:
                logger.error(f"    ❌ Failed to process {file_path.name}: {str(e)}")
                continue
        
        if not combined_data:
            logger.warning("⚠️ No valid transactions data to combine")
            # Create empty file anyway
            empty_df = pd.DataFrame()
            empty_df.to_excel(output_path, index=False)
            logger.info(f"📝 Created empty transactions file: {output_path}")
            return True
        
        # Combine all dataframes
        final_df = pd.concat(combined_data, ignore_index=True)
        
        # Save to output file
        try:
            final_df.to_excel(output_path, index=False)
            logger.info(f"✅ Combined transactions saved: {output_path}")
            logger.info(f"📊 Total transactions: {len(final_df)} rows, {len(final_df.columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save combined transactions file: {str(e)}")
            return False
    
    def combine_all_files(self, jb_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Combine all JB files for a specific date.
        
        Args:
            jb_dir: Directory containing individual JB files
            output_dir: Directory for combined output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, only show what would be done
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting JB file combination for date: {date}")
        logger.info(f"📂 Input directory: {jb_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        
        if dry_run:
            logger.info("🔍 DRY RUN MODE - No files will be created")
        
        # Discover files
        discovered_files = self.discover_jb_files(jb_dir, date)
        
        if not discovered_files['securities'] and not discovered_files['transactions']:
            logger.error("❌ No JB files found for the specified date")
            return False
        
        if dry_run:
            logger.info("✅ DRY RUN COMPLETE - Files discovered successfully")
            return True
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        
        success = True
        
        # Combine securities files
        if discovered_files['securities']:
            securities_output = output_dir / f"{self.bank_code}_securities_{date}.xlsx"
            if not self.combine_securities_files(discovered_files['securities'], securities_output):
                success = False
        
        # Combine transactions files
        if discovered_files['transactions']:
            transactions_output = output_dir / f"{self.bank_code}_transactions_{date}.xlsx"
            if not self.combine_transactions_files(discovered_files['transactions'], transactions_output):
                success = False
        
        if success:
            logger.info("🎉 JB file combination completed successfully!")
        else:
            logger.error("❌ JB file combination completed with errors")
        
        return success 