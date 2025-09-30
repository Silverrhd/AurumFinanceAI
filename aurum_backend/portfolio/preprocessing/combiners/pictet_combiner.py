#!/usr/bin/env python3
"""
Pictet File Combiner

Combines individual Pictet client files into unified bank files.
Adds bank, client, and account columns extracted from filenames.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

# Add the project root to Python path
aurum_backend_root = Path(__file__).parent.parent.parent  # Gets to aurum_backend
sys.path.insert(0, str(aurum_backend_root))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv(aurum_backend_root / '.env')

try:
    from portfolio.services.mappings_encryption_service import MappingsEncryptionService
except ImportError:
    from services.mappings_encryption_service import MappingsEncryptionService
from preprocessing.bank_detector import BankDetector

logger = logging.getLogger(__name__)


class PictetCombiner:
    """Combines individual Pictet client files into unified bank files."""
    
    def __init__(self):
        """Initialize the Pictet combiner."""
        self.bank_code = 'Pictet'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
        
    def load_account_mappings(self, mappings_file: str) -> Dict[str, Dict[str, str]]:
        """
        Load Pictet account mappings from encrypted Excel file.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            
        Returns:
            Dict mapping account numbers to client/account info
        """
        logger.info(f"Loading Pictet account mappings from {mappings_file}")
        
        try:
            encryption_service = MappingsEncryptionService()
            df = encryption_service.read_encrypted_excel(mappings_file + '.encrypted', sheet_name='Pictet')
            
            # Validate required columns
            required_cols = ['account number', 'client', 'account']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Pictet mappings missing required columns: {missing_cols}")
            
            # Create mapping dictionary: account_number -> {client, account}
            mappings = {}
            
            for _, row in df.iterrows():
                account_num = str(row['account number']).replace(' ', '').strip()  # Normalize
                client = str(row['client']).strip()
                account = str(row['account']).strip()
                
                # Skip rows with missing data
                if pd.isna(row['account number']) or pd.isna(row['client']) or pd.isna(row['account']):
                    logger.warning(f"Skipping mapping for account {account_num} - missing data")
                    continue
                
                mappings[account_num] = {
                    'client': client,
                    'account': account
                }
            
            logger.info(f"Loaded {len(mappings)} Pictet account mappings")
            return mappings
            
        except Exception as e:
            logger.error(f"Error loading Pictet account mappings: {str(e)}")
            raise
    
    def discover_pictet_files(self, pictet_dir: Path, date: str) -> Dict[str, List[Dict]]:
        """
        Discover Pictet files for a specific date.
        
        Args:
            pictet_dir: Directory containing individual Pictet files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict with 'securities' and 'transactions' lists containing file info
        """
        logger.info(f"🔍 Scanning for Pictet files in: {pictet_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not pictet_dir.exists():
            logger.error(f"❌ Pictet directory does not exist: {pictet_dir}")
            return {'securities': [], 'transactions': []}
        
        discovered_files = {'securities': [], 'transactions': []}
        clients_found = set()
        
        # Scan for Pictet files
        for file in pictet_dir.glob("*.xlsx"):
            # Check if file matches Pictet pattern and date
            if not file.name.startswith('Pictet_'):
                logger.debug(f"  Skipping non-Pictet file: {file.name}")
                continue
            
            # Extract date from filename
            file_date = BankDetector.extract_date_from_filename(file.name)
            if file_date != date:
                logger.debug(f"  Skipping file with different date: {file.name} (date: {file_date})")
                continue
            
            # Determine file type first
            if 'securities' in file.name.lower():
                file_type = 'securities'
            elif 'transactions' in file.name.lower():
                file_type = 'transactions'
            else:
                logger.warning(f"⚠️ Unknown file type: {file.name}")
                continue
            
            # Handle securities files (have client/account) vs transactions (pre-combined)
            if file_type == 'securities':
                # Extract bank, client, account for securities files
                extraction = BankDetector.extract_client_account_from_filename(file.name)
                if not extraction:
                    logger.warning(f"⚠️ Could not extract client/account from securities file: {file.name}")
                    continue
                
                bank, client, account = extraction
                clients_found.add(f"{client}_{account}")
                
                file_info = {
                    'file': file,
                    'bank': bank,
                    'client': client,
                    'account': account,
                    'date': file_date,
                    'type': file_type
                }
            else:
                # Transactions file is pre-combined, no client/account in filename
                file_info = {
                    'file': file,
                    'bank': 'Pictet',
                    'client': 'PRECOMBINED',  # Indicates this is pre-combined
                    'account': 'PRECOMBINED',
                    'date': file_date,
                    'type': file_type
                }
            
            discovered_files[file_type].append(file_info)
            if file_type == 'securities':
                logger.info(f"  ✅ Found {file_type}: {client}_{account} -> {file.name}")
            else:
                logger.info(f"  ✅ Found {file_type}: PRECOMBINED -> {file.name}")
        
        # Summary
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
    
    def _find_securities_header_row(self, file_path: Path, max_search_rows: int = 15) -> int:
        """
        Dynamically find header row in Pictet securities file.
        
        Args:
            file_path: Path to securities file
            max_search_rows: Maximum rows to search for headers
            
        Returns:
            Row index (0-based) where headers are found
        """
        logger.debug(f"🔍 Searching for securities headers in: {file_path.name}")
        
        try:
            # Read first N rows without header to search for "Quantity" column
            df_preview = pd.read_excel(file_path, header=None, nrows=max_search_rows)
            
            for row_idx in range(len(df_preview)):
                row_values = df_preview.iloc[row_idx].astype(str)
                
                # Look for "Quantity" column which indicates header row
                for cell_value in row_values:
                    if pd.notna(cell_value) and 'Quantity' in str(cell_value):
                        logger.debug(f"✅ Found securities headers at row {row_idx + 1}")
                        return row_idx
            
            # Fallback to row 9 if no header found
            logger.warning(f"⚠️ Header detection failed, using fallback row 10 (0-based: 9)")
            return 9
            
        except Exception as e:
            logger.error(f"❌ Error during header detection: {str(e)}")
            logger.warning(f"⚠️ Using fallback row 10 (0-based: 9)")
            return 9
    
    def _process_single_securities_file(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process single Pictet securities file with dynamic header detection,
        data filtering, and column renaming.
        
        Args:
            file_path: Path to securities file
            client: Client code from filename
            account: Account code from filename
            
        Returns:
            Processed DataFrame with securities data
        """
        logger.info(f"  📄 Processing: {client}_{account} -> {file_path.name}")
        
        try:
            # STEP 1: DYNAMIC HEADER DETECTION
            header_row = self._find_securities_header_row(file_path)
            
            # STEP 2: READ WITH DETECTED HEADER
            logger.debug(f"📖 Reading file with header at row {header_row + 1}")
            df = pd.read_excel(file_path, header=header_row)
            
            if df.empty:
                logger.warning(f"⚠️ Empty file after reading: {file_path.name}")
                return pd.DataFrame()
            
            logger.debug(f"📊 Loaded {len(df)} total rows from file")
            
            # STEP 3: RENAME COLUMN 3 TO "name"
            if len(df.columns) > 3:
                old_col_name = df.columns[3]
                df = df.rename(columns={old_col_name: 'name'})
                logger.debug(f"📝 Renamed column 3 from '{old_col_name}' to 'name'")
            else:
                logger.error(f"❌ File has insufficient columns: {len(df.columns)}")
                return pd.DataFrame()
            
            # STEP 4: FILTER VALID SECURITIES (Col[0]==2 AND name is not NaN)
            df_securities = df[(df.iloc[:, 0] == 2) & df['name'].notna()].copy()
            logger.debug(f"🔍 Filtered to {len(df_securities)} valid securities (from {len(df)} total rows)")
            
            # STEP 5: REMOVE DISCLAIMER ROWS
            df_clean = df_securities[
                ~df_securities.iloc[:, 0].astype(str).str.contains('Data exported from Pictet', na=False)
            ].copy()
            
            if len(df_clean) != len(df_securities):
                removed = len(df_securities) - len(df_clean)
                logger.debug(f"🧹 Removed {removed} disclaimer rows")
            
            # STEP 6: ADD SYSTEM COLUMNS
            df_clean.insert(0, 'bank', 'Pictet')
            df_clean.insert(1, 'client', client)
            df_clean.insert(2, 'account', account)
            
            logger.info(f"  ✅ Processed {len(df_clean)} securities from {client}_{account}")
            return df_clean
            
        except Exception as e:
            logger.error(f"  ❌ Error processing {file_path.name}: {str(e)}")
            return pd.DataFrame()
    
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
        
        logger.info(f"🔗 Combining {len(files)} Pictet securities files...")
        
        combined_data = []
        successful_files = 0
        failed_files = 0
        total_securities = 0
        
        for file_info in files:
            file_path = file_info['file']
            client = file_info['client']
            account = file_info['account']
            
            # Process single securities file
            df_processed = self._process_single_securities_file(file_path, client, account)
            
            if df_processed.empty:
                logger.warning(f"  ⚠️ No securities data from: {client}_{account}")
                failed_files += 1
                continue
            
            combined_data.append(df_processed)
            successful_files += 1
            total_securities += len(df_processed)
        
        if not combined_data:
            logger.error("❌ No valid securities data to combine")
            return False
        
        try:
            # Combine all DataFrames
            final_df = pd.concat(combined_data, ignore_index=True)
            
            # Save to output file
            final_df.to_excel(output_path, index=False)
            
            logger.info(f"✅ Securities combination completed!")
            logger.info(f"  📊 Total securities: {total_securities}")
            logger.info(f"  📁 Total columns: {len(final_df.columns)}")
            logger.info(f"  ✅ Successful files: {successful_files}")
            if failed_files > 0:
                logger.warning(f"  ❌ Failed files: {failed_files}")
            logger.info(f"  💾 Saved to: {output_path.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving combined securities file: {str(e)}")
            return False
    
    def combine_transactions_files(self, files: List[Dict], output_path: Path, account_mappings: Dict[str, Dict[str, str]]) -> bool:
        """
        Process pre-combined Pictet transactions file with dynamic header detection,
        disclaimer removal, and account mapping.
        
        Args:
            files: List with single file info dict (pre-combined transactions)
            output_path: Path for output file  
            account_mappings: Account number to client/account mapping from encrypted file
            
        Returns:
            True if successful, False otherwise
        """
        if not files:
            logger.warning("⚠️ No transactions files to process")
            return False
        
        if len(files) > 1:
            logger.warning(f"⚠️ Expected 1 pre-combined transactions file, found {len(files)}. Using first one.")
        
        file_info = files[0]
        file_path = file_info['file']
        bank = 'Pictet'  # Extract from filename: file_info['bank']
        
        logger.info(f"🔗 Processing pre-combined transactions file: {file_path.name}")
        
        try:
            # STEP 1: DYNAMIC HEADER DETECTION
            header_row = self._find_transactions_header_row(file_path)
            
            # STEP 2: READ WITH DETECTED HEADER
            logger.info(f"📖 Reading file with header at row {header_row + 1}")
            df = pd.read_excel(file_path, header=header_row)
            
            if df.empty:
                logger.warning(f"⚠️ Empty file after reading: {file_path.name}")
                return False
            
            logger.info(f"📊 Loaded {len(df)} total rows from file")
            
            # STEP 3: DYNAMIC DISCLAIMER REMOVAL  
            df_clean = self._remove_disclaimer_rows(df)
            
            # STEP 4: VALIDATE REQUIRED COLUMNS
            if 'Portfolio' not in df_clean.columns:
                logger.error(f"❌ Missing 'Portfolio' column for account mapping")
                return False
            
            # STEP 5: ADD SYSTEM COLUMNS (bank, client, account)
            df_final = self._add_system_columns(df_clean, bank, account_mappings)
            
            # STEP 6: SAVE RESULT
            df_final.to_excel(output_path, index=False)
            
            logger.info(f"✅ Transactions processing completed!")
            logger.info(f"  📊 Total records: {len(df_final)}")
            logger.info(f"  💾 Saved to: {output_path.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error processing transactions file {file_path.name}: {str(e)}")
            return False
    
    def _find_transactions_header_row(self, file_path: Path, max_search_rows: int = 25) -> int:
        """
        Dynamically find header row in Pictet transactions file.
        
        Args:
            file_path: Path to transactions file
            max_search_rows: Maximum rows to search for headers
            
        Returns:
            Row index (0-based) where headers are found
        """
        logger.info(f"🔍 Searching for transaction headers in: {file_path.name}")
        
        # Expected key columns for Pictet transactions
        expected_columns = ['Portfolio', 'Booking date', 'Description', 'Amount']
        
        try:
            # Read first N rows without header to search for patterns
            df_preview = pd.read_excel(file_path, header=None, nrows=max_search_rows)
            
            logger.debug(f"📊 Scanning {len(df_preview)} rows for header patterns...")
            
            for row_idx in range(len(df_preview)):
                row_values = df_preview.iloc[row_idx].astype(str)
                
                # Count matches (case-insensitive, partial matching for Amount columns)
                matches = 0
                matched_columns = []
                
                for expected_col in expected_columns:
                    for cell_value in row_values:
                        if pd.notna(cell_value) and expected_col.lower() in str(cell_value).lower():
                            matches += 1
                            matched_columns.append(expected_col)
                            break
                
                # If we find 3/4 expected columns, this is likely the header row
                if matches >= 3:
                    logger.info(f"✅ Found transaction headers at row {row_idx + 1}")
                    logger.info(f"📋 Matched columns: {', '.join(matched_columns)}")
                    logger.info(f"📊 Match rate: {matches}/{len(expected_columns)}")
                    return row_idx
            
            # Fallback to row 13 if no header found
            logger.warning(f"⚠️ Header detection failed, using fallback row 14 (0-based: 13)")
            return 13
            
        except Exception as e:
            logger.error(f"❌ Error during header detection: {str(e)}")
            logger.warning(f"⚠️ Using fallback row 14 (0-based: 13)")
            return 13
    
    def _remove_disclaimer_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove disclaimer and empty rows from end of transactions data.
        
        Args:
            df: DataFrame with transactions data
            
        Returns:
            Cleaned DataFrame with only transaction records
        """
        logger.info(f"🧹 Removing disclaimer rows from {len(df)} total rows")
        
        # Keep only rows where Portfolio contains valid account numbers (numeric patterns)
        df_clean = df[
            df['Portfolio'].notna() &  # Not NaN
            df['Portfolio'].astype(str).str.match(r'^\d+\.\d+$')  # Matches pattern like "586347.001"
        ].copy()
        
        removed_rows = len(df) - len(df_clean)
        logger.info(f"📊 Removed {removed_rows} disclaimer/empty rows, kept {len(df_clean)} transactions")
        return df_clean
    
    def _add_system_columns(self, df: pd.DataFrame, bank: str, account_mappings: Dict[str, Dict[str, str]]) -> pd.DataFrame:
        """
        Add bank, client, account columns using account mappings.
        
        Args:
            df: Clean transactions DataFrame
            bank: Bank name ('Pictet')
            account_mappings: Account number to client/account mapping
            
        Returns:
            DataFrame with bank, client, account columns added
        """
        logger.info(f"🏦 Adding system columns to {len(df)} transactions")
        
        client_list = []
        account_list = []
        unmapped_accounts = set()
        successful_mappings = 0
        
        # Process each transaction row
        for _, row in df.iterrows():
            account_num = str(row['Portfolio']).strip()
            
            if account_num in account_mappings:
                client = account_mappings[account_num]['client']
                account = account_mappings[account_num]['account']
                successful_mappings += 1
                logger.debug(f"✅ Mapped {account_num} → {client}/{account}")
            else:
                client = 'UNMAPPED'
                account = 'UNMAPPED'
                unmapped_accounts.add(account_num)
                logger.debug(f"⚠️ Unmapped account: {account_num}")
            
            client_list.append(client)
            account_list.append(account)
        
        # Insert system columns at the beginning
        df_result = df.copy()
        df_result.insert(0, 'bank', bank)
        df_result.insert(1, 'client', client_list)
        df_result.insert(2, 'account', account_list)
        
        # Logging summary
        logger.info(f"📊 Account mapping summary:")
        logger.info(f"  ✅ Successfully mapped: {successful_mappings}/{len(df)} accounts")
        
        if unmapped_accounts:
            logger.warning(f"  ⚠️ Unmapped accounts: {sorted(unmapped_accounts)}")
            logger.warning(f"  💡 These accounts need to be added to the Pictet mappings sheet")
        
        # Log account distribution
        account_distribution = df_result.groupby(['client', 'account']).size()
        logger.info(f"📈 Transaction distribution:")
        for (client, account), count in account_distribution.items():
            logger.info(f"  {client}/{account}: {count} transactions")
        
        return df_result
    
    def combine_all_files(self, pictet_dir: Path, output_dir: Path, date: str, mappings_file: str, dry_run: bool = False) -> bool:
        """
        Main method to combine all Pictet files for a specific date.
        
        Args:
            pictet_dir: Directory containing individual Pictet files
            output_dir: Directory for output files
            date: Date string in DD_MM_YYYY format
            mappings_file: Path to Mappings.xlsx file
            dry_run: If True, show what would be processed without actually processing
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting Pictet file combination for date: {date}")
        logger.info(f"📁 Input directory: {pictet_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        logger.info(f"🗺️  Mappings file: {mappings_file}")
        
        try:
            # Load account mappings (needed for transactions)
            account_mappings = self.load_account_mappings(mappings_file)
            
            # Discover files
            discovered_files = self.discover_pictet_files(pictet_dir, date)
            
            if not discovered_files['securities'] and not discovered_files['transactions']:
                logger.error(f"❌ No Pictet files found for date {date}")
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
            
            results = []
            
            # Use main input_files directory for output
            input_files_dir = Path('data/excel/input_files')
            
            # Combine securities files
            if discovered_files['securities']:
                securities_output = input_files_dir / f"Pictet_securities_{date}.xlsx"
                success = self.combine_securities_files(discovered_files['securities'], securities_output)
                results.append(success)
                
                if not success:
                    logger.error("❌ Failed to combine securities files")
            else:
                logger.info("ℹ️ No securities files to combine")
        
            # Process transactions file (pre-combined)
            if discovered_files['transactions']:
                transactions_output = input_files_dir / f"Pictet_transactions_{date}.xlsx"
                success = self.combine_transactions_files(discovered_files['transactions'], transactions_output, account_mappings)
                results.append(success)
                
                if not success:
                    logger.error("❌ Failed to process transactions file")
            else:
                logger.info("ℹ️ No transactions file to process")
        
            # Overall success
            overall_success = all(results) if results else False
            
            if overall_success:
                logger.info("🎉 Pictet file combination completed successfully!")
            else:
                logger.error("❌ Pictet file combination completed with errors")
            
            return overall_success
            
        except Exception as e:
            logger.error(f"❌ Error during Pictet file combination: {str(e)}")
            return False