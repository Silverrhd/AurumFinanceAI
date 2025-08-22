#!/usr/bin/env python3
"""
CSC (Charles Schwab) File Combiner

Combines individual CSC client files into unified bank files.
Securities files: header=4, filter "Account Total" rows, keep "Cash & Cash Investments"
Transactions files: header=1, no filtering needed
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


class CSCCombiner:
    """Combines individual CSC client files into unified bank files."""
    
    def __init__(self):
        """Initialize the CSC combiner."""
        self.bank_code = 'CSC'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
    
    def discover_csc_files(self, csc_dir: Path, date: str) -> Dict[str, List[Dict]]:
        """
        Discover CSC files for a specific date.
        
        Args:
            csc_dir: Directory containing individual CSC files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict with 'securities' and 'transactions' lists containing file info
        """
        logger.info(f"🔍 Scanning for CSC files in: {csc_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not csc_dir.exists():
            logger.error(f"❌ CSC directory does not exist: {csc_dir}")
            return {'securities': [], 'transactions': []}
        
        discovered_files = {'securities': [], 'transactions': []}
        clients_found = set()
        
        # Scan for CSC files (support both .xlsx and .xls)
        for pattern in ["*.xlsx", "*.xls"]:
            for file in csc_dir.glob(pattern):
                # Skip temp files
                if file.name.startswith('~$'):
                    logger.debug(f"  Skipping temp file: {file.name}")
                    continue
                
                # Check if file matches CSC pattern and date
                if not file.name.startswith('CSC_'):
                    logger.debug(f"  Skipping non-CSC file: {file.name}")
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
    
    def process_securities_file(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process CSC securities file with header=4 and Account Total filtering.
        
        Args:
            file_path: Path to securities file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
        """
        logger.info(f"  📄 Processing securities: {client}_{account} -> {file_path.name}")
        
        try:
            # CSC securities files have column titles in row 5 (0-indexed = 4)
            df = pd.read_excel(file_path, header=4)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty securities file: {file_path.name}")
                return pd.DataFrame()
            
            initial_rows = len(df)
            logger.info(f"    📊 Loaded {initial_rows} rows with {len(df.columns)} columns")
            
            # Filter out "Account Total" rows but keep "Cash & Cash Investments"
            if 'Symbol' in df.columns:
                # Remove Account Total rows
                account_total_mask = df['Symbol'] == 'Account Total'
                account_total_count = account_total_mask.sum()
                df = df[~account_total_mask]
                
                if account_total_count > 0:
                    logger.info(f"    🚮 Filtered out {account_total_count} 'Account Total' rows")
                
                # Log if we have Cash & Cash Investments (keeping these)
                cash_mask = df['Symbol'] == 'Cash & Cash Investments'
                cash_count = cash_mask.sum()
                if cash_count > 0:
                    logger.info(f"    💰 Keeping {cash_count} 'Cash & Cash Investments' rows")
                
                final_rows = len(df)
                logger.info(f"    ✅ Final dataset: {final_rows} rows (filtered {initial_rows - final_rows} rows)")
            else:
                logger.warning(f"    ⚠️ No 'Symbol' column found for filtering in: {file_path.name}")
            
            return df
            
        except Exception as e:
            logger.error(f"    ❌ Error processing securities file {file_path.name}: {str(e)}")
            raise
    
    def process_transactions_file(self, file_path: Path, client: str, account: str) -> pd.DataFrame:
        """
        Process CSC transactions file with header=1.
        
        Args:
            file_path: Path to transactions file
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with processed data
        """
        logger.info(f"  💰 Processing transactions: {client}_{account} -> {file_path.name}")
        
        try:
            # CSC transactions files have column titles in row 2 (0-indexed = 1)
            df = pd.read_excel(file_path, header=1)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty transactions file: {file_path.name}")
                return pd.DataFrame()
            
            logger.info(f"    ✅ Loaded {len(df)} transactions with {len(df.columns)} columns")
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
                # Process securities file
                df = self.process_securities_file(file_path, client, account)
                
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
        
        # Combine all DataFrames
        logger.info("🔄 Concatenating securities data...")
        combined_df = pd.concat(combined_data, ignore_index=True)
        
        # Save to output file
        logger.info(f"💾 Saving combined securities to: {output_path}")
        combined_df.to_excel(output_path, index=False)
        
        # Log final summary
        total_records = len(combined_df)
        total_columns = len(combined_df.columns)
        logger.info(f"✅ Securities combination completed:")
        logger.info(f"  📊 Total records: {total_records}")
        logger.info(f"  📋 Total columns: {total_columns}")
        logger.info(f"  ✅ Successful files: {successful_files}")
        if failed_files > 0:
            logger.warning(f"  ❌ Failed files: {failed_files}")
        
        return True
    
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
        
        # Combine all DataFrames
        logger.info("🔄 Concatenating transactions data...")
        combined_df = pd.concat(combined_data, ignore_index=True)
        
        # Save to output file
        logger.info(f"💾 Saving combined transactions to: {output_path}")
        combined_df.to_excel(output_path, index=False)
        
        # Log final summary
        total_records = len(combined_df)
        total_columns = len(combined_df.columns)
        logger.info(f"✅ Transactions combination completed:")
        logger.info(f"  📊 Total records: {total_records}")
        logger.info(f"  📋 Total columns: {total_columns}")
        logger.info(f"  ✅ Successful files: {successful_files}")
        if failed_files > 0:
            logger.warning(f"  ❌ Failed files: {failed_files}")
        
        return True
    
    def combine_all_files(self, csc_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Main method to combine all CSC files for a specific date.
        
        Args:
            csc_dir: Directory containing individual CSC files
            output_dir: Directory for combined output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, only show what would be processed
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("🚀 Starting CSC file combination process...")
        
        try:
            # Discover files
            discovered_files = self.discover_csc_files(csc_dir, date)
            
            securities_files = discovered_files['securities']
            transactions_files = discovered_files['transactions']
            
            if not securities_files and not transactions_files:
                logger.error("❌ No CSC files found for the specified date")
                return False
            
            if dry_run:
                logger.info("🧪 DRY RUN MODE - No files will be processed")
                logger.info("📋 Would process the following files:")
                
                if securities_files:
                    logger.info(f"  📄 Securities files ({len(securities_files)}):")
                    for file_info in securities_files:
                        logger.info(f"    • {file_info['client']}_{file_info['account']} -> {file_info['file'].name}")
                
                if transactions_files:
                    logger.info(f"  💰 Transactions files ({len(transactions_files)}):")
                    for file_info in transactions_files:
                        logger.info(f"    • {file_info['client']}_{file_info['account']} -> {file_info['file'].name}")
                
                return True
            
            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Combine securities files
            securities_success = True
            if securities_files:
                securities_output = output_dir / f"CSC_securities_{date}.xlsx"
                securities_success = self.combine_securities_files(securities_files, securities_output)
            else:
                logger.warning("⚠️ No securities files to combine")
            
            # Combine transactions files
            transactions_success = True
            if transactions_files:
                transactions_output = output_dir / f"CSC_transactions_{date}.xlsx"
                transactions_success = self.combine_transactions_files(transactions_files, transactions_output)
            else:
                logger.warning("⚠️ No transactions files to combine")
            
            # Overall success
            overall_success = securities_success and transactions_success
            
            if overall_success:
                logger.info("🎉 CSC file combination completed successfully!")
            else:
                logger.error("❌ CSC file combination completed with errors")
            
            return overall_success
            
        except Exception as e:
            logger.error(f"❌ Unexpected error during CSC combination: {str(e)}")
            logger.exception("Full error details:")
            return False 