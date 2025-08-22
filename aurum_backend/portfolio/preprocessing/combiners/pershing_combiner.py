#!/usr/bin/env python3
"""
Pershing File Combiner

Combines enriched Pershing securities and transactions files into final combined files.
Works with files that have already been enriched with unit cost data.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.bank_detector import BankDetector
from preprocessing.combiners.header_detector import HeaderDetector

logger = logging.getLogger(__name__)


class PershingCombiner:
    """Combines enriched Pershing securities and transactions files."""
    
    def __init__(self):
        """Initialize the Pershing combiner."""
        self.bank_code = 'Pershing'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
    
    def discover_enriched_files(self, input_dir: Path, date: str) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        Discover enriched Pershing files for a specific date and group by client/account.
        
        Args:
            input_dir: Directory containing enriched Pershing files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping client_account to file paths:
            {
                'LP_MT': {
                    'securities': Path(...),
                    'transactions': Path(...) or None
                }
            }
        """
        logger.info(f"🔍 Scanning for enriched Pershing files in: {input_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not input_dir.exists():
            logger.error(f"❌ Input directory does not exist: {input_dir}")
            return {}
        
        client_files = {}
        
        # Scan for enriched Pershing files (support both .xlsx and .xls)
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
                
                # Determine file type (only securities and transactions for combination)
                if 'securities' in file.name.lower():
                    file_type = 'securities'
                elif 'transactions' in file.name.lower():
                    file_type = 'transactions'
                else:
                    logger.debug(f"  Skipping non-combinable file: {file.name}")
                    continue
                
                # Initialize client entry if not exists
                if client_key not in client_files:
                    client_files[client_key] = {
                        'securities': None,
                        'transactions': None
                    }
                
                client_files[client_key][file_type] = file
                logger.info(f"  ✅ Found {file_type} file: {client_key} -> {file.name}")
        
        # Log summary
        logger.info(f"📊 Discovery summary:")
        valid_clients = 0
        
        for client_key, files in client_files.items():
            securities_status = "✅" if files['securities'] else "❌"
            transactions_status = "✅" if files['transactions'] else "⚠️"
            
            logger.info(f"  👤 {client_key}: securities {securities_status}, transactions {transactions_status}")
            
            # Check if we have required files (at least securities)
            if files['securities']:
                valid_clients += 1
            else:
                logger.warning(f"  ⚠️ {client_key} missing securities file")
        
        logger.info(f"  📊 Valid client sets: {valid_clients}/{len(client_files)}")
        
        return client_files
    
    def read_enriched_file(self, file_path: Path, file_type: str) -> pd.DataFrame:
        """
        Read enriched Pershing file.
        
        Args:
            file_path: Path to file
            file_type: Type of file ('securities', 'transactions')
            
        Returns:
            DataFrame with file data
        """
        logger.debug(f"  📖 Reading {file_type} file: {file_path.name}")
        
        try:
            # Use appropriate header detection method
            if file_type == 'securities':
                header_row = HeaderDetector.find_pershing_securities_header_row(file_path)
                expected_columns = HeaderDetector.PERSHING_SECURITIES_KEY_COLUMNS
            elif file_type == 'transactions':
                header_row = HeaderDetector.find_pershing_transactions_header_row(file_path)
                expected_columns = HeaderDetector.PERSHING_TRANSACTIONS_KEY_COLUMNS
            else:
                raise ValueError(f"Unknown file type: {file_type}")
            
            # Read the file
            df = HeaderDetector.read_excel_with_fallback(file_path, header_row)
            
            if df.empty:
                logger.warning(f"    ⚠️ Empty {file_type} file: {file_path.name}")
                return pd.DataFrame()
            
            # Validate columns (relaxed validation for enriched files)
            if HeaderDetector.validate_columns(df, expected_columns, min_match_ratio=0.3):
                logger.info(f"    ✅ Loaded {len(df)} {file_type} records (header row {header_row + 1})")
                return df
            else:
                logger.warning(f"    ⚠️ Column validation failed for {file_type} file, proceeding anyway")
                logger.info(f"    ✅ Loaded {len(df)} {file_type} records (header row {header_row + 1})")
                return df  # Return anyway for enriched files
                
        except Exception as e:
            logger.error(f"    ❌ Error reading {file_type} file {file_path.name}: {str(e)}")
            raise
    
    def add_bank_client_account_columns(self, df: pd.DataFrame, bank: str, client: str, account: str) -> pd.DataFrame:
        """
        Add Bank, Client, and Account columns at the beginning of the DataFrame.
        
        Args:
            df: DataFrame to modify
            bank: Bank name
            client: Client name
            account: Account name
            
        Returns:
            DataFrame with added columns
        """
        if df.empty:
            return df
        
        # Create a copy to avoid modifying original
        result_df = df.copy()
        
        # Insert columns at the beginning
        result_df.insert(0, 'Bank', bank)
        result_df.insert(1, 'Client', client)
        result_df.insert(2, 'Account', account)
        
        return result_df
    
    def combine_securities_files(self, client_files: Dict[str, Dict[str, Optional[Path]]], date: str) -> pd.DataFrame:
        """
        Combine all securities files into a single DataFrame.
        
        Args:
            client_files: Dict of client files
            date: Date string
            
        Returns:
            Combined securities DataFrame
        """
        logger.info(f"🔗 Combining securities files...")
        
        all_securities = []
        successful_files = 0
        failed_files = 0
        
        for client_key, files in client_files.items():
            if not files['securities']:
                logger.warning(f"  ⚠️ No securities file for {client_key}")
                failed_files += 1
                continue
            
            try:
                logger.info(f"  📄 Processing securities: {client_key}")
                
                # Read securities file
                securities_df = self.read_enriched_file(files['securities'], 'securities')
                
                if securities_df.empty:
                    logger.warning(f"    ⚠️ Empty securities file for {client_key}")
                    failed_files += 1
                    continue
                
                # Extract client and account from client_key
                parts = client_key.split('_', 1)
                if len(parts) == 2:
                    client, account = parts
                else:
                    client, account = client_key, 'Unknown'
                
                # Add bank/client/account columns
                securities_with_meta = self.add_bank_client_account_columns(
                    securities_df, self.bank_code, client, account
                )
                
                all_securities.append(securities_with_meta)
                successful_files += 1
                logger.info(f"    ✅ Added {len(securities_df)} securities records")
                
            except Exception as e:
                logger.error(f"    ❌ Failed to process securities for {client_key}: {str(e)}")
                failed_files += 1
                continue
        
        if not all_securities:
            logger.error("❌ No securities files were successfully processed")
            return pd.DataFrame()
        
        # Combine all securities
        combined_securities = pd.concat(all_securities, ignore_index=True)
        
        logger.info(f"  📊 Securities combination summary:")
        logger.info(f"    ✅ Successful files: {successful_files}")
        if failed_files > 0:
            logger.warning(f"    ❌ Failed files: {failed_files}")
        logger.info(f"    📋 Total securities records: {len(combined_securities)}")
        
        return combined_securities
    
    def combine_transactions_files(self, client_files: Dict[str, Dict[str, Optional[Path]]], date: str) -> pd.DataFrame:
        """
        Combine all transactions files into a single DataFrame.
        
        Args:
            client_files: Dict of client files
            date: Date string
            
        Returns:
            Combined transactions DataFrame
        """
        logger.info(f"💰 Combining transactions files...")
        
        all_transactions = []
        successful_files = 0
        failed_files = 0
        missing_files = 0
        
        for client_key, files in client_files.items():
            if not files['transactions']:
                logger.debug(f"  📋 No transactions file for {client_key}")
                missing_files += 1
                continue
            
            try:
                logger.info(f"  💰 Processing transactions: {client_key}")
                
                # Read transactions file
                transactions_df = self.read_enriched_file(files['transactions'], 'transactions')
                
                if transactions_df.empty:
                    logger.warning(f"    ⚠️ Empty transactions file for {client_key}")
                    failed_files += 1
                    continue
                
                # Extract client and account from client_key
                parts = client_key.split('_', 1)
                if len(parts) == 2:
                    client, account = parts
                else:
                    client, account = client_key, 'Unknown'
                
                # Add bank/client/account columns
                transactions_with_meta = self.add_bank_client_account_columns(
                    transactions_df, self.bank_code, client, account
                )
                
                all_transactions.append(transactions_with_meta)
                successful_files += 1
                logger.info(f"    ✅ Added {len(transactions_df)} transactions records")
                
            except Exception as e:
                logger.error(f"    ❌ Failed to process transactions for {client_key}: {str(e)}")
                failed_files += 1
                continue
        
        if not all_transactions:
            logger.warning("⚠️ No transactions files were found or successfully processed")
            return pd.DataFrame()
        
        # Combine all transactions
        combined_transactions = pd.concat(all_transactions, ignore_index=True)
        
        logger.info(f"  📊 Transactions combination summary:")
        logger.info(f"    ✅ Successful files: {successful_files}")
        if failed_files > 0:
            logger.warning(f"    ❌ Failed files: {failed_files}")
        if missing_files > 0:
            logger.info(f"    📋 Missing files: {missing_files}")
        logger.info(f"    💰 Total transactions records: {len(combined_transactions)}")
        
        return combined_transactions
    
    def combine_all_files(self, input_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Main method to combine all enriched Pershing files for a specific date.
        
        Args:
            input_dir: Directory containing enriched Pershing files
            output_dir: Directory for combined output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without actually processing
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting Pershing file combination for date: {date}")
        logger.info(f"📁 Input directory: {input_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        
        # Discover enriched files
        client_files = self.discover_enriched_files(input_dir, date)
        
        if not client_files:
            logger.error(f"❌ No enriched Pershing files found for date {date}")
            return False
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be combined")
            logger.info("📋 Would combine the following files:")
            for client_key, files in client_files.items():
                securities_status = "✅" if files['securities'] else "❌"
                transactions_status = "✅" if files['transactions'] else "⚠️"
                logger.info(f"  👤 {client_key}: securities {securities_status}, transactions {transactions_status}")
            return True
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Combine securities files
            combined_securities = self.combine_securities_files(client_files, date)
            
            if not combined_securities.empty:
                securities_output = output_dir / f"Pershing_securities_{date}.xlsx"
                combined_securities.to_excel(securities_output, index=False)
                logger.info(f"💾 Saved combined securities: {securities_output.name}")
                logger.info(f"  📊 Total records: {len(combined_securities)}")
            else:
                logger.error("❌ No securities data to save")
                return False
            
            # Combine transactions files
            combined_transactions = self.combine_transactions_files(client_files, date)
            
            if not combined_transactions.empty:
                transactions_output = output_dir / f"Pershing_transactions_{date}.xlsx"
                combined_transactions.to_excel(transactions_output, index=False)
                logger.info(f"💾 Saved combined transactions: {transactions_output.name}")
                logger.info(f"  💰 Total records: {len(combined_transactions)}")
            else:
                logger.warning("⚠️ No transactions data to save")
            
            # Final summary
            logger.info(f"🎉 Pershing file combination completed!")
            logger.info(f"  📁 Output files saved in: {output_dir}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during file combination: {str(e)}")
            return False 