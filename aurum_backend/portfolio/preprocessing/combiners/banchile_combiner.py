#!/usr/bin/env python3
"""
Banchile File Combiner

Combines individual Banchile client files with dual sheets into unified bank files.
Each Banchile file contains two sheets: "Posiciones" (securities) and "Movimientos" (transactions).
Adds bank, client, and account columns extracted from filenames and mappings.
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

logger = logging.getLogger(__name__)


class BanchileCombiner:
    """Combines individual Banchile client files with dual sheets into unified bank files."""
    
    def __init__(self):
        """Initialize the Banchile combiner."""
        self.bank_code = 'Banchile'
        self.securities_sheet_name = 'Posiciones'
        self.transactions_sheet_name = 'Movimientos'
        self.header_row = 4  # Row 5 in Excel (0-indexed)
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
        logger.info(f"📄 Securities sheet: {self.securities_sheet_name} (header row {self.header_row + 1})")
        logger.info(f"💰 Transactions sheet: {self.transactions_sheet_name} (header row {self.header_row + 1})")
    
    def discover_banchile_files(self, banchile_dir: Path, date: str) -> List[Dict]:
        """
        Discover Banchile files for a specific date.
        
        Args:
            banchile_dir: Directory containing individual Banchile files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            List of file info dictionaries with client/account codes
        """
        logger.info(f"🔍 Scanning for Banchile files in: {banchile_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not banchile_dir.exists():
            logger.error(f"❌ Banchile directory does not exist: {banchile_dir}")
            return []
        
        discovered_files = []
        clients_found = set()
        
        # Scan for Banchile files with pattern: Banchile_CLIENT_ACCOUNT_DATE.xlsx
        for file in banchile_dir.glob("*.xlsx"):
            # Check if file matches Banchile pattern
            if not file.name.startswith('Banchile_'):
                logger.debug(f"  Skipping non-Banchile file: {file.name}")
                continue
            
            # Extract date from filename
            file_date = BankDetector.extract_date_from_filename(file.name)
            if file_date != date:
                logger.debug(f"  Skipping file with different date: {file.name} (date: {file_date})")
                continue
            
            # Extract client/account from filename: Banchile_CLIENT_ACCOUNT_DATE.xlsx
            parts = file.name.replace('.xlsx', '').split('_')
            if len(parts) < 5:  # Banchile_CLIENT_ACCOUNT_DD_MM_YYYY
                logger.warning(f"⚠️ Invalid filename format: {file.name}")
                continue
            
            client = parts[1]
            account = parts[2]
            clients_found.add(f"{client}_{account}")
            
            file_info = {
                'file': file,
                'client': client,
                'account': account,
                'bank': self.bank_code
            }
            
            discovered_files.append(file_info)
            logger.info(f"  ✅ Found Banchile file: {client}_{account} -> {file.name}")
        
        # Log summary
        logger.info(f"📊 Discovery summary:")
        logger.info(f"  📁 Total files found: {len(discovered_files)}")
        logger.info(f"  👥 Unique clients/accounts: {len(clients_found)}")
        
        if not discovered_files:
            logger.warning("⚠️ No Banchile files found for the specified date")
        
        return discovered_files
    
    def process_single_file(self, file_path: Path, client: str, account: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process single Banchile file with dual sheets.
        
        Args:
            file_path: Path to Banchile Excel file
            client: Client code
            account: Account code
            
        Returns:
            Tuple of (securities_df, transactions_df)
        """
        logger.info(f"  📄 Processing file: {client}_{account} -> {file_path.name}")
        
        try:
            # Verify sheets exist
            xl_file = pd.ExcelFile(file_path)
            available_sheets = xl_file.sheet_names
            logger.debug(f"    Available sheets: {available_sheets}")
            
            if self.securities_sheet_name not in available_sheets:
                logger.error(f"    ❌ Missing '{self.securities_sheet_name}' sheet in {file_path.name}")
                return pd.DataFrame(), pd.DataFrame()
            
            if self.transactions_sheet_name not in available_sheets:
                logger.error(f"    ❌ Missing '{self.transactions_sheet_name}' sheet in {file_path.name}")
                return pd.DataFrame(), pd.DataFrame()
            
            # Read Posiciones sheet (securities)
            logger.debug(f"    📄 Reading {self.securities_sheet_name} sheet with header row {self.header_row + 1}")
            securities_df = pd.read_excel(file_path, sheet_name=self.securities_sheet_name, header=self.header_row)
            
            # Read Movimientos sheet (transactions)
            logger.debug(f"    💰 Reading {self.transactions_sheet_name} sheet with header row {self.header_row + 1}")
            transactions_df = pd.read_excel(file_path, sheet_name=self.transactions_sheet_name, header=self.header_row)
            
            # Filter out disclaimer rows
            logger.debug(f"    🔧 Filtering disclaimer rows from both sheets")
            securities_df = self.filter_disclaimer_rows(securities_df, "securities")
            transactions_df = self.filter_disclaimer_rows(transactions_df, "transactions")
            
            logger.info(f"    ✅ Processed: {len(securities_df)} securities, {len(transactions_df)} transactions")
            
            return securities_df, transactions_df
            
        except Exception as e:
            logger.error(f"    ❌ Error processing {file_path.name}: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()
    
    def filter_disclaimer_rows(self, df: pd.DataFrame, sheet_type: str) -> pd.DataFrame:
        """
        Remove rows starting with '(*)' and empty rows from dataframe.
        
        Args:
            df: DataFrame to filter
            sheet_type: Type of sheet for logging ("securities" or "transactions")
            
        Returns:
            Filtered DataFrame
        """
        if df.empty:
            logger.debug(f"      Empty {sheet_type} dataframe, nothing to filter")
            return df
        
        original_len = len(df)
        
        # Get first column name
        first_col = df.columns[0]
        
        # Filter out disclaimer rows and empty rows
        filtered_df = df[
            ~df[first_col].astype(str).str.startswith('(*)', na=False) &  # Remove disclaimer rows
            ~df[first_col].isna()  # Remove empty rows
        ].copy()
        
        filtered_len = len(filtered_df)
        removed_count = original_len - filtered_len
        
        if removed_count > 0:
            logger.debug(f"      Filtered {sheet_type}: removed {removed_count} disclaimer/empty rows ({original_len} → {filtered_len})")
        else:
            logger.debug(f"      No disclaimer rows found in {sheet_type} sheet")
        
        return filtered_df
    
    def add_bank_client_account_columns(self, df: pd.DataFrame, bank: str, client: str, account: str) -> pd.DataFrame:
        """
        Add bank, client, account columns at the beginning of dataframe.
        Following Valley/CS combiner pattern.
        
        Args:
            df: DataFrame to modify
            bank: Bank code
            client: Client code
            account: Account code
            
        Returns:
            DataFrame with added columns
        """
        if df.empty:
            return df
        
        # Insert columns at the beginning
        df.insert(0, 'bank', bank)
        df.insert(1, 'client', client)
        df.insert(2, 'account', account)
        
        return df
    
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
        
        logger.info(f"🔗 Combining {len(files)} Banchile securities files...")
        
        combined_data = []
        successful_files = 0
        failed_files = 0
        total_records = 0
        
        for file_info in files:
            file_path = file_info['file']
            client = file_info['client']
            account = file_info['account']
            bank = file_info['bank']
            
            try:
                # Process single file to get securities data
                securities_df, _ = self.process_single_file(file_path, client, account)
                
                if securities_df.empty:
                    logger.warning(f"  ⚠️ Empty securities data: {client}_{account}")
                    continue
                
                # Add bank, client, account columns
                securities_df = self.add_bank_client_account_columns(securities_df, bank, client, account)
                
                combined_data.append(securities_df)
                successful_files += 1
                record_count = len(securities_df)
                total_records += record_count
                logger.info(f"  ✅ Added {record_count} securities records from {client}_{account}")
                
            except Exception as e:
                logger.error(f"  ❌ Error processing securities from {file_path.name}: {str(e)}")
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
            logger.info(f"  📊 Total records: {total_records}")
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
        
        logger.info(f"🔗 Combining {len(files)} Banchile transactions files...")
        
        combined_data = []
        successful_files = 0
        failed_files = 0
        total_records = 0
        
        for file_info in files:
            file_path = file_info['file']
            client = file_info['client']
            account = file_info['account']
            bank = file_info['bank']
            
            try:
                # Process single file to get transactions data
                _, transactions_df = self.process_single_file(file_path, client, account)
                
                if transactions_df.empty:
                    logger.warning(f"  ⚠️ Empty transactions data: {client}_{account}")
                    continue
                
                # Add bank, client, account columns
                transactions_df = self.add_bank_client_account_columns(transactions_df, bank, client, account)
                
                combined_data.append(transactions_df)
                successful_files += 1
                record_count = len(transactions_df)
                total_records += record_count
                logger.info(f"  ✅ Added {record_count} transactions records from {client}_{account}")
                
            except Exception as e:
                logger.error(f"  ❌ Error processing transactions from {file_path.name}: {str(e)}")
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
            logger.info(f"  📊 Total records: {total_records}")
            logger.info(f"  ✅ Successful files: {successful_files}")
            if failed_files > 0:
                logger.warning(f"  ❌ Failed files: {failed_files}")
            logger.info(f"  💾 Saved to: {output_path.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving combined transactions file: {str(e)}")
            return False
    
    def combine_all_files(self, banchile_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Combine all Banchile files for a specific date.
        
        Args:
            banchile_dir: Directory containing individual Banchile files
            output_dir: Directory for combined output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without combining
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("🚀 Starting Banchile file combination")
        logger.info("=" * 50)
        
        # Discover files
        files = self.discover_banchile_files(banchile_dir, date)
        
        if not files:
            logger.error("❌ No Banchile files found to combine")
            return False
        
        # Define output paths
        securities_output = output_dir / f"Banchile_securities_{date}.xlsx"
        transactions_output = output_dir / f"Banchile_transactions_{date}.xlsx"
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - Showing what would be processed:")
            logger.info(f"  📄 Would combine {len(files)} files")
            logger.info(f"  📄 Securities output: {securities_output}")
            logger.info(f"  💰 Transactions output: {transactions_output}")
            for file_info in files:
                logger.info(f"    - {file_info['client']}_{file_info['account']}: {file_info['file'].name}")
            return True
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Combine securities files
        logger.info("\n📄 Processing securities files...")
        securities_success = self.combine_securities_files(files, securities_output)
        
        # Combine transactions files
        logger.info("\n💰 Processing transactions files...")
        transactions_success = self.combine_transactions_files(files, transactions_output)
        
        # Final summary
        logger.info("\n" + "=" * 50)
        if securities_success and transactions_success:
            logger.info("🎉 Banchile file combination completed successfully!")
            logger.info(f"📁 Output files saved to: {output_dir}")
            logger.info(f"  📄 Securities: {securities_output.name}")
            logger.info(f"  💰 Transactions: {transactions_output.name}")
            return True
        else:
            logger.error("❌ Banchile file combination failed")
            if not securities_success:
                logger.error("  📄 Securities combination failed")
            if not transactions_success:
                logger.error("  💰 Transactions combination failed")
            return False 