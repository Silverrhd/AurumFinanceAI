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

from portfolio.services.mappings_encryption_service import MappingsEncryptionService
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
                'date': file_date,
                'type': file_type
            }
            
            discovered_files[file_type].append(file_info)
            logger.info(f"  ✅ Found {file_type}: {client}_{account} -> {file.name}")
        
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
                logger.info(f"  📄 Processing: {client}_{account} -> {file_path.name}")
                
                # Read the Excel file
                # TODO: Add specific header detection logic for Pictet securities files
                df = pd.read_excel(file_path)
                
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
    
    def combine_transactions_files(self, files: List[Dict], output_path: Path, account_mappings: Dict[str, Dict[str, str]]) -> bool:
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
                logger.info(f"  💰 Processing: {client}_{account} -> {file_path.name}")
                
                # Read the Excel file
                # TODO: Add specific header detection logic for Pictet transactions files
                df = pd.read_excel(file_path)
                
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
            
            results = []
            
            # Combine securities files
            if discovered_files['securities']:
                securities_output = output_dir / f"Pictet_securities_{date}.xlsx"
                success = self.combine_securities_files(discovered_files['securities'], securities_output)
                results.append(success)
                
                if not success:
                    logger.error("❌ Failed to combine securities files")
            else:
                logger.info("ℹ️ No securities files to combine")
        
            # Process transactions file (pre-combined)
            if discovered_files['transactions']:
                transactions_output = output_dir / f"Pictet_transactions_{date}.xlsx"
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