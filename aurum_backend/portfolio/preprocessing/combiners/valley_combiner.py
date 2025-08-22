#!/usr/bin/env python3
"""
Valley File Combiner

Combines individual Valley client files into unified bank files.
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


class ValleyCombiner:
    """Combines individual Valley client files into unified bank files."""
    
    def __init__(self):
        """Initialize the Valley combiner."""
        self.bank_code = 'Valley'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
    
    def discover_valley_files(self, valley_dir: Path, date: str) -> Dict[str, List[Dict]]:
        """
        Discover Valley files for a specific date.
        
        Args:
            valley_dir: Directory containing individual Valley files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict with 'securities' and 'transactions' lists containing file info
        """
        logger.info(f"🔍 Scanning for Valley files in: {valley_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not valley_dir.exists():
            logger.error(f"❌ Valley directory does not exist: {valley_dir}")
            return {'securities': [], 'transactions': []}
        
        discovered_files = {'securities': [], 'transactions': []}
        clients_found = set()
        
        # Scan for Valley files
        for file in valley_dir.glob("*.xlsx"):
            # Check if file matches Valley pattern and date
            if not file.name.startswith('Valley_'):
                logger.debug(f"  Skipping non-Valley file: {file.name}")
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
            if 'Securities' in file.name or 'securities' in file.name:
                file_type = 'securities'
            elif 'transactions' in file.name:
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
                logger.info(f"  💰 Processing: {client}_{account} -> {file_path.name}")
                
                # Read the Excel file with header=1 for Valley transactions files
                # (they have a header row above the column titles)
                df = pd.read_excel(file_path, header=1)
                
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
    
    def combine_all_files(self, valley_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Main method to combine all Valley files for a specific date.
        
        Args:
            valley_dir: Directory containing individual Valley files
            output_dir: Directory for output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without actually processing
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting Valley file combination for date: {date}")
        logger.info(f"📁 Input directory: {valley_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        
        # Discover files
        discovered_files = self.discover_valley_files(valley_dir, date)
        
        if not discovered_files['securities'] and not discovered_files['transactions']:
            logger.error(f"❌ No Valley files found for date {date}")
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
        securities_output = output_dir / f"Valley_securities_{date}.xlsx"
        transactions_output = output_dir / f"Valley_transactions_{date}.xlsx"
        
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
            logger.info("🎉 Valley file combination completed successfully!")
        else:
            logger.error("❌ Valley file combination completed with errors")
        
        return success 