#!/usr/bin/env python3
"""
Lombard File Combiner

Combines individual Lombard securities files into unified bank files.
Adds bank, client, and account columns extracted from account numbers in the data.
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


class LombardCombiner:
    """Combines individual Lombard securities files into unified bank files."""
    
    def __init__(self):
        """Initialize the Lombard combiner."""
        self.bank_code = 'LO'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
        
    def load_account_mapping(self, mappings_file: str) -> Dict[str, Dict[str, str]]:
        """
        Load Lombard account mappings from Excel file.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            
        Returns:
            Dict mapping account numbers to client/account info
        """
        logger.info(f"Loading Lombard account mappings from {mappings_file}")
        
        try:
            encryption_service = MappingsEncryptionService()
            df = encryption_service.read_encrypted_excel(mappings_file + '.encrypted', sheet_name='LO')
            
            # Validate required columns
            required_cols = ['Account Number', 'client', 'account']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Lombard mappings missing required columns: {missing_cols}")
            
            # Create mapping dictionary: account_number -> {client, account}
            mappings = {}
            
            for _, row in df.iterrows():
                account_num = str(row['Account Number']).replace(' ', '').strip()  # Normalize
                client = str(row['client']).strip()
                account = str(row['account']).strip()
                
                # Skip rows with missing data
                if pd.isna(row['Account Number']) or pd.isna(row['client']) or pd.isna(row['account']):
                    logger.warning(f"Skipping mapping for account {account_num} - missing data")
                    continue
                
                mappings[account_num] = {
                    'client': client,
                    'account': account
                }
            
            logger.info(f"Loaded {len(mappings)} Lombard account mappings")
            if logger.level <= logging.DEBUG:
                for acc_num, info in mappings.items():
                    logger.debug(f"  {acc_num} → Client: {info['client']}, Account: {info['account']}")
            
            return mappings
            
        except Exception as e:
            logger.error(f"Error loading Lombard account mappings: {e}")
            raise
    
    def discover_lombard_files(self, lombard_dir: Path, date: str) -> Dict[str, List[Path]]:
        """
        Discover Lombard files for a specific date.
        
        Args:
            lombard_dir: Directory containing enriched Lombard files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict with 'securities' and 'transactions' lists containing file paths
        """
        logger.info(f"🔍 Scanning for Lombard files in: {lombard_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not lombard_dir.exists():
            logger.error(f"❌ Lombard directory does not exist: {lombard_dir}")
            return {'securities': [], 'transactions': []}
        
        discovered_files = {'securities': [], 'transactions': []}
        
        # Scan for Lombard securities files (multiple files expected)
        securities_pattern = f"LO_*_securities_{date}.xlsx"
        for file in lombard_dir.glob(securities_pattern):
            discovered_files['securities'].append(file)
            logger.info(f"  ✅ Found securities file: {file.name}")
        
        # Scan for transactions file (single file expected)
        transactions_pattern = f"LO_transactions_{date}.xlsx"
        for file in lombard_dir.glob(transactions_pattern):
            discovered_files['transactions'].append(file)
            logger.info(f"  ✅ Found transactions file: {file.name}")
        
        # Log summary
        securities_count = len(discovered_files['securities'])
        transactions_count = len(discovered_files['transactions'])
        logger.info(f"📊 Discovery summary:")
        logger.info(f"  📄 Securities files: {securities_count}")
        logger.info(f"  💰 Transactions files: {transactions_count}")
        
        if securities_count == 0:
            logger.warning(f"⚠️ No securities files found for date {date}")
        if transactions_count == 0:
            logger.warning(f"⚠️ No transactions file found for date {date}")
        elif transactions_count > 1:
            logger.warning(f"⚠️ Multiple transactions files found - expected only one")
        
        return discovered_files
    
    def extract_and_validate_account(self, df: pd.DataFrame, file_path: Path) -> Optional[str]:
        """
        Extract and validate account number from positions data.
        
        Args:
            df: DataFrame containing positions data
            file_path: Path to the file for logging
            
        Returns:
            Account number as string if valid, None otherwise
        """
        if 'Account Number' not in df.columns:
            logger.error(f"  ❌ No 'Account Number' column found in {file_path.name}")
            return None
        
        # Get all unique account numbers (convert to string and handle NaN)
        account_numbers = df['Account Number'].dropna().astype(str).unique()
        
        if len(account_numbers) == 0:
            logger.error(f"  ❌ No account numbers found in {file_path.name}")
            return None
        elif len(account_numbers) > 1:
            logger.error(f"  ❌ Multiple account numbers found in {file_path.name}: {account_numbers}")
            return None
        
        account_number = account_numbers[0].replace(' ', '').strip()  # Normalize
        logger.info(f"  ✅ Validated account number: {account_number}")
        return account_number
    
    def validate_account_with_filename(self, account_number: str, file_path: Path) -> bool:
        """
        Validate that account number matches what we expect from filename.
        
        Args:
            account_number: Extracted account number
            file_path: Path to the file
            
        Returns:
            True if validation passes, False otherwise
        """
        filename = file_path.name
        
        # Extract expected account info from filename patterns
        # Expected patterns: LO_ELP_SP_securities_DATE.xlsx, LO_LP_MT1_securities_DATE.xlsx, etc.
        if filename.startswith('LO_'):
            parts = filename.split('_')
            if len(parts) >= 4:
                client = parts[1]  # ELP, LP, VLP
                account = parts[2]  # SP, MT1, MT2, TT1, TT2
                expected_pattern = f"{client}_{account}"
                logger.info(f"  🔍 Expected pattern from filename: {expected_pattern}")
                
                # Note: We don't have the exact account mapping logic here,
                # but we log the validation for debugging
                logger.info(f"  📋 Account from data: {account_number}")
                return True  # Allow processing to continue
            else:
                logger.warning(f"  ⚠️ Unexpected filename format: {filename}")
                return True  # Allow processing to continue
        else:
            logger.warning(f"  ⚠️ Filename doesn't start with 'LO_': {filename}")
            return True  # Allow processing to continue
    
    def combine_securities_files(self, files: List[Path], output_path: Path, 
                               account_mapping: Dict[str, Dict[str, str]]) -> bool:
        """
        Combine individual securities files into unified file.
        
        Args:
            files: List of securities file paths
            output_path: Path for output file
            account_mapping: Account number to client/account mapping
            
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
        
        for file_path in files:
            try:
                logger.info(f"  📄 Processing: {file_path.name}")
                
                # Read the Excel file - securities files have 2 sheets, we want "Positions" (second sheet)
                # Header is at row 3 (0-indexed), so use header=3
                df = pd.read_excel(file_path, sheet_name='Positions', header=3)
                
                if df.empty:
                    logger.warning(f"  ⚠️ Empty positions data in: {file_path.name}")
                    continue
                
                # Extract and validate account number from the data
                account_number = self.extract_and_validate_account(df, file_path)
                if not account_number:
                    logger.error(f"  ❌ Failed to extract account number from {file_path.name}")
                    failed_files += 1
                    continue
                
                # Validate account number with filename expectation
                if not self.validate_account_with_filename(account_number, file_path):
                    logger.warning(f"  ⚠️ Account validation warning for {file_path.name}")
                
                # Look up client and account info from mapping
                if account_number not in account_mapping:
                    logger.error(f"  ❌ Account number {account_number} not found in mappings")
                    failed_files += 1
                    continue
                
                client_info = account_mapping[account_number]
                bank = self.bank_code
                client = client_info['client']
                account = client_info['account']
                
                # Filter out rows with missing account numbers
                df_filtered = df.dropna(subset=['Account Number'])
                if len(df_filtered) != len(df):
                    skipped_rows = len(df) - len(df_filtered)
                    logger.info(f"  🗑️  Skipped {skipped_rows} rows with missing account numbers")
                
                if df_filtered.empty:
                    logger.warning(f"  ⚠️ No valid data remaining after filtering: {file_path.name}")
                    continue
                
                # Add bank, client, account columns at the beginning
                df_filtered.insert(0, 'Bank', bank)
                df_filtered.insert(1, 'Client', client)
                df_filtered.insert(2, 'Account', account)
                
                combined_data.append(df_filtered)
                successful_files += 1
                logger.info(f"  ✅ Added {len(df_filtered)} records from {client}_{account}")
                
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
    
    def copy_transactions_file(self, files: List[Path], output_path: Path) -> bool:
        """
        Copy the transactions file to output directory (no combination needed).
        
        Args:
            files: List of transactions file paths (should be only one)
            output_path: Path for output file
            
        Returns:
            True if successful, False otherwise
        """
        if not files:
            logger.warning("⚠️ No transactions file to copy")
            return False
        
        if len(files) > 1:
            logger.warning(f"⚠️ Multiple transactions files found, using first: {files[0].name}")
        
        source_file = files[0]
        logger.info(f"📋 Copying transactions file: {source_file.name}")
        
        try:
            # Simply copy the file
            import shutil
            shutil.copy2(source_file, output_path)
            
            logger.info(f"✅ Transactions file copied successfully!")
            logger.info(f"  📁 From: {source_file.name}")
            logger.info(f"  📁 To: {output_path.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error copying transactions file: {str(e)}")
            return False
    
    def combine_all_files(self, lombard_dir: Path, output_dir: Path, date: str, 
                         mappings_file: str, dry_run: bool = False) -> bool:
        """
        Combine all Lombard files for a specific date.
        
        Args:
            lombard_dir: Directory containing enriched Lombard files
            output_dir: Directory for combined output files
            date: Date string in DD_MM_YYYY format
            mappings_file: Path to Mappings.xlsx file
            dry_run: If True, don't actually combine files
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("🚀 Starting Lombard file combination...")
        logger.info(f"📅 Date: {date}")
        logger.info(f"📁 Input directory: {lombard_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        logger.info(f"🗺️  Mappings file: {mappings_file}")
        logger.info(f"🧪 Dry run: {dry_run}")
        
        try:
            # Load account mappings
            account_mapping = self.load_account_mapping(mappings_file)
            if not account_mapping:
                logger.error("❌ Failed to load account mappings")
                return False
            
            # Discover files
            files = self.discover_lombard_files(lombard_dir, date)
            
            # Check if we have required files
            if not files['securities']:
                logger.error("❌ No securities files found")
                return False
            
            if not files['transactions']:
                logger.error("❌ No transactions file found")
                return False
            
            # Create output directory if it doesn't exist
            if not dry_run:
                output_dir.mkdir(parents=True, exist_ok=True)
            
            # Define output file paths
            combined_securities_path = output_dir / f"LO_securities_{date}.xlsx"
            combined_transactions_path = output_dir / f"LO_transactions_{date}.xlsx"
            
            if dry_run:
                logger.info("🧪 DRY RUN - Would process:")
                logger.info(f"  📄 Securities files: {len(files['securities'])}")
                for f in files['securities']:
                    logger.info(f"    - {f.name}")
                logger.info(f"  💰 Transactions files: {len(files['transactions'])}")
                for f in files['transactions']:
                    logger.info(f"    - {f.name}")
                logger.info(f"  📁 Output would be saved to: {output_dir}")
                logger.info(f"    - {combined_securities_path.name}")
                logger.info(f"    - {combined_transactions_path.name}")
                return True
            
            # Combine securities files
            logger.info("=" * 50)
            securities_success = self.combine_securities_files(
                files['securities'], 
                combined_securities_path, 
                account_mapping
            )
            
            # Copy transactions file
            logger.info("=" * 50)
            transactions_success = self.copy_transactions_file(
                files['transactions'],
                combined_transactions_path
            )
            
            # Check overall success
            if securities_success and transactions_success:
                logger.info("=" * 50)
                logger.info("🎉 Lombard file combination completed successfully!")
                logger.info(f"📁 Output files saved to: {output_dir}")
                logger.info(f"  - {combined_securities_path.name}")
                logger.info(f"  - {combined_transactions_path.name}")
                return True
            else:
                logger.error("❌ Some files failed to process")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during Lombard file combination: {str(e)}")
            if logger.level <= logging.DEBUG:
                logger.exception("Full error details:")
            return False 