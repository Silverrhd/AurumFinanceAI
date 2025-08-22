#!/usr/bin/env python3
"""
Lombard Data Enricher

Enriches Lombard transaction files by filtering and combining transactions and cashmovements.
Removes "Withdrawal" transactions and adds valid cashmovements.
Uses account mappings from Mappings.xlsx to map account numbers to client/account info.
"""

import os
import sys
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import shutil

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.bank_detector import BankDetector

logger = logging.getLogger(__name__)


class LombardEnricher:
    """Enriches Lombard transaction files by filtering and combining transactions and cashmovements."""
    
    def __init__(self):
        """Initialize the Lombard enricher."""
        self.bank_code = 'LO'
        logger.info(f"🏦 Initialized {self.bank_code} data enricher")
    
    def load_account_mappings(self, mappings_file: str) -> Dict[str, Dict[str, str]]:
        """
        Load Lombard account mappings from Excel file.
        
        Args:
            mappings_file: Path to Mappings.xlsx
            
        Returns:
            Dict mapping account numbers to client/account info
        """
        logger.info(f"Loading Lombard account mappings from {mappings_file}")
        
        try:
            df = pd.read_excel(mappings_file, sheet_name='LO')
            
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
    
    def normalize_account_number(self, account_str: str) -> str:
        """
        Normalize account number by removing spaces.
        
        Args:
            account_str: Account number string
            
        Returns:
            Normalized account number
        """
        return str(account_str).replace(' ', '').strip()
    
    def discover_lombard_files(self, input_dir: Path, date: str) -> Dict[str, Optional[Path]]:
        """
        Discover Lombard files for a specific date.
        
        Args:
            input_dir: Directory containing Lombard files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping file types to paths
        """
        logger.info(f"🔍 Scanning for Lombard files in: {input_dir}")
        logger.info(f"📅 Looking for date: {date}")
        
        if not input_dir.exists():
            logger.error(f"❌ Input directory does not exist: {input_dir}")
            return {}
        
        files = {
            'transactions': None,
            'cashmovements': None,
            'securities': []  # Multiple securities files
        }
        
        # Scan for Lombard files (support both .xlsx and .xls)
        for pattern in ["*.xlsx", "*.xls"]:
            for file in input_dir.glob(pattern):
                # Check if file matches Lombard pattern and date
                if not file.name.startswith('LO_'):
                    logger.debug(f"  Skipping non-Lombard file: {file.name}")
                    continue
                
                # Extract date from filename
                file_date = BankDetector.extract_date_from_filename(file.name)
                if file_date != date:
                    logger.debug(f"  Skipping file with different date: {file.name} (date: {file_date})")
                    continue
                
                # Determine file type
                if f'transactions_{date}' in file.name:
                    files['transactions'] = file
                    logger.info(f"  ✅ Found transactions file: {file.name}")
                elif f'cashmovements_{date}' in file.name:
                    files['cashmovements'] = file
                    logger.info(f"  ✅ Found cashmovements file: {file.name}")
                elif f'securities_{date}' in file.name:
                    files['securities'].append(file)
                    logger.info(f"  ✅ Found securities file: {file.name}")
                else:
                    logger.debug(f"  Unknown Lombard file type: {file.name}")
                    continue
        
        # Log summary and validate required files
        logger.info(f"📊 Discovery summary:")
        transactions_status = "✅" if files['transactions'] else "❌"
        cashmovements_status = "✅" if files['cashmovements'] else "❌"
        securities_count = len(files['securities'])
        securities_status = "✅" if securities_count > 0 else "❌"
        
        logger.info(f"  💰 Transactions: {transactions_status}")
        logger.info(f"  💸 Cashmovements: {cashmovements_status}")
        logger.info(f"  📄 Securities: {securities_status} ({securities_count} files)")
        
        # Check if we have required files
        missing_files = []
        if not files['transactions']:
            missing_files.append("transactions")
        if not files['cashmovements']:
            missing_files.append("cashmovements")
        if not files['securities']:
            missing_files.append("securities")
        
        if missing_files:
            logger.error(f"❌ Missing required files: {missing_files}")
        
        return files
    
    def process_transactions(self, input_dir: Path, date: str, account_mapping: Dict[str, Dict[str, str]], 
                           dry_run: bool = False) -> pd.DataFrame:
        """
        Process and combine LO_transactions and LO_cashmovements files.
        
        This is the core Lombard enrichment logic:
        1. Remove ALL "Withdrawal" transactions from LO_transactions file
        2. Add "Deposit", "Withdrawal", "Fees" from LO_cashmovements (exclude "Income")
        3. Map account numbers to Bank="LO", Client, Account using mappings
        4. STANDARDIZE COLUMNS: Map Description → Position for cashmovements
        5. ADD MISSING COLUMNS: Add transaction-specific columns with NaN for cashmovements
        
        Args:
            input_dir: Input directory containing files
            date: Date string in DD_MM_YYYY format
            account_mapping: Account number to client/account mapping
            dry_run: If True, don't actually process, just show what would be done
            
        Returns:
            Combined and filtered transactions DataFrame with standardized columns
        """
        logger.info("🔄 Processing and combining Lombard transactions...")
        
        transactions_file = input_dir / f"LO_transactions_{date}.xlsx"
        cashmovements_file = input_dir / f"LO_cashmovements_{date}.xlsx"
        
        # Load transactions file and filter out withdrawals
        logger.info(f"📄 Processing transactions file: {transactions_file.name}")
        if not transactions_file.exists():
            logger.error(f"❌ Transactions file not found: {transactions_file}")
            raise FileNotFoundError(f"Transactions file not found: {transactions_file}")
        
        # Read transactions file with header=3 (as discovered in analysis)
        transactions_df = pd.read_excel(transactions_file, header=3)
        original_transactions_count = len(transactions_df)
        logger.info(f"  📊 Loaded {original_transactions_count} original transactions")
        
        # Filter OUT all "Withdrawal" entries from transactions
        if 'Transaction' in transactions_df.columns:
            withdrawal_count = len(transactions_df[transactions_df['Transaction'] == 'Withdrawal'])
            transactions_df = transactions_df[transactions_df['Transaction'] != 'Withdrawal']
            logger.info(f"  🗑️  Removed {withdrawal_count} 'Withdrawal' entries from transactions")
            logger.info(f"  📊 Remaining transactions: {len(transactions_df)}")
        else:
            logger.warning(f"  ⚠️  No 'Transaction' column found in transactions file")
        
        # Load cashmovements file and filter for specific transaction types
        logger.info(f"💰 Processing cashmovements file: {cashmovements_file.name}")
        if not cashmovements_file.exists():
            logger.error(f"❌ Cashmovements file not found: {cashmovements_file}")
            raise FileNotFoundError(f"Cashmovements file not found: {cashmovements_file}")
        
        # Read cashmovements file with header=3
        cashmovements_df = pd.read_excel(cashmovements_file, header=3)
        original_cashmovements_count = len(cashmovements_df)
        logger.info(f"  📊 Loaded {original_cashmovements_count} original cashmovements")
        
        # Filter for only "Deposit", "Withdrawal", "Fees" (exclude "Income")
        allowed_types = ['Deposit', 'Withdrawal', 'Fees']
        if 'Transaction' in cashmovements_df.columns:
            cashmovements_filtered = cashmovements_df[cashmovements_df['Transaction'].isin(allowed_types)]
            
            # Log what we found and what we're keeping
            all_types = cashmovements_df['Transaction'].unique()
            kept_types = cashmovements_filtered['Transaction'].unique()
            excluded_types = set(all_types) - set(kept_types)
            
            logger.info(f"  🔍 Found transaction types: {list(all_types)}")
            logger.info(f"  ✅ Keeping types: {list(kept_types)} ({len(cashmovements_filtered)} entries)")
            if excluded_types:
                excluded_count = len(cashmovements_df[cashmovements_df['Transaction'].isin(excluded_types)])
                logger.info(f"  🚫 Excluding types: {list(excluded_types)} ({excluded_count} entries)")
        else:
            logger.warning(f"  ⚠️  No 'Transaction' column found in cashmovements file")
            cashmovements_filtered = pd.DataFrame()  # Empty DataFrame
        
        # COLUMN STANDARDIZATION: Map Description → Position for cashmovements
        if not cashmovements_filtered.empty and 'Description' in cashmovements_filtered.columns:
            logger.info("🔧 Standardizing cashmovements columns...")
            
            # Map Description → Position
            cashmovements_filtered = cashmovements_filtered.copy()
            cashmovements_filtered['Position'] = cashmovements_filtered['Description']
            logger.info(f"  ✅ Mapped Description → Position for {len(cashmovements_filtered)} cashmovements")
            
            # Add missing transaction-specific columns with NaN values
            transaction_specific_columns = ['Quantity', 'Price', 'Price currency']
            for col in transaction_specific_columns:
                if col not in cashmovements_filtered.columns:
                    cashmovements_filtered[col] = pd.NA
                    logger.info(f"  ➕ Added missing column '{col}' with NaN values")
            
            logger.info("  🎯 Column standardization completed for cashmovements")
        
        if dry_run:
            logger.info("🧪 DRY RUN - Would combine:")
            logger.info(f"  📄 Transactions (after filtering): {len(transactions_df)} rows")
            logger.info(f"  💰 Cashmovements (after filtering & standardization): {len(cashmovements_filtered)} rows")
            logger.info(f"  📊 Total combined rows: {len(transactions_df) + len(cashmovements_filtered)}")
            return pd.DataFrame()  # Return empty for dry run
        
        # Combine the filtered data
        combined_data = []
        
        # Add filtered transactions with account mapping
        logger.info("📍 Processing transactions with account mapping...")
        for _, row in transactions_df.iterrows():
            # Account number is in column O (Unnamed: 14) for transactions
            account_col = 'Unnamed: 14'
            if account_col in row and pd.notna(row[account_col]):
                account_normalized = self.normalize_account_number(row[account_col])
                
                if account_normalized in account_mapping:
                    mapping_info = account_mapping[account_normalized]
                    row_dict = row.to_dict()
                    row_dict['Bank'] = self.bank_code
                    row_dict['Client'] = mapping_info['client']
                    row_dict['Account'] = mapping_info['account']
                    row_dict['AccountNumber'] = account_normalized
                    combined_data.append(row_dict)
                    
                    if logger.level <= logging.DEBUG:
                        trans_type = row.get('Transaction', 'Unknown')
                        logger.debug(f"  📍 Mapped transaction {trans_type} account {row[account_col]} → {mapping_info}")
                else:
                    logger.warning(f"  ⚠️  Transaction account {account_normalized} not found in mapping")
            else:
                logger.warning(f"  ⚠️  Transaction row missing account number in column {account_col}")
        
        # Add filtered cashmovements with account mapping (now with standardized columns)
        logger.info("📍 Processing cashmovements with account mapping...")
        for _, row in cashmovements_filtered.iterrows():
            # Account number is in column M (Unnamed: 12) for cashmovements
            account_col = 'Unnamed: 12'
            if account_col in row and pd.notna(row[account_col]):
                account_normalized = self.normalize_account_number(row[account_col])
                
                if account_normalized in account_mapping:
                    mapping_info = account_mapping[account_normalized]
                    row_dict = row.to_dict()
                    row_dict['Bank'] = self.bank_code
                    row_dict['Client'] = mapping_info['client']
                    row_dict['Account'] = mapping_info['account']
                    row_dict['AccountNumber'] = account_normalized
                    combined_data.append(row_dict)
                    
                    if logger.level <= logging.DEBUG:
                        trans_type = row.get('Transaction', 'Unknown')
                        position = row.get('Position', 'Unknown')
                        logger.debug(f"  📍 Mapped cashmovement {trans_type} (Position: {position}) account {row[account_col]} → {mapping_info}")
                else:
                    logger.warning(f"  ⚠️  Cashmovement account {account_normalized} not found in mapping")
            else:
                logger.warning(f"  ⚠️  Cashmovement row missing account number in column {account_col}")
        
        logger.info(f"🎯 Successfully combined {len(combined_data)} total transactions")
        logger.info(f"  📄 From transactions (after removing withdrawals): {len(transactions_df)} rows")
        logger.info(f"  💰 From cashmovements (filtered & standardized): {len(cashmovements_filtered)} rows")
        logger.info("  ✅ All rows now have standardized column structure with Position column")
        
        return pd.DataFrame(combined_data)
    
    def copy_securities_files(self, input_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Copy securities files unchanged from input to output directory.
        
        Args:
            input_dir: Input directory
            output_dir: Output directory
            date: Date string
            dry_run: If True, don't actually copy
            
        Returns:
            True if successful
        """
        logger.info("📄 Copying securities files unchanged...")
        
        # Find all securities files for the date
        securities_pattern = f"LO_*_securities_{date}.xlsx"
        securities_files = list(input_dir.glob(securities_pattern))
        
        if not securities_files:
            logger.error(f"❌ No securities files found matching pattern: {securities_pattern}")
            return False
        
        logger.info(f"📄 Found {len(securities_files)} securities files to copy")
        
        if dry_run:
            logger.info("🧪 DRY RUN - Would copy securities files:")
            for file in securities_files:
                logger.info(f"  📄 {file.name}")
            return True
        
        # Copy each securities file
        copied_count = 0
        for file in securities_files:
            try:
                dest_file = output_dir / file.name
                shutil.copy2(file, dest_file)
                copied_count += 1
                
                if logger.level <= logging.DEBUG:
                    logger.debug(f"  ✅ Copied: {file.name}")
                    
            except Exception as e:
                logger.error(f"❌ Error copying {file.name}: {e}")
                return False
        
        logger.info(f"✅ Successfully copied {copied_count} securities files")
        return True
    
    def enrich_lombard_files(self, input_dir: Path, output_dir: Path, date: str, 
                           mappings_file: str, dry_run: bool = False) -> bool:
        """
        Main enrichment function for Lombard data.
        
        This implements the complete Lombard enrichment process:
        1. Load account mappings from Mappings.xlsx LO sheet
        2. Process transactions: remove withdrawals from LO_transactions
        3. Add filtered cashmovements (Deposit, Withdrawal, Fees - exclude Income)
        4. Add Bank="LO", Client, Account columns using mapping
        5. Copy securities files unchanged
        
        Args:
            input_dir: Input directory containing raw files
            output_dir: Output directory for enriched files
            date: Date string in DD_MM_YYYY format
            mappings_file: Path to Mappings.xlsx file
            dry_run: If True, don't actually process files
            
        Returns:
            True if successful
        """
        logger.info(f"🚀 Starting Lombard data enrichment for date: {date}")
        logger.info(f"📁 Input directory: {input_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        
        try:
            # Discover required files
            files = self.discover_lombard_files(input_dir, date)
            if not files['transactions'] or not files['cashmovements'] or not files['securities']:
                logger.error("❌ Required files not found")
                return False
            
            # Load account mapping
            logger.info("📋 Loading account mapping from Mappings.xlsx LO sheet...")
            account_mapping = self.load_account_mappings(mappings_file)
            
            # Process transactions (core Lombard enrichment logic)
            combined_transactions = self.process_transactions(input_dir, date, account_mapping, dry_run)
            
            if not dry_run:
                # Save enriched transactions file
                output_transactions_file = output_dir / f"LO_transactions_{date}.xlsx"
                combined_transactions.to_excel(output_transactions_file, index=False)
                logger.info(f"💾 Saved enriched transactions: {output_transactions_file.name}")
                logger.info(f"  📊 Contains {len(combined_transactions)} total enriched transactions")
            
            # Copy securities files unchanged
            if not self.copy_securities_files(input_dir, output_dir, date, dry_run):
                return False
            
            logger.info("✅ Lombard data enrichment completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during Lombard enrichment: {e}")
            if logger.level <= logging.DEBUG:
                import traceback
                logger.debug(f"Full traceback: {traceback.format_exc()}")
            return False 