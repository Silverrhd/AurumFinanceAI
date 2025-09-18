#!/usr/bin/env python3
"""
Santander Switzerland (STDSZ) File Combiner

Simple pass-through combiner that takes enriched STDSZ files and prepares them for transformation.
Adds client and account columns extracted from filename and moves to input_files directory.

Currently handles single client (EI_Mazal) but designed to be future-ready for multiple clients.

Input: santander_switzerland/STDSZ_EI_Mazal_securities_31_07_2025.xlsx
Output: input_files/STDSZ_securities_31_07_2025.xlsx (with Client and Account columns added)

Author: Generated for Project Aurum
Date: 2025-01-15
"""

import os
import sys
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.bank_detector import BankDetector

logger = logging.getLogger(__name__)


class STDSZCombiner:
    """Combines enriched STDSZ securities and transactions files."""
    
    def __init__(self):
        """Initialize the STDSZ combiner."""
        self.bank_code = 'STDSZ'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")
    
    def discover_enriched_files(self, input_dir: Path, date: str) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        Discover enriched STDSZ files for a specific date and group by client/account.
        
        Args:
            input_dir: Directory containing enriched STDSZ files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping client_account to file paths
        """
        try:
            discovered_files = {}
            
            if not input_dir.exists():
                logger.warning(f"📁 Input directory does not exist: {input_dir}")
                return discovered_files
            
            # Pattern for enriched STDSZ files: STDSZ_EI_Mazal_securities_31_07_2025.xlsx
            pattern = re.compile(r'^STDSZ_([A-Za-z0-9]+)_([A-Za-z0-9]+)_(securities|transactions)_(\d{2}_\d{2}_\d{4})\.xlsx?$')
            
            for file_path in input_dir.glob("*.xlsx"):
                match = pattern.match(file_path.name)
                if match:
                    client, account, file_type, file_date = match.groups()
                    
                    if file_date == date:
                        client_account = f"{client}_{account}"
                        
                        if client_account not in discovered_files:
                            discovered_files[client_account] = {
                                'securities': None,
                                'transactions': None,
                                'client': client,
                                'account': account
                            }
                        
                        discovered_files[client_account][file_type] = file_path
                        logger.info(f"📁 Found {file_type} file for {client_account}: {file_path.name}")
            
            logger.info(f"📊 Discovered {len(discovered_files)} client/account combinations for date {date}")
            return discovered_files
            
        except Exception as e:
            logger.error(f"❌ Error discovering enriched STDSZ files: {e}")
            return {}
    
    def combine_client_files(self, client_files: Dict[str, Dict[str, Optional[Path]]], output_dir: Path, date: str) -> Tuple[Optional[Path], Optional[Path]]:
        """
        Combine individual client files into unified securities and transactions files.
        For now, this is a simple pass-through with client/account column addition.
        
        Args:
            client_files: Dict mapping client_account to file paths
            output_dir: Output directory for combined files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Tuple of (combined_securities_path, combined_transactions_path)
        """
        try:
            logger.info(f"🔄 Combining STDSZ files for date {date}")
            
            combined_securities_data = []
            combined_transactions_data = []
            
            # Process each client/account
            for client_account, files in client_files.items():
                client = files['client']
                account = files['account']
                logger.info(f"📊 Processing client/account: {client_account} (Client: {client}, Account: {account})")
                
                # Process securities file
                if files['securities'] and files['securities'].exists():
                    securities_df = pd.read_excel(files['securities'])
                    
                    # Add client and account columns
                    securities_df['Client'] = client
                    securities_df['Account'] = account
                    
                    # Reorder columns to put Client and Account first
                    cols = ['Client', 'Account'] + [col for col in securities_df.columns if col not in ['Client', 'Account']]
                    securities_df = securities_df[cols]
                    
                    logger.info(f"   📈 Loaded securities: {len(securities_df)} rows, added Client={client}, Account={account}")
                    combined_securities_data.append(securities_df)
                else:
                    logger.warning(f"   ⚠️ No securities file found for {client_account}")
                
                # Process transactions file (when available)
                if files['transactions'] and files['transactions'].exists():
                    transactions_df = pd.read_excel(files['transactions'])
                    
                    # Add client and account columns
                    transactions_df['Client'] = client
                    transactions_df['Account'] = account
                    
                    # Reorder columns to put Client and Account first
                    cols = ['Client', 'Account'] + [col for col in transactions_df.columns if col not in ['Client', 'Account']]
                    transactions_df = transactions_df[cols]
                    
                    logger.info(f"   💰 Loaded transactions: {len(transactions_df)} rows, added Client={client}, Account={account}")
                    combined_transactions_data.append(transactions_df)
                else:
                    logger.info(f"   ℹ️ No transactions file for {client_account}")
            
            # Combine securities data
            securities_output_path = None
            if combined_securities_data:
                combined_securities_df = pd.concat(combined_securities_data, ignore_index=True)
                securities_output_path = output_dir / f"STDSZ_securities_{date}.xlsx"
                combined_securities_df.to_excel(securities_output_path, index=False, engine='openpyxl')
                logger.info(f"✅ Combined securities saved: {securities_output_path}")
                logger.info(f"   Total rows: {len(combined_securities_df)}")
                logger.info(f"   Clients: {combined_securities_df['Client'].unique().tolist()}")
                logger.info(f"   Accounts: {combined_securities_df['Account'].unique().tolist()}")
            
            # Combine transactions data
            transactions_output_path = None
            if combined_transactions_data:
                combined_transactions_df = pd.concat(combined_transactions_data, ignore_index=True)
                transactions_output_path = output_dir / f"STDSZ_transactions_{date}.xlsx"
                combined_transactions_df.to_excel(transactions_output_path, index=False, engine='openpyxl')
                logger.info(f"✅ Combined transactions saved: {transactions_output_path}")
                logger.info(f"   Total rows: {len(combined_transactions_df)}")
            
            return securities_output_path, transactions_output_path
            
        except Exception as e:
            logger.error(f"❌ Error combining client files: {e}")
            return None, None
    
    def combine_stdsz_files(self, input_dir: Path, date: str) -> Dict[str, Path]:
        """
        Main combination function for STDSZ files.
        
        Args:
            input_dir: Directory containing enriched STDSZ files
            date: Date string in DD_MM_YYYY format
            
        Returns:
            Dict mapping file types to output paths
        """
        try:
            logger.info(f"🚀 Starting STDSZ combination for date: {date}")
            
            # Set up output directory (navigate up to find the main input_files directory)
            output_dir = input_dir
            while output_dir.name != 'input_files' and output_dir.parent != output_dir:
                output_dir = output_dir.parent
            
            if output_dir.name != 'input_files':
                # Fallback: assume we're in input_files and go up one level
                output_dir = input_dir.parent
            
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Discover enriched files
            discovered_files = self.discover_enriched_files(input_dir, date)
            
            if not discovered_files:
                logger.warning(f"⚠️ No enriched STDSZ files found for date {date}")
                return {}
            
            # Combine client files (currently just pass-through with client/account addition)
            securities_path, transactions_path = self.combine_client_files(
                discovered_files, 
                output_dir, 
                date
            )
            
            # Build result
            result = {}
            if securities_path:
                result['securities'] = securities_path
            if transactions_path:
                result['transactions'] = transactions_path
            
            logger.info(f"🎉 STDSZ combination completed! Generated {len(result)} files")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ STDSZ combination failed: {e}")
            return {}


def main():
    """Main function for running STDSZ combination from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='STDSZ File Combiner')
    parser.add_argument('--date', required=True, help='Date in DD_MM_YYYY format')
    parser.add_argument('--input-dir', required=True, help='Input directory containing enriched STDSZ files')
    parser.add_argument('--output-dir', help='Output directory (optional, defaults to input_files)')
    
    # Support both named arguments (from webapp) and positional arguments (backwards compatibility)
    if len(sys.argv) == 3 and not sys.argv[1].startswith('--'):
        # Legacy positional arguments: python stdsz_combiner.py 31_07_2025 /path/to/santander_switzerland
        date = sys.argv[1]
        input_dir = Path(sys.argv[2])
    else:
        # Named arguments: python stdsz_combiner.py --date 31_07_2025 --input-dir /path/to/santander_switzerland
        args = parser.parse_args()
        date = args.date
        input_dir = Path(args.input_dir)
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run combination
    combiner = STDSZCombiner()
    result = combiner.combine_stdsz_files(input_dir, date)
    
    if result:
        print(f"✅ STDSZ combination completed successfully!")
        for file_type, path in result.items():
            print(f"   {file_type}: {path}")
    else:
        print(f"❌ STDSZ combination failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()