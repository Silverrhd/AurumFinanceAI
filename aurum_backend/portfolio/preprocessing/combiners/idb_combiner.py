"""
IDB File Combiner

Combines individual IDB client files into unified bank files.
Handles IDB-specific file naming patterns and data consolidation.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional
import re

logger = logging.getLogger(__name__)


class IDBCombiner:
    """Combiner for IDB client files into unified bank files."""
    
    def __init__(self):
        """Initialize IDB combiner."""
        logger.info("IDB combiner initialized")
    
    def combine_all_files(self, idb_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Combine individual IDB client files into unified bank files.
        
        Args:
            idb_dir: Directory containing individual IDB client files
            output_dir: Directory for combined output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without processing
            
        Returns:
            True if combination successful, False otherwise
        """
        try:
            logger.info(f"🔍 Scanning for IDB client files in {idb_dir}")
            
            # Find all IDB files for the specified date
            all_files = list(idb_dir.glob(f"*{date}*.xlsx"))
            
            if not all_files:
                logger.warning(f"⚠️ No IDB files found for date {date}")
                return True  # Not an error, just no files to process
            
            # Separate securities and transactions files
            securities_files = []
            transactions_files = []
            
            for file in all_files:
                if 'securities' in file.name.lower():
                    securities_files.append(file)
                elif 'transactions' in file.name.lower():
                    transactions_files.append(file)
                else:
                    logger.debug(f"🤔 Unrecognized file type: {file.name}")
            
            logger.info(f"📋 Found {len(securities_files)} securities files and {len(transactions_files)} transactions files")
            
            if dry_run:
                logger.info("🧪 DRY RUN - Would combine:")
                for file in securities_files:
                    logger.info(f"  📄 Securities: {file.name}")
                for file in transactions_files:
                    logger.info(f"  📄 Transactions: {file.name}")
                return True
            
            # Combine securities files
            if securities_files:
                success = self._combine_securities_files(securities_files, output_dir, date)
                if not success:
                    return False
            
            # Combine transactions files
            if transactions_files:
                success = self._combine_transactions_files(transactions_files, output_dir, date)
                if not success:
                    return False
            
            logger.info("🎉 IDB file combination completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during IDB file combination: {str(e)}")
            return False
    
    def _combine_securities_files(self, securities_files: List[Path], output_dir: Path, date: str) -> bool:
        """
        Combine multiple IDB securities files into one unified file.
        
        Args:
            securities_files: List of securities file paths
            output_dir: Output directory
            date: Date string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"🔄 Combining {len(securities_files)} securities files")
            
            combined_data = []
            
            for file in securities_files:
                logger.info(f"📖 Reading securities file: {file.name}")
                
                try:
                    # Read the Excel file
                    df = pd.read_excel(file)
                    
                    if df.empty:
                        logger.warning(f"⚠️ Empty file: {file.name}")
                        continue
                    
                    # Extract client info from filename if needed
                    # IDB files typically follow pattern: IDB_CLIENT_ACCOUNT_securities_DD_MM_YYYY.xlsx
                    client_info = self._extract_client_info(file.name)
                    
                    # Add client information if not already present
                    if 'bank' not in df.columns:
                        df['bank'] = 'IDB'
                    if 'client' not in df.columns and client_info.get('client'):
                        df['client'] = client_info['client']
                    if 'account' not in df.columns and client_info.get('account'):
                        df['account'] = client_info['account']
                    
                    combined_data.append(df)
                    logger.info(f"✅ Added {len(df)} records from {file.name}")
                    
                except Exception as e:
                    logger.error(f"❌ Error reading {file.name}: {str(e)}")
                    continue
            
            if not combined_data:
                logger.warning("⚠️ No securities data to combine")
                return True
            
            # Combine all DataFrames
            combined_df = pd.concat(combined_data, ignore_index=True)
            
            # Save combined file
            output_file = output_dir / f"IDB_securities_{date}.xlsx"
            combined_df.to_excel(output_file, index=False)
            
            logger.info(f"💾 Saved combined securities file: {output_file}")
            logger.info(f"📊 Total records: {len(combined_df)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error combining securities files: {str(e)}")
            return False
    
    def _combine_transactions_files(self, transactions_files: List[Path], output_dir: Path, date: str) -> bool:
        """
        Combine multiple IDB transactions files into one unified file.
        
        Args:
            transactions_files: List of transactions file paths
            output_dir: Output directory
            date: Date string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"🔄 Combining {len(transactions_files)} transactions files")
            
            combined_data = []
            
            for file in transactions_files:
                logger.info(f"📖 Reading transactions file: {file.name}")
                
                try:
                    # Read the Excel file
                    df = pd.read_excel(file)
                    
                    if df.empty:
                        logger.warning(f"⚠️ Empty file: {file.name}")
                        continue
                    
                    # Extract client info from filename if needed
                    client_info = self._extract_client_info(file.name)
                    
                    # Add client information if not already present
                    if 'bank' not in df.columns:
                        df['bank'] = 'IDB'
                    if 'client' not in df.columns and client_info.get('client'):
                        df['client'] = client_info['client']
                    if 'account' not in df.columns and client_info.get('account'):
                        df['account'] = client_info['account']
                    
                    combined_data.append(df)
                    logger.info(f"✅ Added {len(df)} records from {file.name}")
                    
                except Exception as e:
                    logger.error(f"❌ Error reading {file.name}: {str(e)}")
                    continue
            
            if not combined_data:
                logger.warning("⚠️ No transactions data to combine")
                return True
            
            # Combine all DataFrames
            combined_df = pd.concat(combined_data, ignore_index=True)
            
            # Save combined file
            output_file = output_dir / f"IDB_transactions_{date}.xlsx"
            combined_df.to_excel(output_file, index=False)
            
            logger.info(f"💾 Saved combined transactions file: {output_file}")
            logger.info(f"📊 Total records: {len(combined_df)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error combining transactions files: {str(e)}")
            return False
    
    def _extract_client_info(self, filename: str) -> Dict[str, str]:
        """
        Extract client and account information from IDB filename.
        
        Args:
            filename: IDB filename (e.g., 'IDB_JC_Datim_securities_10_07_2025.xlsx')
            
        Returns:
            Dict with client and account information
        """
        try:
            # Pattern: IDB_CLIENT_ACCOUNT_type_date.xlsx
            pattern = r'IDB_([^_]+)_([^_]+)_(?:securities|transactions)_\d{2}_\d{2}_\d{4}\.xlsx'
            match = re.match(pattern, filename)
            
            if match:
                return {
                    'client': match.group(1),
                    'account': match.group(2)
                }
            else:
                logger.debug(f"Could not extract client info from filename: {filename}")
                return {'client': 'Unknown', 'account': 'Unknown'}
                
        except Exception as e:
            logger.warning(f"Error extracting client info from {filename}: {str(e)}")
            return {'client': 'Unknown', 'account': 'Unknown'}