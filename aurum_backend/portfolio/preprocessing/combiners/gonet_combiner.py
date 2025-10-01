#!/usr/bin/env python3
"""
Gonet File Combiner

Combines individual Gonet client files into unified bank files.
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


class GonetCombiner:
    """Combines individual Gonet client files into unified bank files."""

    def __init__(self):
        """Initialize the Gonet combiner."""
        self.bank_code = 'Gonet'
        logger.info(f"🏦 Initialized {self.bank_code} file combiner")

    def discover_gonet_files(self, gonet_dir: Path, date: str) -> Dict[str, List[Dict]]:
        """
        Discover Gonet files for a specific date.

        Args:
            gonet_dir: Directory containing individual Gonet files
            date: Date string in DD_MM_YYYY format

        Returns:
            Dict with 'securities' and 'transactions' lists containing file info
        """
        logger.info(f"🔍 Scanning for Gonet files in: {gonet_dir}")
        logger.info(f"📅 Looking for date: {date}")

        if not gonet_dir.exists():
            logger.error(f"❌ Gonet directory does not exist: {gonet_dir}")
            return {'securities': [], 'transactions': []}

        discovered_files = {'securities': [], 'transactions': []}
        clients_found = set()

        # Scan for Gonet files
        for file in gonet_dir.glob("*.xlsx"):
            # Check if file matches Gonet pattern and date
            if not file.name.startswith('Gonet_'):
                logger.debug(f"  Skipping non-Gonet file: {file.name}")
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

        logger.info(f"🔗 Combining {len(files)} Gonet securities files...")

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

        logger.info(f"🔗 Combining {len(files)} Gonet transactions files...")

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

    def combine_all_files(self, gonet_dir: Path, output_dir: Path, date: str, dry_run: bool = False) -> bool:
        """
        Main method to combine all Gonet files for a specific date.

        Args:
            gonet_dir: Directory containing individual Gonet files
            output_dir: Directory for output files
            date: Date string in DD_MM_YYYY format
            dry_run: If True, show what would be processed without actually processing

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"🚀 Starting Gonet file combination for date: {date}")
        logger.info(f"📁 Input directory: {gonet_dir}")
        logger.info(f"📁 Output directory: {output_dir}")

        try:
            # Discover files
            discovered_files = self.discover_gonet_files(gonet_dir, date)

            if not discovered_files['securities'] and not discovered_files['transactions']:
                logger.error(f"❌ No Gonet files found for date {date}")
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

            # Combine securities files
            if discovered_files['securities']:
                securities_output = output_dir / f"Gonet_securities_{date}.xlsx"
                success = self.combine_securities_files(discovered_files['securities'], securities_output)
                results.append(success)

                if not success:
                    logger.error("❌ Failed to combine securities files")
            else:
                logger.info("ℹ️ No securities files to combine")

            # Combine transactions files
            if discovered_files['transactions']:
                transactions_output = output_dir / f"Gonet_transactions_{date}.xlsx"
                success = self.combine_transactions_files(discovered_files['transactions'], transactions_output)
                results.append(success)

                if not success:
                    logger.error("❌ Failed to combine transactions files")
            else:
                logger.info("ℹ️ No transactions files to combine")

            # Overall success
            overall_success = all(results) if results else False

            if overall_success:
                logger.info("🎉 Gonet file combination completed successfully!")
            else:
                logger.error("❌ Gonet file combination completed with errors")

            return overall_success

        except Exception as e:
            logger.error(f"❌ Error during Gonet file combination: {str(e)}")
            return False
