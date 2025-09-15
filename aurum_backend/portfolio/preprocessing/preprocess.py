#!/usr/bin/env python3
"""
Unified Multi-Bank Preprocessing Script

This script automatically discovers and processes files from multiple banks (JPM, MS, etc.),
combines their outputs into unified securities and transactions files.

Features:
- Auto-discovery of banks and latest dates
- Sequential bank processing with error isolation
- In-memory DataFrame combination for efficiency
- Robust error handling with detailed logging
- Dry-run mode for testing
- Backup creation for existing files
- Extensible architecture for new banks

Usage:
    python preprocessing/preprocess.py                    # Process all banks automatically
    python preprocessing/preprocess.py --date 27_05_2025  # Process specific date
    python preprocessing/preprocess.py --banks JPM MS     # Process specific banks
    python preprocessing/preprocess.py --dry-run          # Show what would be processed
"""

import os
import sys
import argparse
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import pandas as pd
import numpy as np

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from preprocessing.bank_detector import BankDetector
from preprocessing.progress_tracker import ProgressTracker, BankProgressBox, ConversionProgressTracker, FileProgressTracker

# Configure logging with both console and file output
log_dir = Path(__file__).parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)

# Create file handler for preprocessing logs
log_file = log_dir / 'preprocessing_pipeline.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Simple formatter for console (keeping the existing format)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
console_handler.setFormatter(console_formatter)

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)


class UnifiedPreprocessor:
    """Unified preprocessor for multiple banks with robust error handling."""
    
    def __init__(self):
        """Initialize the unified preprocessor."""
        self.project_root = Path(__file__).parent.parent.parent
        self.supported_banks = ['JPM', 'MS', 'CSC', 'Pershing', 'CS', 'JB', 'HSBC', 'Valley', 'Safra', 'LO', 'IDB', 'Banchile', 'ALT', 'Citi', 'STDSZ']
        self.transformer_registry = {
            'JPM': 'preprocessing.transformers.jpm_transformer.JPMorganTransformer',
            'MS': 'preprocessing.transformers.ms_transformer.MorganStanleyTransformer',
            'CSC': 'preprocessing.transformers.csc_transformer.CSCTransformer',
            'Pershing': 'preprocessing.transformers.pershing_transformer.PershingTransformer',
            'CS': 'preprocessing.transformers.cs_transformer.CSTransformer',
            'JB': 'preprocessing.transformers.jb_transformer.JBTransformer',
            'HSBC': 'preprocessing.transformers.hsbc_transformer.HSBCTransformer',
            'Valley': 'preprocessing.transformers.valley_transformer.ValleyTransformer',
            'Safra': 'preprocessing.transformers.safra_transformer.SafraTransformer',
            'LO': 'preprocessing.transformers.lombard_transformer.LombardTransformer',
            'IDB': 'preprocessing.transformers.idb_transformer.IDBTransformer',
            'Banchile': 'preprocessing.transformers.banchile_transformer.BanchileTransformer',
            'Citi': 'preprocessing.transformers.citi_transformer.CitiTransformer',
            'STDSZ': 'preprocessing.transformers.stdsz_transformer.STDSZTransformer'
        }
        self.loaded_transformers = {}
        self.progress_tracker = ProgressTracker()
        logger.info("🚀 Unified preprocessor initialized")
        logger.info(f"📋 Supported banks: {', '.join(self.supported_banks)}")
    
    def discover_banks_and_dates(self, input_dir: Path) -> Dict[str, str]:
        """
        Discover available banks and their latest dates from input directory.
        
        Args:
            input_dir: Path to input directory containing bank files
            
        Returns:
            Dict mapping bank_code to latest_date_str
        """
        self.progress_tracker.start_operation("Scanning for bank files", "🔍")
        
        bank_files = {}
        
        # Get all Excel files for progress tracking
        excel_files = list(input_dir.glob("*.xlsx"))
        
        # Scan for bank files with progress bar
        file_progress = self.progress_tracker.create_progress_bar(
            total=len(excel_files),
            desc="Scanning files",
            unit="file"
        )
        
        for file in excel_files:
            if file.name == "Mappings.xlsx":
                file_progress.update(1)
                continue
                
            bank = BankDetector.detect_bank(file.name)
            date = BankDetector.extract_date_from_filename(file.name)
            
            if bank and date:
                if bank not in bank_files:
                    bank_files[bank] = set()
                bank_files[bank].add(date)
                logger.debug(f"  Found: {bank} file for date {date}")
            
            file_progress.update(1)
        
        file_progress.close()
        
        # Find latest date for each supported bank
        latest_dates = {}
        for bank, dates in bank_files.items():
            if bank in self.supported_banks:
                latest_date = max(dates, key=lambda x: tuple(map(int, x.split('_')[::-1])))
                latest_dates[bank] = latest_date
                self.progress_tracker.show_success(f"{bank}: Found {len(dates)} dates, using latest: {latest_date}")
            else:
                self.progress_tracker.show_warning(f"Unsupported bank found: {bank} (skipping)")
        
        if not latest_dates:
            self.progress_tracker.show_error("No supported bank files found")
        else:
            self.progress_tracker.show_success(f"Discovered {len(latest_dates)} banks ready for processing")
            self.progress_tracker.update_stats(banks_discovered=len(latest_dates))
        
        return latest_dates
    
    def discover_available_dates(self, input_dir: Path) -> List[str]:
        """
        Discover last 5 available dates from all bank files (main dir + subdirectories).
        
        Args:
            input_dir: Path to input directory containing bank files
            
        Returns:
            List of dates in DD_MM_YYYY format, sorted newest first (max 5)
        """
        all_dates = set()
        
        # Scan main input_files directory
        for file in input_dir.glob("*.xlsx"):
            if file.name == "Mappings.xlsx":
                continue
            date = BankDetector.extract_date_from_filename(file.name)
            if date:
                all_dates.add(date)
        
        # Scan subdirectories for files that need enrichment/combination
        subdirs = [
            'pershing/nonenriched_pershing', 
            'lombard/nonenriched_lombard', 
            'hsbc', 
            'idb', 
            'cs', 
            'csc', 
            'jb', 
            'valley'
        ]
        
        for subdir in subdirs:
            subdir_path = input_dir / subdir
            if subdir_path.exists():
                for file in subdir_path.glob("*.xlsx"):
                    date = BankDetector.extract_date_from_filename(file.name)
                    if date:
                        all_dates.add(date)
        
        # Sort dates (newest first) and return top 5
        if not all_dates:
            logger.warning("No dates found in any directory")
            return []
        
        sorted_dates = sorted(all_dates, key=lambda x: tuple(map(int, x.split('_')[::-1])), reverse=True)
        top_dates = sorted_dates[:5]
        
        logger.info(f"📅 Found {len(all_dates)} unique dates, returning top {len(top_dates)}: {top_dates}")
        return top_dates
    
    def _run_script_with_date(self, script_path: str, date: str, input_dir: str = None, output_dir: str = None) -> Dict[str, Any]:
        """
        Run a preprocessing script with date parameter.
        
        Args:
            script_path: Path to the script to run
            date: Date in DD_MM_YYYY format
            input_dir: Optional input directory override
            output_dir: Optional output directory override
            
        Returns:
            Dict with success status and execution details
        """
        try:
            import subprocess
            import time
            
            # Build command
            cmd = ['python3', script_path, '--date', date]
            
            # Handle bank-specific argument patterns
            if input_dir:
                # JB combiner expects --jb-dir instead of --input-dir
                if 'combine_jb.py' in script_path:
                    cmd.extend(['--jb-dir', input_dir])
                else:
                    # All other combiners use --input-dir
                    cmd.extend(['--input-dir', input_dir])
            
            if output_dir:
                cmd.extend(['--output-dir', output_dir])
            
            logger.info(f"  🔄 Running: {' '.join(cmd)}")
            start_time = time.time()
            
            # Detect if we're in HTTP request context to prevent broken pipe errors
            import threading
            is_http_context = hasattr(threading.current_thread(), 'name') and 'Thread' in threading.current_thread().name
            
            # Run the script - disable output capture during HTTP requests to prevent buffer overflow
            if is_http_context:
                result = subprocess.run(
                    cmd,
                    cwd=str(self.project_root),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=300  # 5 minute timeout
                )
            else:
                result = subprocess.run(
                    cmd,
                    cwd=str(self.project_root),
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout
                )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                logger.info(f"  ✅ Script completed successfully in {execution_time:.1f}s")
                return {
                    'success': True,
                    'execution_time': execution_time,
                    'output': result.stdout if hasattr(result, 'stdout') and result.stdout else '',
                    'error': result.stderr if hasattr(result, 'stderr') and result.stderr else ''
                }
            else:
                logger.error(f"  ❌ Script failed with return code {result.returncode}")
                logger.error(f"  Error output: {result.stderr}")
                return {
                    'success': False,
                    'execution_time': execution_time,
                    'output': result.stdout if hasattr(result, 'stdout') and result.stdout else '',
                    'error': result.stderr if hasattr(result, 'stderr') and result.stderr else '',
                    'return_code': result.returncode
                }
                
        except subprocess.TimeoutExpired:
            logger.error(f"  ❌ Script timed out after 5 minutes")
            return {
                'success': False,
                'execution_time': 300,
                'error': 'Script timed out after 5 minutes'
            }
        except Exception as e:
            logger.error(f"  ❌ Error running script: {str(e)}")
            return {
                'success': False,
                'execution_time': 0,
                'error': str(e)
            }
    
    def run_preprocessing_phase(self, date_str: str, input_dir: Path) -> Dict[str, Any]:
        """
        Run enrichers and combiners before main preprocessing.
        
        Args:
            date_str: Date in DD_MM_YYYY format
            input_dir: Input directory containing bank files
            
        Returns:
            Dict with results from enrichers and combiners
        """
        results = {
            'enrichers': {},
            'combiners': {},
            'errors': [],
            'skipped': [],
            'total_success': 0,
            'total_attempted': 0
        }
        
        logger.info("🚀 Starting pre-processing phase (enrichers + combiners)")
        
        # Phase 1: Enrichers (must run first to create files for combiners)
        logger.info("📋 Phase 1: Running Enrichers")
        enricher_configs = {
            'HSBC': {
                'script': 'portfolio/preprocessing/enrich_hsbc.py',
                'input_dir': str(input_dir / 'hsbc'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'hsbc'
            },
            'Lombard': {
                'script': 'portfolio/preprocessing/enrich_lombard.py', 
                'input_dir': str(input_dir / 'lombard' / 'nonenriched_lombard'),
                'output_dir': str(input_dir / 'lombard'),
                'check_path': input_dir / 'lombard' / 'nonenriched_lombard'
            },
            'Pershing': {
                'script': 'portfolio/preprocessing/enrich_pershing.py',
                'input_dir': str(input_dir / 'pershing' / 'nonenriched_pershing'),
                'output_dir': str(input_dir / 'pershing'),
                'check_path': input_dir / 'pershing' / 'nonenriched_pershing'
            },
            'IDB': {
                'script': 'portfolio/preprocessing/enrich_idb.py',
                'input_dir': str(input_dir / 'idb'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'idb'
            },
        }
        
        for bank, config in enricher_configs.items():
            results['total_attempted'] += 1
            
            # Check if files exist for this bank and date
            if not config['check_path'].exists():
                logger.info(f"  ⚠️ {bank}: No directory found at {config['check_path']}, skipping enrichment")
                results['skipped'].append(f"{bank} enricher - no directory")
                continue
            
            # Check for files with the specific date
            files_found = list(config['check_path'].glob(f"*{date_str}*.xlsx"))
            if not files_found:
                logger.info(f"  ⚠️ {bank}: No files found for date {date_str}, skipping enrichment")
                results['skipped'].append(f"{bank} enricher - no files for date")
                continue
            
            logger.info(f"  🔄 Running {bank} enricher...")
            result = self._run_script_with_date(
                config['script'], 
                date_str, 
                config['input_dir'], 
                config['output_dir']
            )
            
            results['enrichers'][bank] = result
            if result['success']:
                results['total_success'] += 1
                logger.info(f"  ✅ {bank} enrichment completed")
            else:
                logger.error(f"  ❌ {bank} enrichment failed: {result['error']}")
                results['errors'].append(f"{bank} enricher: {result['error']}")
        
        # Phase 2: Combiners (after enrichment is complete)
        logger.info("📋 Phase 2: Running Combiners")
        combiner_configs = {
            'CS': {
                'script': 'portfolio/preprocessing/combine_cs.py',
                'input_dir': str(input_dir / 'cs'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'cs'
            },
            'CSC': {
                'script': 'portfolio/preprocessing/combine_csc.py',
                'input_dir': str(input_dir / 'csc'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'csc'
            },
            'JB': {
                'script': 'portfolio/preprocessing/combine_jb.py',
                'input_dir': str(input_dir / 'jb'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'jb'
            },
            'Valley': {
                'script': 'portfolio/preprocessing/combine_valley.py',
                'input_dir': str(input_dir / 'valley'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'valley'
            },
            'Banchile': {
                'script': 'portfolio/preprocessing/combine_banchile.py',
                'input_dir': str(input_dir / 'banchile'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'banchile'
            },
            'Pershing': {
                'script': 'portfolio/preprocessing/combine_pershing.py',
                'input_dir': str(input_dir / 'pershing'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'pershing'
            },
            'Lombard': {
                'script': 'portfolio/preprocessing/combine_lombard.py',
                'input_dir': str(input_dir / 'lombard'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'lombard'
            },
            'IDB': {
                'script': 'portfolio/preprocessing/combine_idb.py',
                'input_dir': str(input_dir / 'idb'),
                'output_dir': str(input_dir),
                'check_path': input_dir / 'idb'
            }
        }
        
        for bank, config in combiner_configs.items():
            results['total_attempted'] += 1
            
            # Check if files exist for this bank and date
            if not config['check_path'].exists():
                logger.info(f"  ⚠️ {bank}: No directory found at {config['check_path']}, skipping combination")
                results['skipped'].append(f"{bank} combiner - no directory")
                continue
            
            # Check for files with the specific date
            files_found = list(config['check_path'].glob(f"*{date_str}*.xlsx"))
            if not files_found:
                logger.info(f"  ⚠️ {bank}: No files found for date {date_str}, skipping combination")
                results['skipped'].append(f"{bank} combiner - no files for date")
                continue
            
            logger.info(f"  🔄 Running {bank} combiner...")
            result = self._run_script_with_date(
                config['script'], 
                date_str, 
                config['input_dir'], 
                config['output_dir']
            )
            
            results['combiners'][bank] = result
            if result['success']:
                results['total_success'] += 1
                logger.info(f"  ✅ {bank} combination completed")
            else:
                logger.error(f"  ❌ {bank} combination failed: {result['error']}")
                results['errors'].append(f"{bank} combiner: {result['error']}")
        
        # Summary
        logger.info("📊 Pre-processing phase summary:")
        logger.info(f"  ✅ Successful: {results['total_success']}/{results['total_attempted']}")
        logger.info(f"  ⚠️ Skipped: {len(results['skipped'])}")
        logger.info(f"  ❌ Errors: {len(results['errors'])}")
        
        if results['skipped']:
            logger.info("  📋 Skipped operations:")
            for skip in results['skipped']:
                logger.info(f"    - {skip}")
        
        if results['errors']:
            logger.warning("  🚨 Error summary:")
            for error in results['errors']:
                logger.warning(f"    - {error}")
        
        return results
    
    def _bank_has_files(self, bank_code: str, date_str: str, input_dir: Path) -> bool:
        """Check if a bank has files for the specified date."""
        logger = logging.getLogger(__name__)
        logger.info(f"Checking files for bank: {bank_code}")
        logger.info(f"Input directory: {input_dir}")
        
        # For IDB, check in subdirectory
        if bank_code == 'IDB':
            idb_dir = input_dir / 'idb'
            if not idb_dir.exists():
                logger.info(f"IDB directory {idb_dir} does not exist")
                return False
            # Check if any IDB files exist for the date
            files_found = list(idb_dir.glob(f"*{date_str}*.xlsx"))
            logger.info(f"IDB files found for {date_str}: {[f.name for f in files_found]}")
            return len(files_found) > 0
        
        # For Banchile, check in subdirectory (files contain both securities and transactions in sheets)
        if bank_code == 'Banchile':
            banchile_dir = input_dir / 'banchile'
            if not banchile_dir.exists():
                logger.info(f"Banchile directory {banchile_dir} does not exist")
                return False
            # Check if any Banchile files exist for the date (each file contains both securities and transactions)
            files_found = list(banchile_dir.glob(f"*{date_str}*.xlsx"))
            logger.info(f"Banchile files found for {date_str}: {[f.name for f in files_found]}")
            return len(files_found) > 0
        
        # For other banks, check in main directory with case-insensitive patterns
        securities_patterns = [
            f"{bank_code}_Securities_{date_str}.xlsx",
            f"{bank_code}_securities_{date_str}.xlsx",
            f"{bank_code.title()}_Securities_{date_str}.xlsx",  # Added for case variations
            f"{bank_code.title()}_securities_{date_str}.xlsx"   # Added for case variations
        ]
        transactions_patterns = [
            f"{bank_code}_transactions_{date_str}.xlsx",
            f"{bank_code.title()}_transactions_{date_str}.xlsx"  # Added for case variations
        ]
        
        # Log the patterns we're looking for
        logger.info(f"Looking for securities patterns: {securities_patterns}")
        logger.info(f"Looking for transactions patterns: {transactions_patterns}")
        
        # Check if files exist and log results
        for pattern in securities_patterns:
            file_path = input_dir / pattern
            exists = file_path.exists()
            logger.info(f"Checking {file_path}: {'exists' if exists else 'not found'}")
        
        # Check transactions files
        transactions_exists = False
        for pattern in transactions_patterns:
            file_path = input_dir / pattern
            exists = file_path.exists()
            logger.info(f"Checking {file_path}: {'exists' if exists else 'not found'}")
            if exists:
                transactions_exists = True
                break
        
        # Check if at least one securities file exists
        securities_exists = any((input_dir / pattern).exists() for pattern in securities_patterns)
        
        logger.info(f"Final result for {bank_code}: securities_exists={securities_exists}, transactions_exists={transactions_exists}")
        
        return securities_exists or transactions_exists
    
    def load_transformer(self, bank_code: str):
        """
        Dynamically load and cache transformer for the specified bank.
        
        Args:
            bank_code: Bank code (e.g., 'JPM', 'MS')
            
        Returns:
            Transformer instance for the bank
        """
        if bank_code in self.loaded_transformers:
            return self.loaded_transformers[bank_code]
        
        if bank_code not in self.transformer_registry:
            raise ValueError(f"No transformer registered for bank: {bank_code}")
        
        try:
            # Dynamic import
            module_path = self.transformer_registry[bank_code]
            module_name, class_name = module_path.rsplit('.', 1)
            
            module = __import__(module_name, fromlist=[class_name])
            transformer_class = getattr(module, class_name)
            
            # Instantiate with API key for Valley and IDB transformers
            if bank_code in ['Valley', 'IDB']:
                # Get API key from Django settings or environment
                api_key = None
                try:
                    from django.conf import settings
                    api_key = getattr(settings, 'OPENFIGI_API_KEY', None)
                except ImportError:
                    # Fallback to environment variable if Django not available
                    import os
                    api_key = os.environ.get('OPENFIGI_API_KEY')
                
                if api_key:
                    transformer = transformer_class(api_key=api_key)
                    logger.info(f"📦 Loaded {bank_code} transformer with API key")
                else:
                    transformer = transformer_class()
                    logger.warning(f"⚠️ Loaded {bank_code} transformer without API key - may fail during processing")
            else:
                transformer = transformer_class()
            
            self.loaded_transformers[bank_code] = transformer
            
            logger.debug(f"📦 Loaded transformer for {bank_code}")
            return transformer
            
        except Exception as e:
            logger.error(f"❌ Failed to load transformer for {bank_code}: {str(e)}")
            raise
    
    def process_bank(self, bank_code: str, date_str: str, input_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process single bank with enhanced error handling.
        
        Args:
            bank_code: Bank code (e.g., 'JPM', 'MS', 'CSC')
            date_str: Date string in DD_MM_YYYY format
            input_dir: Input directory containing bank files
            
        Returns:
            Tuple of (securities_df, transactions_df)
        """
        logger.info(f"  🔄 Loading {bank_code} transformer...")
        transformer = self.load_transformer(bank_code)
        
        securities_df = pd.DataFrame()
        transactions_df = pd.DataFrame()
        
        # Banks that need mappings file vs those that already have bank/client/account columns
        banks_needing_mappings = ['JPM', 'MS', 'Safra', 'Citi']
        banks_with_embedded_data = ['CSC', 'Pershing', 'CS', 'JB', 'HSBC', 'Valley', 'LO', 'IDB', 'Banchile', 'STDSZ']
        
        mappings_file = None
        if bank_code in banks_needing_mappings:
            mappings_file = input_dir / "Mappings.xlsx"
            # Check if mappings file exists
            if not mappings_file.exists():
                raise FileNotFoundError(f"Required Mappings.xlsx file not found in {input_dir}")
        
        # Handle IDB special case - process from subdirectory
        if bank_code == 'IDB':
            idb_dir = input_dir / 'idb'
            
            # Find IDB securities file
            securities_file = None
            for file in idb_dir.glob(f"*securities*{date_str}*.xlsx"):
                securities_file = file
                break
            
            if securities_file:
                try:
                    logger.info(f"  📄 Processing {bank_code} securities file: {securities_file.name}")
                    securities_df = transformer.transform_securities(str(securities_file))
                    logger.info(f"  ✅ {bank_code} securities: {len(securities_df)} records processed")
                except Exception as e:
                    logger.error(f"  ❌ Error processing {bank_code} securities: {str(e)}")
                    logger.warning(f"  ⚠️ Continuing with empty securities data for {bank_code}")
                    # Continue with empty DataFrame
            else:
                logger.warning(f"  ⚠️ No securities file found for {bank_code} on date {date_str}")
            
            # Find IDB transactions file
            transactions_file = None
            for file in idb_dir.glob(f"*transactions*{date_str}*.xlsx"):
                transactions_file = file
                break
            
            if transactions_file:
                try:
                    logger.info(f"  📄 Processing {bank_code} transactions file: {transactions_file.name}")
                    transactions_df = transformer.transform_transactions(str(transactions_file))
                    logger.info(f"  ✅ {bank_code} transactions: {len(transactions_df)} records processed")
                except Exception as e:
                    logger.error(f"  ❌ Error processing {bank_code} transactions: {str(e)}")
                    logger.warning(f"  ⚠️ Continuing with empty transactions data for {bank_code}")
                    # Continue with empty DataFrame
            else:
                logger.warning(f"  ⚠️ No transactions file found for {bank_code} on date {date_str}")
        else:
            # Process securities file for other banks
            securities_patterns = [
                f"{bank_code}_Securities_{date_str}.xlsx",
                f"{bank_code}_securities_{date_str}.xlsx",
                f"{bank_code.title()}_Securities_{date_str}.xlsx",  # Added for case variations
                f"{bank_code.title()}_securities_{date_str}.xlsx"   # Added for case variations
            ]
            
            securities_file = None
            for pattern in securities_patterns:
                potential_file = input_dir / pattern
                if potential_file.exists():
                    securities_file = potential_file
                    break
            
            if securities_file:
                try:
                    logger.info(f"  📄 Processing {bank_code} securities file: {securities_file.name}")
                    if bank_code in banks_needing_mappings:
                        securities_df = transformer.transform_securities(str(securities_file), str(mappings_file))
                    else:
                        securities_df = transformer.transform_securities(str(securities_file))
                    logger.info(f"  ✅ {bank_code} securities: {len(securities_df)} records processed")
                except Exception as e:
                    logger.error(f"  ❌ Error processing {bank_code} securities: {str(e)}")
                    logger.warning(f"  ⚠️ Continuing with empty securities data for {bank_code}")
                    # Continue with empty DataFrame
            else:
                logger.warning(f"  ⚠️ No securities file found for {bank_code} on date {date_str}")
            
            # Process transactions file for other banks with case variations
            transactions_patterns = [
                f"{bank_code}_transactions_{date_str}.xlsx",
                f"{bank_code.title()}_transactions_{date_str}.xlsx"  # Added for case variations
            ]
            
            transactions_file = None
            for pattern in transactions_patterns:
                potential_file = input_dir / pattern
                if potential_file.exists():
                    transactions_file = potential_file
                    break
                    
            if transactions_file:
                try:
                    logger.info(f"  📄 Processing {bank_code} transactions file: {transactions_file.name}")
                    if bank_code in banks_needing_mappings:
                        transactions_df = transformer.transform_transactions(str(transactions_file), str(mappings_file))
                    elif bank_code == 'LO':
                        # Lombard needs securities data for CUSIP lookup - pass the already processed securities DataFrame
                        securities_file_for_cusip = securities_file if securities_file else input_dir / f"{bank_code}_securities_{date_str}.xlsx"
                        transactions_df = transformer.transform_transactions(str(transactions_file), str(securities_file_for_cusip), securities_df)
                    else:
                        transactions_df = transformer.transform_transactions(str(transactions_file))
                    logger.info(f"  ✅ {bank_code} transactions: {len(transactions_df)} records processed")
                except Exception as e:
                    logger.error(f"  ❌ Error processing {bank_code} transactions: {str(e)}")
                    logger.warning(f"  ⚠️ Continuing with empty transactions data for {bank_code}")
                    # Continue with empty DataFrame
            else:
                logger.warning(f"  ⚠️ No transactions file found for {bank_code} on date {date_str}")
        
        # Check if we got any data
        if securities_df.empty and transactions_df.empty:
            raise ValueError(f"No data could be processed for {bank_code} - both securities and transactions failed or missing")
        
        return securities_df, transactions_df
    
    def combine_dataframes(self, bank_results: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Combine DataFrames from all banks in memory.
        
        Args:
            bank_results: Dict mapping bank_code to (securities_df, transactions_df)
            
        Returns:
            Tuple of (combined_securities_df, combined_transactions_df)
        """
        logger.info("🔗 Combining data from all banks...")
        
        all_securities = []
        all_transactions = []
        
        for bank_code, (securities_df, transactions_df) in bank_results.items():
            logger.info(f"  📊 Adding {bank_code}: {len(securities_df)} securities, {len(transactions_df)} transactions")
            
            if not securities_df.empty:
                all_securities.append(securities_df)
            if not transactions_df.empty:
                all_transactions.append(transactions_df)
        
        # Combine DataFrames
        combined_securities = pd.concat(all_securities, ignore_index=True) if all_securities else pd.DataFrame()
        combined_transactions = pd.concat(all_transactions, ignore_index=True) if all_transactions else pd.DataFrame()
        
        logger.info(f"✅ Combined totals: {len(combined_securities)} securities, {len(combined_transactions)} transactions")
        
        return combined_securities, combined_transactions
    
    def create_backup_if_exists(self, file_path: Path, output_dir: Path) -> bool:
        """
        Create backup of existing file if it exists in a dedicated backups folder.
        
        Args:
            file_path: Path to file that might need backup
            output_dir: Output directory where backups folder will be created
            
        Returns:
            True if backup was created, False if no backup needed
        """
        if file_path.exists():
            # Create backups directory if it doesn't exist
            backups_dir = output_dir / "backups"
            backups_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{file_path.stem}.backup_{timestamp}{file_path.suffix}"
            backup_path = backups_dir / backup_filename
            
            shutil.copy2(file_path, backup_path)
            logger.info(f"💾 Created backup: backups/{backup_path.name}")
            return True
        return False
    
    def _is_numeric_text(self, value) -> bool:
        """
        Check if a value is numeric text (equivalent to VBA IsNumeric).
        Enhanced to handle ALL European formats, percentages, currency symbols, and spaces.
        
        Args:
            value: Value to check
            
        Returns:
            True if value represents a number, False otherwise
        """
        if pd.isna(value) or value == '' or value is None:
            return False
        
        # If already a number, it's numeric
        if isinstance(value, (int, float, np.number)):
            return not (isinstance(value, float) and np.isnan(value))
        
        if not isinstance(value, str):
            return False
        
        # Clean the string for testing
        cleaned = str(value).strip()
        if not cleaned:
            return False
        
        # Remove common currency symbols and percentage signs
        currency_symbols = ['€', '$', '£', '¥', '₹', 'CHF', 'USD', 'EUR', 'GBP']
        for symbol in currency_symbols:
            cleaned = cleaned.replace(symbol, '')
        
        # Remove percentage sign
        cleaned = cleaned.replace('%', '')
        
        # Remove spaces (used as thousands separators in some locales)
        cleaned = cleaned.replace(' ', '')
        
        # Strip again after removals
        cleaned = cleaned.strip()
        if not cleaned:
            return False
        
        # Try different number format patterns
        test_patterns = [
            # Original value
            cleaned,
            # European format: replace comma with period for decimal
            cleaned.replace(',', '.'),
            # Handle thousands separators: remove dots if comma is decimal separator
            self._handle_european_thousands_separator(cleaned),
            # Handle US format: remove commas if period is decimal separator
            self._handle_us_thousands_separator(cleaned),
        ]
        
        for pattern in test_patterns:
            if pattern and self._try_float_conversion(pattern):
                return True
        
        return False
    
    def _handle_european_thousands_separator(self, value: str) -> str:
        """Handle European format: 1.234,56 -> 1234.56"""
        if ',' in value and '.' in value:
            # If both comma and period, assume European format
            # Last comma should be decimal separator
            last_comma = value.rfind(',')
            last_period = value.rfind('.')
            
            if last_comma > last_period:
                # European format: 1.234,56
                # Remove all periods (thousands separators) and replace comma with period
                before_comma = value[:last_comma].replace('.', '')
                after_comma = value[last_comma + 1:]
                return f"{before_comma}.{after_comma}"
        
        return value
    
    def _handle_us_thousands_separator(self, value: str) -> str:
        """Handle US format: 1,234.56 -> 1234.56"""
        if ',' in value and '.' in value:
            # If both comma and period, check if it's US format
            last_comma = value.rfind(',')
            last_period = value.rfind('.')
            
            if last_period > last_comma:
                # US format: 1,234.56
                # Remove all commas (thousands separators)
                return value.replace(',', '')
        elif ',' in value and '.' not in value:
            # Only comma, could be thousands separator: 1,234 -> 1234
            # But be careful not to convert European decimals: 123,45
            comma_parts = value.split(',')
            if len(comma_parts) == 2:
                # Check if it looks like thousands separator (first part >= 3 digits)
                if len(comma_parts[0]) >= 3 and len(comma_parts[1]) == 3:
                    # Likely thousands separator: 1,234
                    return value.replace(',', '')
        
        return value
    
    def _try_float_conversion(self, value: str) -> bool:
        """Try to convert string to float, return True if successful."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _convert_numeric_text(self, value):
        """
        Convert numeric text to actual number (equivalent to VBA cell.Value = cell.Value * 1).
        Enhanced to handle ALL European formats, percentages, currency symbols, and spaces.
        
        Args:
            value: Numeric text value to convert
            
        Returns:
            Converted numeric value (float or int)
        """
        if pd.isna(value) or value == '' or value is None:
            return value
        
        # If already a number, ensure it's the right type
        if isinstance(value, (int, float, np.number)):
            if isinstance(value, float) and np.isnan(value):
                return value
            return float(value) if isinstance(value, np.number) else value
        
        if not isinstance(value, str):
            return value
        
        # Clean the string
        cleaned = str(value).strip()
        if not cleaned:
            return value
        
        # Track if it's a percentage
        is_percentage = '%' in cleaned
        
        # Remove common currency symbols and percentage signs
        currency_symbols = ['€', '$', '£', '¥', '₹', 'CHF', 'USD', 'EUR', 'GBP']
        for symbol in currency_symbols:
            cleaned = cleaned.replace(symbol, '')
        
        # Remove percentage sign
        cleaned = cleaned.replace('%', '')
        
        # Remove spaces (used as thousands separators)
        cleaned = cleaned.replace(' ', '')
        
        # Strip again after removals
        cleaned = cleaned.strip()
        if not cleaned:
            return value
        
        # Try conversion with different patterns
        converted_value = None
        
        # Try patterns in order of specificity
        patterns = [
            # European format with thousands separator: 1.234,56
            self._handle_european_thousands_separator(cleaned),
            # US format with thousands separator: 1,234.56
            self._handle_us_thousands_separator(cleaned),
            # Simple European decimal: 123,45 -> 123.45
            cleaned.replace(',', '.'),
            # Original cleaned value
            cleaned,
        ]
        
        for pattern in patterns:
            try:
                if pattern and self._try_float_conversion(pattern):
                    converted_value = float(pattern)
                    break
            except (ValueError, TypeError):
                continue
        
        if converted_value is None:
            logger.warning(f"Could not convert numeric text: {value}")
            return value
        
        # Apply percentage conversion
        if is_percentage:
            converted_value = converted_value / 100
        
        # Return as int if it's a whole number, otherwise float
        if converted_value == int(converted_value):
            return int(converted_value)
        else:
            return converted_value
    
    def convert_text_to_numbers(self, df: pd.DataFrame, target_columns: List[str]) -> pd.DataFrame:
        """
        Replicate VBA macro functionality: For each cell, if IsNumeric(cell.Value) then cell.Value = cell.Value * 1
        This converts text-formatted numbers to actual numeric values that Excel recognizes as numbers.
        
        Args:
            df: DataFrame to process
            target_columns: List of column names to convert
            
        Returns:
            DataFrame with converted numeric columns (actual float/int values)
        """
        if df.empty:
            return df
        
        df_copy = df.copy()
        
        # Filter to only columns that exist in the DataFrame
        existing_columns = [col for col in target_columns if col in df_copy.columns]
        
        if not existing_columns:
            return df_copy
        
        # Create progress tracker for conversion
        conversion_tracker = ConversionProgressTracker(len(existing_columns), self.progress_tracker)
        
        for col in existing_columns:
            col_converted = 0
            for idx in df_copy.index:
                cell_value = df_copy.at[idx, col]
                if self._is_numeric_text(cell_value):
                    df_copy.at[idx, col] = self._convert_numeric_text(cell_value)
                    col_converted += 1
            
            # Update progress tracker
            conversion_tracker.update(col_converted)
        
        # Complete conversion tracking
        conversion_tracker.complete()
        
        return df_copy
    
    def save_combined_files(self, securities_df: pd.DataFrame, transactions_df: pd.DataFrame, 
                          output_dir: Path, date_str: str):
        """
        Save combined DataFrames to Excel files with backup handling.
        
        Args:
            securities_df: Combined securities DataFrame
            transactions_df: Combined transactions DataFrame
            output_dir: Output directory
            date_str: Date string for filename
        """
        self.progress_tracker.start_operation("Saving combined files", "💾")
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define output file paths
        securities_file = output_dir / f"securities_{date_str}.xlsx"
        transactions_file = output_dir / f"transactions_{date_str}.xlsx"
        
        # Count files to save
        files_to_save = []
        if not securities_df.empty:
            files_to_save.append(("securities", securities_file, securities_df))
        if not transactions_df.empty:
            files_to_save.append(("transactions", transactions_file, transactions_df))
        
        if not files_to_save:
            self.progress_tracker.show_warning("No data to save")
            return
        
        # Create file progress tracker
        file_tracker = FileProgressTracker(len(files_to_save), "Saving files", self.progress_tracker)
        
        # SIMPLIFIED APPROACH: Apply Python conversion directly to DataFrames before saving
        # This converts text-formatted numbers to actual numbers that Excel recognizes
        
        for file_type, file_path, df in files_to_save:
            # Create backup if file exists
            self.create_backup_if_exists(file_path, output_dir)
            
            # Apply Python conversion based on file type
            if file_type == "securities":
                columns_to_convert = ['quantity', 'price', 'market_value', 'cost_basis', 'coupon_rate']
            else:  # transactions
                columns_to_convert = ['price', 'quantity', 'amount']
            
            converted_df = self.convert_text_to_numbers(df, columns_to_convert)
            
            # Save converted DataFrame to Excel
            converted_df.to_excel(file_path, index=False)
            
            # Update progress
            file_tracker.update(file_path.name, len(converted_df))
        
        # Complete file tracking
        file_tracker.complete()
    
    def process_all_banks(self, input_dir: Path, output_dir: Path, target_date: str = None, 
                         banks_filter: List[str] = None, dry_run: bool = False) -> bool:
        """
        Main orchestration method with robust error handling.
        
        Args:
            input_dir: Input directory containing bank files
            output_dir: Output directory for combined files
            target_date: Specific date to process (DD_MM_YYYY format)
            banks_filter: List of specific banks to process
            dry_run: If True, show what would be processed without actually processing
            
        Returns:
            True if processing was successful, False otherwise
        """
        logger.info("🚀 Starting unified bank preprocessing...")
        logger.info(f"📁 Input directory: {input_dir}")
        logger.info(f"📁 Output directory: {output_dir}")
        
        # Validate input directory
        if not input_dir.exists():
            logger.error(f"❌ Input directory does not exist: {input_dir}")
            return False
        
        # Auto-detect date if not provided
        if not target_date:
            logger.info("🔍 Auto-detecting latest available date...")
            available_dates = self.discover_available_dates(input_dir)
            if not available_dates:
                logger.error("❌ No dates found in uploaded files")
                return False
            target_date = available_dates[0]  # Use latest
            logger.info(f"📅 Auto-detected latest date: {target_date}")
        else:
            logger.info(f"🎯 Using specified date: {target_date}")
        
        # Step 1: Run pre-processing phase (enrichers + combiners)
        logger.info("🔄 Starting pre-processing phase (enrichers + combiners)")
        preprocessing_results = self.run_preprocessing_phase(target_date, input_dir)
        
        # Log preprocessing results for system monitoring
        logger.info("📊 Pre-processing phase completed:")
        logger.info(f"  ✅ Successful operations: {preprocessing_results['total_success']}")
        logger.info(f"  ⚠️ Skipped operations: {len(preprocessing_results['skipped'])}")
        logger.info(f"  ❌ Failed operations: {len(preprocessing_results['errors'])}")
        
        # Step 2: Discover banks and dates after preprocessing
        logger.info("🔍 Discovering banks ready for main preprocessing...")
        # Check which banks have files for the target date
        discovered_banks = {}
        for bank in self.supported_banks:
            if self._bank_has_files(bank, target_date, input_dir):
                discovered_banks[bank] = target_date
                logger.info(f"✅ {bank} has files for date {target_date}")
            else:
                logger.warning(f"⚠️ {bank} has no files for date {target_date}")
        
        # Apply bank filter if specified
        if banks_filter:
            logger.info(f"🎯 Filtering to specific banks: {', '.join(banks_filter)}")
            discovered_banks = {k: v for k, v in discovered_banks.items() if k in banks_filter}
        
        if not discovered_banks:
            logger.error("❌ No valid bank files found for processing")
            logger.info("💡 Make sure your input directory contains files like:")
            logger.info("   - JPM_securities_DD_MM_YYYY.xlsx")
            logger.info("   - MS_securities_DD_MM_YYYY.xlsx")
            logger.info("   - CSC_securities_DD_MM_YYYY.xlsx")
            logger.info("   - Pershing_securities_DD_MM_YYYY.xlsx")
            logger.info("   - CS_securities_DD_MM_YYYY.xlsx")
            logger.info("   - JB_securities_DD_MM_YYYY.xlsx")
            logger.info("   - HSBC_securities_DD_MM_YYYY.xlsx")
            logger.info("   - Valley_securities_DD_MM_YYYY.xlsx")
            logger.info("   - Safra_securities_DD_MM_YYYY.xlsx")
            logger.info("   - LO_securities_DD_MM_YYYY.xlsx")  # Added Lombard
            logger.info("   - IDB_securities_DD_MM_YYYY.xlsx")  # Added IDB
            logger.info("   - Citi_securities_DD_MM_YYYY.xlsx")
            logger.info("   - JPM_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - MS_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - CSC_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - Pershing_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - CS_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - JB_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - HSBC_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - Valley_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - Safra_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - LO_transactions_DD_MM_YYYY.xlsx")  # Added Lombard
            logger.info("   - Citi_transactions_DD_MM_YYYY.xlsx")
            logger.info("   - Mappings.xlsx")
            return False
        
        logger.info(f"📋 Banks ready for processing: {', '.join(discovered_banks.keys())}")
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No files will be processed")
            logger.info("📋 Would process the following:")
            for bank, date in discovered_banks.items():
                logger.info(f"  🏦 {bank} for date {date}")
            return True
        
        # Process banks sequentially (alphabetical order)
        self.progress_tracker.start_operation("Processing banks", "🏦")
        bank_results = {}
        successful_banks = []
        failed_banks = []
        
        for bank_code in sorted(discovered_banks.keys()):
            date_str = discovered_banks[bank_code]
            
            # Create detailed box-style progress indicator for this bank
            # For now, assume 2 files per bank (securities + transactions)
            bank_progress = self.progress_tracker.create_bank_progress_box(bank_code, date_str, 2)
            
            try:
                securities_df, transactions_df = self.process_bank(bank_code, date_str, input_dir)
                
                # Update progress with actual record counts
                securities_count = len(securities_df) if not securities_df.empty else 0
                transactions_count = len(transactions_df) if not transactions_df.empty else 0
                
                bank_progress.update_file_progress(securities_count, transactions_count)
                bank_progress.complete()
                
                bank_results[bank_code] = (securities_df, transactions_df)
                successful_banks.append(bank_code)
                
            except Exception as e:
                bank_progress.complete()
                self.progress_tracker.show_error(f"Failed to process {bank_code}: {str(e)}")
                failed_banks.append(bank_code)
                continue  # Skip failed bank, continue with others
        
        # Check if any banks were processed successfully
        if not bank_results:
            self.progress_tracker.show_error("No banks were processed successfully")
            return False
        
        # Combine results and save
        try:
            self.progress_tracker.start_operation("Combining results from successful banks", "🔗")
            combined_securities, combined_transactions = self.combine_dataframes(bank_results)
            
            # Use the date from the first successful bank for output filename
            output_date = discovered_banks[successful_banks[0]]
            
            self.save_combined_files(combined_securities, combined_transactions, output_dir, output_date)
            
            # Show final summary
            self.progress_tracker.show_final_summary()
            self.progress_tracker.show_success(f"Successfully processed banks: {', '.join(successful_banks)}")
            if failed_banks:
                self.progress_tracker.show_warning(f"Failed banks (fix and re-run): {', '.join(failed_banks)}")
            self.progress_tracker.show_success(f"Final output: {len(combined_securities):,} securities, {len(combined_transactions):,} transactions")
            
            return True
            
        except Exception as e:
            self.progress_tracker.show_error(f"Error combining or saving results: {str(e)}")
            return False


def main():
    """Main function with command-line interface."""
    parser = argparse.ArgumentParser(
        description='Unified multi-bank preprocessing script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python preprocessing/preprocess.py                    # Process all banks automatically
  python preprocessing/preprocess.py --date 27_05_2025  # Process specific date
  python preprocessing/preprocess.py --banks JPM MS     # Process specific banks
  python preprocessing/preprocess.py --dry-run          # Show what would be processed
        """
    )
    
    parser.add_argument('--input-dir', default='data/excel/input_files', 
                       help='Input directory containing bank files (default: data/excel/input_files)')
    parser.add_argument('--output-dir', default='data/excel', 
                       help='Output directory for combined files (default: data/excel)')
    parser.add_argument('--date', 
                       help='Specific date to process in DD_MM_YYYY format (e.g., 27_05_2025)')
    parser.add_argument('--banks', nargs='+', 
                       help='Specific banks to process (e.g., --banks JPM MS)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be processed without actually processing')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging for debugging')
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("🔧 Verbose logging enabled")
    
    try:
        # Convert paths to Path objects
        input_dir = Path(args.input_dir)
        output_dir = Path(args.output_dir)
        
        # Create and run preprocessor
        preprocessor = UnifiedPreprocessor()
        success = preprocessor.process_all_banks(
            input_dir=input_dir,
            output_dir=output_dir,
            target_date=args.date,
            banks_filter=args.banks,
            dry_run=args.dry_run
        )
        
        if success:
            logger.info("🎉 All done! Check the output directory for your combined files.")
            return 0
        else:
            logger.error("❌ Processing failed. Please check the error messages above.")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("⚠️ Processing interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main()) 