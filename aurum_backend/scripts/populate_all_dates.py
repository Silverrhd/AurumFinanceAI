#!/usr/bin/env python3
"""
Automated Database Population Script for Sequential Date Processing

This script processes all available dates from May 29 to August 14, 2025,
exactly simulating manual button clicks but automated.

Runs database population one by one for each date in chronological order.
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime

# Add Django project to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')

import django
django.setup()

from portfolio.services.portfolio_population_service import PortfolioPopulationService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('populate_all_dates.log')
    ]
)

logger = logging.getLogger(__name__)

def get_available_dates():
    """Get all available dates with both securities and transactions files."""
    data_dir = project_root / "data" / "excel"
    
    available_dates = []
    
    # Find all securities files
    securities_files = list(data_dir.glob("securities_*_2025.xlsx"))
    
    for securities_file in securities_files:
        # Extract date from filename: securities_DD_MM_YYYY.xlsx
        filename = securities_file.name
        date_part = filename.replace('securities_', '').replace('_2025.xlsx', '')
        
        # Check if corresponding transactions file exists
        transactions_file = data_dir / f"transactions_{date_part}_2025.xlsx"
        
        if transactions_file.exists():
            available_dates.append(date_part)
            logger.info(f"Found complete dataset for date: {date_part}")
        else:
            logger.warning(f"Missing transactions file for date: {date_part}")
    
    # Sort dates chronologically (DD_MM format)
    def parse_date_key(date_str):
        day, month = date_str.split('_')
        return (int(month), int(day))
    
    available_dates.sort(key=parse_date_key)
    
    logger.info(f"Total available dates: {len(available_dates)}")
    logger.info(f"Dates in chronological order: {available_dates}")
    
    return available_dates

def populate_single_date(date_str):
    """
    Populate database for a single date.
    
    Args:
        date_str: Date in DD_MM format (e.g., '29_05')
    
    Returns:
        dict: Population results
    """
    logger.info(f"=" * 60)
    logger.info(f"STARTING POPULATION FOR DATE: {date_str}_2025")
    logger.info(f"=" * 60)
    
    data_dir = project_root / "data" / "excel"
    
    # Construct file paths
    securities_file = data_dir / f"securities_{date_str}_2025.xlsx"
    transactions_file = data_dir / f"transactions_{date_str}_2025.xlsx"
    
    # Verify files exist
    if not securities_file.exists():
        raise FileNotFoundError(f"Securities file not found: {securities_file}")
    
    if not transactions_file.exists():
        raise FileNotFoundError(f"Transactions file not found: {transactions_file}")
    
    # Convert date format: DD_MM to YYYY-MM-DD for Django
    day, month = date_str.split('_')
    django_date = f"2025-{month.zfill(2)}-{day.zfill(2)}"
    
    logger.info(f"Securities file: {securities_file}")
    logger.info(f"Transactions file: {transactions_file}")
    logger.info(f"Snapshot date: {django_date}")
    
    # Initialize population service
    service = PortfolioPopulationService()
    
    start_time = time.time()
    
    try:
        # Run population (exactly like clicking the button in the webapp)
        results = service.populate_from_excel(
            securities_file=str(securities_file),
            transactions_file=str(transactions_file),
            snapshot_date=django_date
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.info(f"POPULATION COMPLETED for {date_str}_2025")
        logger.info(f"Processing time: {processing_time:.2f} seconds")
        logger.info(f"Results: {results}")
        
        return {
            'date': date_str,
            'success': True,
            'processing_time': processing_time,
            'results': results,
            'error': None
        }
        
    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time
        
        logger.error(f"POPULATION FAILED for {date_str}_2025")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Processing time before failure: {processing_time:.2f} seconds")
        
        return {
            'date': date_str,
            'success': False,
            'processing_time': processing_time,
            'results': None,
            'error': str(e)
        }

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("AUTOMATED DATABASE POPULATION SCRIPT STARTED")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now()}")
    
    # Get all available dates
    available_dates = get_available_dates()
    
    if not available_dates:
        logger.error("No dates available for processing. Exiting.")
        return
    
    logger.info(f"Will process {len(available_dates)} dates sequentially")
    
    # Process each date sequentially
    all_results = []
    total_start_time = time.time()
    
    for i, date_str in enumerate(available_dates, 1):
        logger.info(f"\n>>> PROCESSING DATE {i}/{len(available_dates)}: {date_str}_2025")
        
        # Populate this date
        result = populate_single_date(date_str)
        all_results.append(result)
        
        if result['success']:
            logger.info(f"✅ SUCCESS: {date_str}_2025 completed in {result['processing_time']:.2f}s")
        else:
            logger.error(f"❌ FAILED: {date_str}_2025 failed after {result['processing_time']:.2f}s")
            logger.error(f"Error: {result['error']}")
        
        # Small delay between dates to prevent overwhelming the system
        if i < len(available_dates):
            logger.info("Waiting 2 seconds before next date...")
            time.sleep(2)
    
    # Final summary
    total_end_time = time.time()
    total_processing_time = total_end_time - total_start_time
    
    logger.info("\n" + "=" * 80)
    logger.info("AUTOMATED POPULATION COMPLETED")
    logger.info("=" * 80)
    
    successful = [r for r in all_results if r['success']]
    failed = [r for r in all_results if not r['success']]
    
    logger.info(f"Total processing time: {total_processing_time:.2f} seconds ({total_processing_time/60:.1f} minutes)")
    logger.info(f"Dates processed: {len(all_results)}")
    logger.info(f"Successful: {len(successful)}")
    logger.info(f"Failed: {len(failed)}")
    
    if successful:
        logger.info(f"\n✅ SUCCESSFUL DATES:")
        for result in successful:
            logger.info(f"  - {result['date']}_2025: {result['processing_time']:.2f}s")
    
    if failed:
        logger.error(f"\n❌ FAILED DATES:")
        for result in failed:
            logger.error(f"  - {result['date']}_2025: {result['error']}")
    
    # Summary statistics from successful runs
    if successful:
        total_clients = sum(r['results']['clients_processed'] for r in successful)
        total_assets = sum(r['results']['assets_created'] for r in successful)
        total_positions = sum(r['results']['positions_created'] for r in successful)
        total_transactions = sum(r['results']['transactions_created'] for r in successful)
        total_snapshots = sum(r['results']['snapshots_created'] for r in successful)
        
        logger.info(f"\n📊 AGGREGATE STATISTICS:")
        logger.info(f"  - Total clients processed: {total_clients}")
        logger.info(f"  - Total assets created: {total_assets}")
        logger.info(f"  - Total positions created: {total_positions}")
        logger.info(f"  - Total transactions created: {total_transactions}")
        logger.info(f"  - Total snapshots created: {total_snapshots}")
    
    logger.info(f"\nEnd time: {datetime.now()}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()