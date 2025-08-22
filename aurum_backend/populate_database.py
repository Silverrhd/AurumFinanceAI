#!/usr/bin/env python3
"""
Single Database Population Script for Aurum Finance

This script populates the Django database from Excel files with smart date extraction.
Supports multiple dates, incremental updates, and database clearing.

Usage:
    python3 populate_database.py --date 10_07_2025
    python3 populate_database.py --date 10_07_2025 --date 17_07_2025
    python3 populate_database.py --clear --date 10_07_2025
    python3 populate_database.py --status
"""

import os
import sys
import django
import argparse
import glob
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')
django.setup()

from portfolio.services.portfolio_population_service import PortfolioPopulationService
from portfolio.services.portfolio_calculation_service import PortfolioCalculationService
from portfolio.preprocessing.bank_detector import BankDetector
from portfolio.models import Client, Asset, Position, Transaction, PortfolioSnapshot

def convert_date_format(date_str):
    """
    Convert DD_MM_YYYY format to YYYY-MM-DD format for database.
    
    Args:
        date_str: Date in DD_MM_YYYY format (e.g., "10_07_2025")
        
    Returns:
        Date in YYYY-MM-DD format (e.g., "2025-07-10")
    """
    try:
        day, month, year = date_str.split('_')
        return f"{year}-{month}-{day}"
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected DD_MM_YYYY format.")

def find_available_dates():
    """
    Find all available dates by scanning for securities files in data/excel/.
    
    Returns:
        List of available dates in DD_MM_YYYY format
    """
    securities_files = glob.glob("data/excel/securities_*.xlsx")
    dates = []
    
    for file_path in securities_files:
        filename = os.path.basename(file_path)
        # Extract date using BankDetector
        date_str = BankDetector.extract_date_from_filename(filename)
        if date_str:
            dates.append(date_str)
    
    return sorted(dates)

def validate_date_files(date_str):
    """
    Validate that both securities and transactions files exist for the given date.
    
    Args:
        date_str: Date in DD_MM_YYYY format
        
    Returns:
        Tuple of (securities_file_path, transactions_file_path)
        
    Raises:
        FileNotFoundError: If either file is missing
    """
    securities_file = f"data/excel/securities_{date_str}.xlsx"
    transactions_file = f"data/excel/transactions_{date_str}.xlsx"
    
    if not os.path.exists(securities_file):
        raise FileNotFoundError(f"Securities file not found: {securities_file}")
    
    if not os.path.exists(transactions_file):
        raise FileNotFoundError(f"Transactions file not found: {transactions_file}")
    
    return securities_file, transactions_file

def clear_database():
    """
    Clear all portfolio data from the database.
    """
    print("🗑️  Clearing database...")
    
    # Delete in correct order to avoid foreign key constraints
    deleted_snapshots = PortfolioSnapshot.objects.all().delete()
    deleted_transactions = Transaction.objects.all().delete()
    deleted_positions = Position.objects.all().delete()
    deleted_assets = Asset.objects.all().delete()
    deleted_clients = Client.objects.all().delete()
    
    print(f"  - Deleted {deleted_snapshots[0]} snapshots")
    print(f"  - Deleted {deleted_transactions[0]} transactions")
    print(f"  - Deleted {deleted_positions[0]} positions")
    print(f"  - Deleted {deleted_assets[0]} assets")
    print(f"  - Deleted {deleted_clients[0]} clients")
    print("✅ Database cleared successfully")

def populate_date(date_str):
    """
    Populate database with data from the specified date.
    
    Args:
        date_str: Date in DD_MM_YYYY format
        
    Returns:
        Boolean indicating success
    """
    try:
        # Validate files exist
        securities_file, transactions_file = validate_date_files(date_str)
        
        # Convert date format for database
        snapshot_date = convert_date_format(date_str)
        
        print(f"\n📊 Processing date: {date_str} (snapshot: {snapshot_date})")
        print(f"  📄 Securities: {securities_file}")
        print(f"  📄 Transactions: {transactions_file}")
        
        # Check if this date already exists
        existing_snapshots = PortfolioSnapshot.objects.filter(snapshot_date=snapshot_date).count()
        if existing_snapshots > 0:
            print(f"  ⚠️  Warning: {existing_snapshots} snapshots already exist for {snapshot_date}")
            response = input("  Continue anyway? (y/N): ")
            if response.lower() != 'y':
                print("  ⏭️  Skipping this date")
                return True
        
        # Step 1: Populate from Excel
        print("  📋 Step 1: Populating from Excel files...")
        population_service = PortfolioPopulationService()
        results = population_service.populate_from_excel(
            securities_file, transactions_file, snapshot_date
        )
        
        print(f"  ✅ Population Results:")
        print(f"    - Clients processed: {results['clients_processed']}")
        print(f"    - Assets created: {results['assets_created']}")
        print(f"    - Positions created: {results['positions_created']}")
        print(f"    - Transactions created: {results['transactions_created']}")
        print(f"    - Snapshots created: {results['snapshots_created']}")
        
        # Show client details
        for client_code, details in results['client_details'].items():
            print(f"    📊 Client {client_code}:")
            print(f"      - Securities: {details['securities_processed']}")
            print(f"      - Transactions: {details['transactions_processed']}")
            print(f"      - Assets created: {details['assets_created']}")
            print(f"      - Positions created: {details['positions_created']}")
        
        # Step 2: Calculate metrics for each client
        print(f"  🔢 Step 2: Calculating portfolio metrics...")
        calculation_service = PortfolioCalculationService()
        
        successful_calculations = 0
        failed_calculations = 0
        total_portfolio_value = 0
        
        for client in Client.objects.all():
            try:
                metrics = calculation_service.calculate_portfolio_metrics(
                    client.code, snapshot_date
                )
                
                total_value = metrics['total_value']
                annual_income = metrics['estimated_annual_income']
                position_count = metrics['position_count']
                asset_types = len(metrics['asset_allocation'])
                period_return = metrics.get('period_return', 0.0)
                
                total_portfolio_value += total_value
                
                print(f"    ✅ {client.code}: ${total_value:,.2f} (return: {period_return:.4f}%)")
                print(f"      - Annual Income: ${annual_income:,.2f}")
                print(f"      - Positions: {position_count}")
                print(f"      - Asset Types: {asset_types}")
                
                # Show top asset allocations
                if metrics['asset_allocation']:
                    print(f"      - Top Asset Types:")
                    sorted_allocations = sorted(
                        metrics['asset_allocation'].items(),
                        key=lambda x: x[1]['value'],
                        reverse=True
                    )[:3]
                    for asset_type, data in sorted_allocations:
                        print(f"        * {asset_type}: ${data['value']:,.2f} ({data['percentage']:.1f}%)")
                
                successful_calculations += 1
                
            except Exception as e:
                print(f"    ❌ {client.code}: Error - {str(e)[:50]}...")
                failed_calculations += 1
                continue
        
        print(f"  📊 Calculation Summary: {successful_calculations} successful, {failed_calculations} failed")
        print(f"  💰 Total Portfolio Value: ${total_portfolio_value:,.2f}")
        
        # Step 3: Update dashboard cache asynchronously
        if successful_calculations > 0:
            print(f"  ⚡ Step 3: Updating dashboard cache...")
            try:
                from portfolio.services.dashboard_cache_service import DashboardCacheService
                
                cache_service = DashboardCacheService()
                cache_result = cache_service.process_new_date(converted_date)
                
                if cache_result['success']:
                    if cache_result.get('skipped'):
                        print(f"    ℹ️  Cache already up to date for {converted_date}")
                    else:
                        processing_time = cache_result.get('processing_time', 0)
                        client_count = cache_result.get('client_count', 0)
                        print(f"    ✅ Cache updated: {client_count} clients processed in {processing_time:.2f}s")
                else:
                    print(f"    ⚠️  Cache update failed: {cache_result.get('error', 'unknown error')}")
                    print(f"    ℹ️  Dashboard will use real-time calculation (slower but accurate)")
                    
            except Exception as e:
                print(f"    ⚠️  Cache update error: {str(e)}")
                print(f"    ℹ️  Dashboard will use real-time calculation (slower but accurate)")
                # Continue - cache failure doesn't affect data integrity
        
        return True
        
    except Exception as e:
        print(f"  ❌ ERROR processing {date_str}: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_database_status():
    """
    Show current database status.
    """
    print("\n📊 Current Database Status:")
    print(f"  - Clients: {Client.objects.count()}")
    print(f"  - Assets: {Asset.objects.count()}")
    print(f"  - Positions: {Position.objects.count()}")
    print(f"  - Transactions: {Transaction.objects.count()}")
    print(f"  - Snapshots: {PortfolioSnapshot.objects.count()}")
    
    # Show snapshot dates
    snapshots = PortfolioSnapshot.objects.values('snapshot_date').distinct().order_by('snapshot_date')
    if snapshots:
        print(f"\n📅 Snapshot dates in database:")
        for snap in snapshots:
            count = PortfolioSnapshot.objects.filter(snapshot_date=snap['snapshot_date']).count()
            print(f"  - {snap['snapshot_date']}: {count} client snapshots")
    else:
        print("\n📅 No snapshots in database")

def main():
    """
    Main function with command line argument parsing.
    """
    parser = argparse.ArgumentParser(
        description="Populate Django database from Excel files with smart date extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --date 10_07_2025                    # Populate with single date
  %(prog)s --date 10_07_2025 --date 17_07_2025  # Populate with multiple dates
  %(prog)s --clear --date 10_07_2025            # Clear database and populate
  %(prog)s --status                             # Show database status
"""
    )
    
    parser.add_argument(
        '--date', 
        action='append',
        help='Date to process in DD_MM_YYYY format (can be used multiple times)'
    )
    
    parser.add_argument(
        '--clear', 
        action='store_true',
        help='Clear database before populating'
    )
    
    parser.add_argument(
        '--status', 
        action='store_true',
        help='Show current database status'
    )
    
    args = parser.parse_args()
    
    # If no arguments provided, show help and available dates
    if not any([args.date, args.clear, args.status]):
        parser.print_help()
        print("\n📅 Available dates:")
        available_dates = find_available_dates()
        if available_dates:
            for date in available_dates:
                print(f"  - {date}")
        else:
            print("  No Excel files found in data/excel/")
        return 0
    
    print("🚀 Aurum Finance Database Population")
    print("=" * 50)
    
    # Handle status command
    if args.status:
        show_database_status()
        return 0
    
    # Handle clear command
    if args.clear:
        clear_database()
    
    # Handle date processing
    if args.date:
        success_count = 0
        total_count = len(args.date)
        
        for date_str in args.date:
            print(f"\n{'='*50}")
            print(f"Processing Date {success_count + 1}/{total_count}: {date_str}")
            print(f"{'='*50}")
            
            if populate_date(date_str):
                success_count += 1
            else:
                print(f"❌ Failed to process {date_str}")
        
        # Final summary
        print(f"\n{'='*50}")
        print(f"📊 Final Summary")
        print(f"{'='*50}")
        
        if success_count == total_count:
            print(f"🎉 SUCCESS! All {total_count} dates processed successfully.")
        else:
            print(f"⚠️  PARTIAL SUCCESS: {success_count}/{total_count} dates processed.")
        
        show_database_status()
        
        return 0 if success_count == total_count else 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())