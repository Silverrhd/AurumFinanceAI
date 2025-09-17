#!/usr/bin/env python3
"""
Universal Client-Bank Integration System
Handles: New clients, existing clients, new banks, existing banks

Usage:
    # New client scenario  
    python universal_client_integrator.py --client EI --bank STDSZ --date 29_05_2025
    
    # Existing client + new bank scenario
    python universal_client_integrator.py --client ELP --bank PICTET --date 29_05_2025
    
    # Batch processing
    python universal_client_integrator.py --client EI --bank STDSZ --date-range 29_05_2025:11_09_2025
"""

import os
import sys
import django
import argparse
import logging
from pathlib import Path
from datetime import datetime
from django.db import transaction

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')
django.setup()

from portfolio.models import Client, Asset, Position, Transaction, PortfolioSnapshot
from portfolio.parsers.excel_parser import StatementParser, TransactionParser
from portfolio.services.portfolio_calculation_service import PortfolioCalculationService
from portfolio.services.correct_dashboard_cache_service import CorrectDashboardCacheService

class UniversalClientIntegrator:
    """Universal integration system supporting all client-bank scenarios"""
    
    def __init__(self, client_code: str, bank_code: str):
        self.client_code = client_code
        self.bank_code = bank_code
        self.calculation_service = PortfolioCalculationService()
        self.cache_service = CorrectDashboardCacheService()
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        self.mode = self._determine_integration_mode()
    
    def _determine_integration_mode(self) -> str:
        """Auto-detect whether to use replacement or additive mode"""
        try:
            client = Client.objects.get(code=self.client_code)
            has_snapshots = PortfolioSnapshot.objects.filter(client=client).exists()
            
            if has_snapshots:
                self.logger.info(f"Client {self.client_code} exists with data → ADDITIVE mode")
                return "additive"
            else:
                self.logger.info(f"Client {self.client_code} exists but no snapshots → REPLACEMENT mode")
                return "replacement"
                
        except Client.DoesNotExist:
            self.logger.info(f"Client {self.client_code} doesn't exist → REPLACEMENT mode")
            return "replacement"
    
    @transaction.atomic
    def integrate_single_date(self, date_str: str) -> dict:
        """Integrate one date with smart mode detection"""
        
        self.logger.info(f"🔄 Integrating {self.client_code} + {self.bank_code} for {date_str}")
        self.logger.info(f"🔧 Using {self.mode.upper()} integration mode")
        
        # Step 1: Validate processed files exist
        securities_file = Path(f"data/excel/securities_{date_str}.xlsx")
        transactions_file = Path(f"data/excel/transactions_{date_str}.xlsx")
        
        if not securities_file.exists() or not transactions_file.exists():
            raise FileNotFoundError(f"Missing processed files for {date_str}: {securities_file}, {transactions_file}")
        
        # Step 2: Apply appropriate integration strategy
        if self.mode == "replacement":
            result = self._replacement_integration(date_str, securities_file, transactions_file)
        else:
            result = self._additive_integration(date_str, securities_file, transactions_file)
        
        # Step 3: Calculate portfolio metrics
        metrics = self.calculation_service.calculate_portfolio_metrics(
            client_code=self.client_code,
            snapshot_date=self._convert_date_format(date_str)
        )
        
        result['portfolio_metrics'] = {
            'total_value': metrics['total_value'],
            'position_count': metrics['position_count'],
            'estimated_annual_income': metrics['estimated_annual_income']
        }
        
        self.logger.info(f"   📊 Portfolio Value: ${metrics['total_value']:,.2f}")
        
        # Step 4: Update dashboard cache
        cache_result = self.cache_service.aggregate_date_data(date_str)
        result['cache_updated'] = cache_result['success']
        
        if cache_result['success']:
            self.logger.info(f"   ✅ Dashboard cache updated for {date_str}")
        else:
            self.logger.warning(f"   ⚠️ Dashboard cache warning: {cache_result.get('error')}")
        
        return result
    
    def _replacement_integration(self, date_str: str, securities_file: Path, transactions_file: Path) -> dict:
        """Complete replacement integration (for new clients or full refresh)"""
        
        # Use existing population service (deletes all positions, creates fresh)
        from portfolio.services.portfolio_population_service import PortfolioPopulationService
        
        population_service = PortfolioPopulationService()
        results = population_service.populate_from_excel(
            securities_file=str(securities_file),
            transactions_file=str(transactions_file),
            snapshot_date=self._convert_date_format(date_str)
        )
        
        # Verify our target client was processed
        if self.client_code not in results['client_details']:
            raise ValueError(f"Expected {self.client_code} but processed: {list(results['client_details'].keys())}")
        
        client_details = results['client_details'][self.client_code]
        
        self.logger.info(f"   ✅ REPLACEMENT: Created {client_details['positions_created']} positions")
        
        return {
            'mode': 'replacement',
            'client': self.client_code,
            'bank': self.bank_code,
            'date': date_str,
            'positions_created': client_details['positions_created'],
            'securities_processed': client_details['securities_processed'],
            'transactions_processed': client_details['transactions_processed']
        }
    
    def _additive_integration(self, date_str: str, securities_file: Path, transactions_file: Path) -> dict:
        """Additive integration (preserves existing bank data, adds/updates target bank)"""
        
        snapshot_date = self._convert_date_format(date_str)
        
        # Step 1: Get existing client and snapshot (create if new date)
        client = Client.objects.get(code=self.client_code)
        snapshot, snapshot_created = PortfolioSnapshot.objects.get_or_create(
            client=client,
            snapshot_date=snapshot_date,
            defaults={'portfolio_metrics': {}}
        )
        
        if snapshot_created:
            self.logger.info(f"   📅 Created new snapshot for {self.client_code} on {snapshot_date}")
        else:
            self.logger.info(f"   📅 Using existing snapshot for {self.client_code} on {snapshot_date}")
        
        # Step 2: Selective cleanup - remove only target bank positions from this snapshot
        existing_positions = Position.objects.filter(
            snapshot=snapshot,
            asset__bank=self.bank_code
        )
        removed_positions = existing_positions.count()
        existing_positions.delete()
        
        self.logger.info(f"   🗑️ Removed {removed_positions} existing {self.bank_code} positions (transactions handled by natural deduplication)")
        
        # Step 4: Use PortfolioPopulationService to add new data
        # This reuses all the proven parsing, validation, and creation logic
        from portfolio.services.portfolio_population_service import PortfolioPopulationService
        
        # Temporarily clear the snapshot to let population service recreate it
        # But first save other bank positions that should be preserved
        other_bank_positions = Position.objects.filter(snapshot=snapshot).exclude(asset__bank=self.bank_code)
        preserved_positions = list(other_bank_positions.values())
        
        # Let population service handle the file processing
        population_service = PortfolioPopulationService()
        results = population_service.populate_from_excel(
            securities_file=str(securities_file),
            transactions_file=str(transactions_file),
            snapshot_date=snapshot_date
        )
        
        # CRITICAL: Remove duplicate transactions that may have been created
        # This happens when the same transaction appears in multiple files at different row positions
        self._remove_duplicate_transactions(self.client_code)
        
        # Restore other bank positions if this was an existing snapshot
        if not snapshot_created and preserved_positions:
            for pos_data in preserved_positions:
                # Remove the id and snapshot reference for recreation
                pos_data.pop('id', None)
                pos_data['snapshot'] = snapshot
                pos_data['asset_id'] = pos_data.pop('asset')  # Fix foreign key reference
                Position.objects.create(**pos_data)
            
            self.logger.info(f"   🔄 Restored {len(preserved_positions)} positions from other banks")
        
        # Extract results for our specific client
        client_details = results['client_details'].get(self.client_code, {})
        
        self.logger.info(f"   ✅ ADDITIVE: Added {client_details.get('positions_created', 0)} {self.bank_code} positions, {client_details.get('transactions_created', 0)} transactions")
        
        return {
            'mode': 'additive',
            'client': self.client_code,
            'bank': self.bank_code,
            'date': date_str,
            'snapshot_created': snapshot_created,
            'positions_removed': removed_positions,
            'positions_added': client_details.get('positions_created', 0),
            'transactions_added': client_details.get('transactions_created', 0),
            'securities_processed': client_details.get('securities_processed', 0)
        }
    
    def _create_or_get_asset(self, security: dict) -> Asset:
        """Create or get asset with proper uniqueness"""
        ticker = security.get('ticker', '')
        bank = security.get('bank', '')
        account = security.get('account', '')
        cusip = security.get('cusip', '')
        name = security.get('name', '')
        
        # Use the same uniqueness constraints as the model
        try:
            asset, created = Asset.objects.get_or_create(
                ticker=ticker,
                cusip=cusip,
                name=name,
                bank=bank,
                account=account,
                client=self.client_code,
                defaults={
                    'asset_type': security.get('asset_type', 'Unknown'),
                    'currency': security.get('currency', 'USD'),
                    'isin': security.get('isin', ''),
                    'coupon_rate': None,
                    'maturity_date': None
                }
            )
            return asset
            
        except Asset.MultipleObjectsReturned:
            # If multiple assets exist, get the first one and log the issue
            self.logger.warning(f"Multiple assets found for {ticker}|{cusip}|{name}|{bank}|{account}|{self.client_code}, using first one")
            asset = Asset.objects.filter(
                ticker=ticker,
                cusip=cusip,
                name=name,
                bank=bank,
                account=account,
                client=self.client_code
            ).first()
            return asset
    
    def _convert_date_format(self, date_str: str) -> str:
        """Convert DD_MM_YYYY to YYYY-MM-DD"""
        day, month, year = date_str.split('_')
        return f"{year}-{month}-{day}"
    
    def _safe_date(self, value):
        """Safely convert value to date object"""
        from datetime import datetime, date
        
        if value is None or value == '':
            return None
        
        if isinstance(value, date):
            return value
            
        if isinstance(value, datetime):
            return value.date()
        
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value == '-' or value.lower() == 'none':
                return None
            
            # Try common date formats
            date_formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']
            for fmt in date_formats:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
        
        return None
    
    def _generate_date_range(self, start_date: str, end_date: str) -> list:
        """Generate list of dates between start and end (you'd implement based on business logic)"""
        # For now, return a simple list - in real implementation, this would generate
        # all weekly dates between start and end based on existing snapshots
        return [
            "29_05_2025", "05_06_2025", "12_06_2025", "19_06_2025", "26_06_2025",
            "03_07_2025", "10_07_2025", "17_07_2025", "24_07_2025", "31_07_2025", 
            "07_08_2025", "14_08_2025", "21_08_2025", "28_08_2025", "04_09_2025", "11_09_2025"
        ]
    
    def _remove_duplicate_transactions(self, client_code: str):
        """
        Remove duplicate transactions based on business characteristics.
        
        Two transactions are considered duplicates if they have the same:
        - Client, Bank, Account, Date, Transaction Type, Amount, CUSIP
        
        Keep the first one found and delete others.
        """
        from portfolio.models import Transaction
        from collections import defaultdict
        
        # Get all transactions for this client
        transactions = Transaction.objects.filter(client__code=client_code).order_by('id')
        
        # Group by business characteristics (excluding row-dependent transaction_id)
        groups = defaultdict(list)
        
        for tx in transactions:
            # Create business key without row index influence
            key = (
                tx.client.code,
                tx.bank,
                tx.account, 
                tx.date,
                tx.transaction_type,
                str(tx.amount),
                tx.asset.cusip if tx.asset else ''
            )
            groups[key].append(tx)
        
        # Remove duplicates (keep first, delete rest)
        duplicates_removed = 0
        for key, tx_list in groups.items():
            if len(tx_list) > 1:
                # Keep the first transaction, delete the rest
                for tx in tx_list[1:]:
                    tx.delete()
                    duplicates_removed += 1
                
                self.logger.info(f"   🗑️ Removed {len(tx_list)-1} duplicate transactions for {key[3]} {key[4]} ${key[5]}")
        
        if duplicates_removed > 0:
            self.logger.info(f"   ✅ Total duplicates removed: {duplicates_removed}")
        else:
            self.logger.info(f"   ✅ No duplicate transactions found")
    
    def integrate_date_range(self, start_date: str, end_date: str, interactive: bool = True):
        """Integrate multiple dates with monitoring"""
        
        # Generate date list
        target_dates = self._generate_date_range(start_date, end_date)
        
        results = []
        
        print(f"🚀 Starting {self.client_code} + {self.bank_code} integration")
        print(f"📅 Processing {len(target_dates)} dates: {start_date} → {end_date}")
        print(f"🔧 Mode: {self.mode.upper()}")
        
        for i, date_str in enumerate(target_dates, 1):
            try:
                print(f"\n📊 [{i}/{len(target_dates)}] Processing {date_str}")
                
                result = self.integrate_single_date(date_str)
                results.append(result)
                
                print(f"✅ {date_str} completed successfully")
                
                if interactive and i < len(target_dates):
                    choice = input(f"\nContinue to {target_dates[i] if i < len(target_dates) else 'end'}? (y/n/auto): ").lower()
                    if choice == 'n':
                        break
                    elif choice == 'auto':
                        interactive = False
                
            except Exception as e:
                print(f"❌ {date_str} failed: {e}")
                
                if interactive:
                    choice = input("Continue to next date? (y/n): ").lower()
                    if choice != 'y':
                        break
                else:
                    break
        
        # Summary report
        self._print_integration_summary(results)
        
        return results
    
    def _print_integration_summary(self, results: list):
        """Print comprehensive integration summary"""
        print(f"\n📈 INTEGRATION SUMMARY")
        print(f"Client: {self.client_code}")
        print(f"Bank: {self.bank_code}")
        print(f"Mode: {self.mode.upper()}")
        print(f"Dates processed: {len(results)}")
        
        if results:
            if self.mode == "replacement":
                total_positions = sum(r['positions_created'] for r in results)
                total_securities = sum(r['securities_processed'] for r in results)
                print(f"Total positions created: {total_positions}")
                print(f"Total securities processed: {total_securities}")
                
            else:  # additive
                total_removed = sum(r['positions_removed'] for r in results)
                total_added = sum(r['positions_added'] for r in results)
                print(f"Total positions removed: {total_removed}")
                print(f"Total positions added: {total_added}")
            
            # Portfolio values
            portfolio_values = [r['portfolio_metrics']['total_value'] for r in results if 'portfolio_metrics' in r]
            if portfolio_values:
                print(f"Portfolio value range: ${min(portfolio_values):,.2f} - ${max(portfolio_values):,.2f}")
                print(f"Latest portfolio value: ${portfolio_values[-1]:,.2f}")

def main():
    parser = argparse.ArgumentParser(description='Universal Client-Bank Integration System')
    parser.add_argument('--client', required=True, help='Client code (e.g., EI, ELP)')
    parser.add_argument('--bank', required=True, help='Bank code (e.g., STDSZ, PICTET)')
    parser.add_argument('--date', help='Single date in DD_MM_YYYY format')
    parser.add_argument('--date-range', help='Date range in format DD_MM_YYYY:DD_MM_YYYY')
    parser.add_argument('--mode', choices=['auto', 'replacement', 'additive'], default='auto',
                       help='Integration mode (auto-detects by default)')
    parser.add_argument('--interactive', action='store_true', default=True,
                       help='Pause between dates for verification')
    
    args = parser.parse_args()
    
    # Initialize integrator
    integrator = UniversalClientIntegrator(args.client, args.bank)
    
    if args.date:
        # Single date integration
        result = integrator.integrate_single_date(args.date)
        print("Integration completed successfully!")
        
    elif args.date_range:
        # Date range integration
        start_date, end_date = args.date_range.split(':')
        results = integrator.integrate_date_range(start_date, end_date, args.interactive)
        print(f"Batch integration completed! Processed {len(results)} dates.")
        
    else:
        print("Error: Must specify either --date or --date-range")
        return 1
    

if __name__ == "__main__":
    sys.exit(main())