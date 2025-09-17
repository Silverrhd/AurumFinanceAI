#!/usr/bin/env python3
"""
Integration Verification Tool
Validates client-bank integration results
"""

import os
import sys
import django
import argparse
from datetime import datetime

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')
django.setup()

from portfolio.models import Client, PortfolioSnapshot, Position, Asset
from portfolio.services.correct_dashboard_cache_service import CorrectDashboardCacheService

def verify_client_integration(client_code: str, bank_code: str = None):
    """Verify client integration completeness and data integrity"""
    
    try:
        client = Client.objects.get(code=client_code)
    except Client.DoesNotExist:
        print(f"❌ Client {client_code} does not exist")
        return False
    
    snapshots = PortfolioSnapshot.objects.filter(client=client).order_by('snapshot_date')
    
    print(f"📊 VERIFICATION REPORT: {client_code}")
    print(f"Total snapshots: {snapshots.count()}")
    
    if not snapshots.exists():
        print("❌ No snapshots found for client")
        return False
    
    for snapshot in snapshots:
        positions = Position.objects.filter(snapshot=snapshot).select_related('asset')
        
        if bank_code:
            bank_positions = positions.filter(asset__bank=bank_code)
            other_positions = positions.exclude(asset__bank=bank_code)
            
            bank_value = sum(float(pos.market_value) for pos in bank_positions)
            total_value = sum(float(pos.market_value) for pos in positions)
            
            print(f"  {snapshot.snapshot_date}: {bank_code}={bank_positions.count()} pos (${bank_value:,.2f}), Others={other_positions.count()}, Total=${total_value:,.2f}")
        else:
            banks = set(pos.asset.bank for pos in positions)
            total_value = sum(float(pos.market_value) for pos in positions)
            print(f"  {snapshot.snapshot_date}: {positions.count()} positions across {len(banks)} banks: {sorted(banks)}, Total=${total_value:,.2f}")
    
    return True

def verify_dashboard_cache(client_code: str = None):
    """Verify dashboard cache includes the client properly"""
    
    cache_service = CorrectDashboardCacheService()
    
    # Get latest cache
    from portfolio.models import DateAggregatedMetrics
    latest_cache = DateAggregatedMetrics.objects.order_by('-snapshot_date').first()
    
    if latest_cache:
        print(f"\n📈 DASHBOARD CACHE STATUS")
        print(f"Latest cache date: {latest_cache.snapshot_date}")
        print(f"Total clients in cache: {latest_cache.client_count}")
        print(f"Total AUM: ${latest_cache.total_aum:,.2f}")
        
        if client_code:
            # Check if specific client is included in calculations
            client_snapshots = PortfolioSnapshot.objects.filter(
                client__code=client_code,
                snapshot_date=latest_cache.snapshot_date
            )
            
            if client_snapshots.exists():
                print(f"✅ {client_code} has snapshot for latest cache date")
            else:
                print(f"⚠️ {client_code} missing snapshot for latest cache date")
    else:
        print("❌ No dashboard cache found")

def check_data_integrity(client_code: str, bank_code: str):
    """Check for common data integrity issues"""
    
    print(f"\n🔍 DATA INTEGRITY CHECK: {client_code} + {bank_code}")
    
    client = Client.objects.get(code=client_code)
    
    # Check for positions without assets
    positions_without_assets = Position.objects.filter(
        snapshot__client=client,
        asset__isnull=True
    ).count()
    
    if positions_without_assets > 0:
        print(f"❌ Found {positions_without_assets} positions without assets")
    else:
        print("✅ All positions have valid assets")
    
    # Check for assets from the target bank
    bank_assets = Asset.objects.filter(
        client_code=client_code,
        bank=bank_code
    ).count()
    
    if bank_assets > 0:
        print(f"✅ Found {bank_assets} assets from {bank_code}")
    else:
        print(f"⚠️ No assets found from {bank_code}")
    
    # Check for duplicate assets
    from django.db.models import Count
    duplicate_assets = Asset.objects.filter(
        client_code=client_code,
        bank=bank_code
    ).values('ticker', 'bank', 'account').annotate(
        count=Count('id')
    ).filter(count__gt=1)
    
    if duplicate_assets.exists():
        print(f"❌ Found {duplicate_assets.count()} potential duplicate assets")
        for dup in duplicate_assets:
            print(f"   Duplicate: {dup}")
    else:
        print("✅ No duplicate assets found")

def compare_before_after(client_code: str, date_str: str):
    """Compare client state before and after integration"""
    
    print(f"\n📊 BEFORE/AFTER COMPARISON: {client_code} on {date_str}")
    
    client = Client.objects.get(code=client_code)
    date_obj = datetime.strptime(date_str, '%d_%m_%Y').date()
    
    try:
        snapshot = PortfolioSnapshot.objects.get(client=client, snapshot_date=date_obj)
        positions = Position.objects.filter(snapshot=snapshot)
        
        banks = set(pos.asset.bank for pos in positions)
        total_positions = positions.count()
        total_value = sum(float(pos.market_value) for pos in positions)
        
        print(f"Current state:")
        print(f"  Banks: {sorted(banks)}")
        print(f"  Positions: {total_positions}")
        print(f"  Total Value: ${total_value:,.2f}")
        
        for bank in sorted(banks):
            bank_positions = positions.filter(asset__bank=bank)
            bank_value = sum(float(pos.market_value) for pos in bank_positions)
            print(f"  {bank}: {bank_positions.count()} positions, ${bank_value:,.2f}")
        
    except PortfolioSnapshot.DoesNotExist:
        print(f"❌ No snapshot found for {client_code} on {date_str}")

def main():
    parser = argparse.ArgumentParser(description='Integration Verification Tool')
    parser.add_argument('--client', required=True, help='Client code to verify')
    parser.add_argument('--bank', help='Specific bank code to check')
    parser.add_argument('--date', help='Specific date to analyze (DD_MM_YYYY)')
    parser.add_argument('--check-cache', action='store_true', help='Verify dashboard cache')
    parser.add_argument('--check-integrity', action='store_true', help='Run data integrity checks')
    parser.add_argument('--compare', help='Compare state for specific date (DD_MM_YYYY)')
    
    args = parser.parse_args()
    
    # Main verification
    success = verify_client_integration(args.client, args.bank)
    
    if not success:
        print("❌ Basic verification failed")
        return 1
    
    # Optional checks
    if args.check_cache:
        verify_dashboard_cache(args.client)
    
    if args.check_integrity and args.bank:
        check_data_integrity(args.client, args.bank)
    
    if args.compare:
        compare_before_after(args.client, args.compare)
    
    print("\n✅ Verification completed")
    return 0

if __name__ == "__main__":
    sys.exit(main())