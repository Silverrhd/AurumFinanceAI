#!/usr/bin/env python3

import os
import sys
import django
import shutil

# Setup Django
sys.path.append('/Users/thomaskemeny/AurumFinance/aurum_backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')
django.setup()

from portfolio.models import Transaction, Position, PortfolioSnapshot, Asset, Report
from django.db import transaction

print('=== SAFE DATABASE AND REPORTS CLEARING ===')
print()

def clear_portfolio_data():
    """Clear portfolio data while preserving clients and user data"""
    print('STEP 1: CLEARING PORTFOLIO DATA')
    print('='*50)
    
    with transaction.atomic():
        # Get counts
        txn_count = Transaction.objects.count()
        pos_count = Position.objects.count()
        snap_count = PortfolioSnapshot.objects.count()
        asset_count = Asset.objects.count()
        report_count = Report.objects.count()
        
        print(f'Current data:')
        print(f'  Transactions: {txn_count}')
        print(f'  Positions: {pos_count}')
        print(f'  Snapshots: {snap_count}')
        print(f'  Assets: {asset_count}')
        print(f'  Reports: {report_count}')
        
        if txn_count == 0 and pos_count == 0:
            print('✅ Portfolio data already empty')
            return
            
        print(f'\\nClearing portfolio data...')
        
        # Delete in correct order
        Position.objects.all().delete()
        print('  ✅ Positions cleared')
        
        PortfolioSnapshot.objects.all().delete()
        print('  ✅ Snapshots cleared')
        
        Transaction.objects.all().delete()
        print('  ✅ Transactions cleared')
        
        Asset.objects.all().delete()
        print('  ✅ Assets cleared')
        
        Report.objects.all().delete()
        print('  ✅ Report records cleared')
        
        print('✅ Portfolio data cleared successfully')

def clear_report_files():
    """Clear generated report HTML files"""
    print('\\nSTEP 2: CLEARING REPORT FILES')
    print('='*50)
    
    reports_dir = '/Users/thomaskemeny/AurumFinance/aurum_backend/reports'
    
    if os.path.exists(reports_dir):
        # Count files before deletion
        file_count = 0
        for root, dirs, files in os.walk(reports_dir):
            file_count += len(files)
        
        print(f'Found {file_count} report files to clear')
        
        if file_count > 0:
            # Remove all contents but keep the directory structure
            for item in os.listdir(reports_dir):
                item_path = os.path.join(reports_dir, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            
            print('✅ Report files cleared successfully')
        else:
            print('✅ No report files to clear')
    else:
        print('⚠️  Reports directory not found')

def verify_clearing():
    """Verify that clearing was successful"""
    print('\\nSTEP 3: VERIFICATION')
    print('='*50)
    
    # Check database
    txn_count = Transaction.objects.count()
    pos_count = Position.objects.count()
    snap_count = PortfolioSnapshot.objects.count()
    asset_count = Asset.objects.count()
    report_count = Report.objects.count()
    
    print(f'Database after clearing:')
    print(f'  Transactions: {txn_count}')
    print(f'  Positions: {pos_count}')
    print(f'  Snapshots: {snap_count}')
    print(f'  Assets: {asset_count}')
    print(f'  Reports: {report_count}')
    
    # Check reports directory
    reports_dir = '/Users/thomaskemeny/AurumFinance/aurum_backend/reports'
    if os.path.exists(reports_dir):
        file_count = sum(len(files) for _, _, files in os.walk(reports_dir))
        print(f'  Report files: {file_count}')
    
    # Check that clients are preserved
    from portfolio.models import Client
    client_count = Client.objects.count()
    print(f'  Clients (preserved): {client_count}')
    
    if txn_count == 0 and pos_count == 0 and snap_count == 0 and asset_count == 0:
        print('\\n✅ CLEARING SUCCESSFUL - Ready for repopulation')
    else:
        print('\\n❌ CLEARING INCOMPLETE - Some data remains')

def main():
    print('This will clear:')
    print('  ✅ All portfolio data (Transactions, Positions, Snapshots, Assets)')
    print('  ✅ All report records and files')
    print('  ✅ Preserve Clients and User accounts')
    print()
    
    response = input('Proceed with clearing? (type YES to confirm): ')
    
    if response == 'YES':
        clear_portfolio_data()
        clear_report_files()
        verify_clearing()
        
        print('\\n=== NEXT STEPS ===')
        print('1. Run your pipeline to populate database')
        print('2. Generate reports to verify fixes')
        print('3. Check that GZ $700K withdrawal is now included')
        print('4. Verify GW MISCELLANEOUS FEES is external')
        
    else:
        print('❌ Clearing cancelled')

if __name__ == '__main__':
    main()