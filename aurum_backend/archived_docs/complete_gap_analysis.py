#!/usr/bin/env python
"""
Complete Gap Analysis - Definitive list of what needs to be added.
"""

import os
import sys
import django
from collections import Counter, defaultdict

# Setup Django
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')
django.setup()

from portfolio.models import Transaction, Client
from portfolio.services.investment_cash_flow_service import InvestmentCashFlowService
from portfolio.services.cash_flow_service import CashFlowService

def complete_gap_analysis():
    print("=" * 80)
    print("COMPLETE GAP ANALYSIS - DEFINITIVE ACTION LIST")
    print("=" * 80)
    
    # Initialize services
    investment_service = InvestmentCashFlowService()
    cash_flow_service = CashFlowService()
    
    # Get all transactions grouped by bank
    all_transactions = Transaction.objects.all()
    bank_transactions = defaultdict(list)
    
    for tx in all_transactions.iterator():
        bank = getattr(tx, 'bank', 'UNKNOWN')
        bank_transactions[bank].append(tx)
    
    print(f"Total transactions: {all_transactions.count():,}")
    print(f"Banks found: {list(bank_transactions.keys())}")
    
    # Analyze each bank comprehensively
    action_items = {
        'add_mappings': {},
        'add_extraction': {},
        'missing_banks': [],
        'coverage_summary': {}
    }
    
    for bank, transactions in bank_transactions.items():
        print(f"\n" + "=" * 60)
        print(f"ANALYZING {bank} BANK ({len(transactions):,} transactions)")
        print("=" * 60)
        
        # Get all unique transaction types for this bank
        tx_type_counts = Counter(tx.transaction_type for tx in transactions)
        
        print(f"Unique transaction types: {len(tx_type_counts)}")
        
        # Check if bank has mappings
        has_mappings = bank in investment_service.TRANSACTION_MAPPINGS
        has_extraction = bank in ['CS', 'IDB']
        
        print(f"Has transaction mappings: {has_mappings}")
        print(f"Has extraction logic: {has_extraction}")
        
        if not has_mappings:
            action_items['missing_banks'].append({
                'bank': bank,
                'transaction_count': len(transactions),
                'unique_types': len(tx_type_counts),
                'types': list(tx_type_counts.keys())
            })
            print(f"❌ BANK NOT IN MAPPINGS - needs complete mapping")
            continue
        
        # Analyze coverage for banks with mappings
        bank_mappings = investment_service.TRANSACTION_MAPPINGS[bank]
        all_mapped_types = set()
        for category, types in bank_mappings.items():
            all_mapped_types.update(types)
        
        covered_transactions = 0
        uncovered_transactions = 0
        uncovered_types = []
        
        for tx_type, count in tx_type_counts.items():
            # Apply extraction if available
            if has_extraction:
                if bank == 'CS':
                    extracted_type = investment_service._extract_cs_transaction_type(tx_type)
                elif bank == 'IDB':
                    extracted_type = investment_service._extract_idb_transaction_type(tx_type)
                else:
                    extracted_type = tx_type
            else:
                extracted_type = tx_type
            
            # Check if extracted type is mapped
            if extracted_type in all_mapped_types:
                covered_transactions += count
            else:
                uncovered_transactions += count
                uncovered_types.append({
                    'original': tx_type,
                    'extracted': extracted_type,
                    'count': count
                })
        
        coverage_pct = (covered_transactions / len(transactions)) * 100
        action_items['coverage_summary'][bank] = {
            'total_transactions': len(transactions),
            'covered_transactions': covered_transactions,
            'uncovered_transactions': uncovered_transactions,
            'coverage_percentage': coverage_pct,
            'uncovered_types_count': len(uncovered_types)
        }
        
        print(f"Coverage: {coverage_pct:.1f}% ({covered_transactions:,}/{len(transactions):,} transactions)")
        
        if uncovered_types:
            print(f"\n❌ MISSING MAPPINGS ({len(uncovered_types)} types, {uncovered_transactions:,} transactions):")
            
            # Group uncovered types by suggested category
            categorized_missing = {
                'TRADING_BUY': [],
                'TRADING_SELL': [],
                'DIVIDEND_INCOME': [],
                'INTEREST_INCOME': [],
                'SERVICE_FEES': [],
                'TAX_FEES': [],
                'EXTERNAL_FLOWS': [],
                'OTHER_EXCLUDED': [],
                'UNKNOWN': []
            }
            
            for item in uncovered_types:
                extracted = item['extracted'].lower()
                category = 'UNKNOWN'
                
                if any(word in extracted for word in ['purchase', 'buy']):
                    category = 'TRADING_BUY'
                elif any(word in extracted for word in ['sale', 'sell', 'redemption']):
                    category = 'TRADING_SELL'
                elif any(word in extracted for word in ['dividend', 'div']):
                    category = 'DIVIDEND_INCOME'
                elif any(word in extracted for word in ['interest', 'coupon']):
                    category = 'INTEREST_INCOME'
                elif any(word in extracted for word in ['fee', 'commission', 'charge']):
                    category = 'SERVICE_FEES'
                elif any(word in extracted for word in ['tax', 'withhold']):
                    category = 'TAX_FEES'
                elif any(word in extracted for word in ['deposit', 'withdrawal', 'wire', 'transfer']):
                    category = 'EXTERNAL_FLOWS'
                elif any(word in extracted for word in ['redemption', 'maturity', 'exchange']):
                    category = 'OTHER_EXCLUDED'
                
                categorized_missing[category].append(item)
            
            # Store for action items
            if bank not in action_items['add_mappings']:
                action_items['add_mappings'][bank] = {}
            
            for category, items in categorized_missing.items():
                if items:
                    print(f"\n  {category}:")
                    action_items['add_mappings'][bank][category] = []
                    for item in sorted(items, key=lambda x: x['count'], reverse=True):
                        print(f"    '{item['extracted']}',  # {item['count']:,} transactions")
                        action_items['add_mappings'][bank][category].append({
                            'type': item['extracted'],
                            'count': item['count'],
                            'original_examples': [item['original']]
                        })
        else:
            print(f"✅ COMPLETE COVERAGE - no missing mappings")
    
    # Check for banks that need extraction logic
    print(f"\n" + "=" * 80)
    print("EXTRACTION LOGIC ANALYSIS")
    print("=" * 80)
    
    banks_needing_extraction = []
    
    for bank, transactions in bank_transactions.items():
        if bank in ['CS', 'IDB']:  # Already have extraction
            continue
        if bank not in investment_service.TRANSACTION_MAPPINGS:  # No mappings yet
            continue
        
        # Check if this bank has complex transaction types that need extraction
        tx_types = [tx.transaction_type for tx in transactions[:10]]  # Sample
        needs_extraction = False
        complex_examples = []
        
        for tx_type in tx_types:
            # Check for patterns that suggest need for extraction
            if (len(tx_type) > 50 or  # Very long descriptions
                any(pattern in tx_type.lower() for pattern in [
                    'shares of', 'parvalue of', '@', 'usd', 'eur', 'gbp',
                    '\n', 'at ', ' of ', ' @ '
                ])):
                needs_extraction = True
                complex_examples.append(tx_type)
        
        if needs_extraction:
            banks_needing_extraction.append({
                'bank': bank,
                'transaction_count': len(transactions),
                'examples': complex_examples[:3]
            })
    
    if banks_needing_extraction:
        print(f"Banks that may need extraction logic:")
        for item in banks_needing_extraction:
            print(f"\n{item['bank']} ({item['transaction_count']:,} transactions):")
            for example in item['examples']:
                print(f"  Example: '{example[:100]}...'")
            action_items['add_extraction'][item['bank']] = {
                'transaction_count': item['transaction_count'],
                'examples': item['examples']
            }
    else:
        print("✅ No additional extraction logic needed")
    
    # Generate final action summary
    print(f"\n" + "=" * 80)
    print("FINAL ACTION SUMMARY")
    print("=" * 80)
    
    total_affected_transactions = 0
    
    print(f"\n1. ADD TRANSACTION MAPPINGS:")
    for bank, categories in action_items['add_mappings'].items():
        bank_total = sum(
            sum(item['count'] for item in items) 
            for items in categories.values()
        )
        total_affected_transactions += bank_total
        print(f"   {bank}: {bank_total:,} transactions need mapping")
    
    print(f"\n2. ADD EXTRACTION LOGIC:")
    for bank, info in action_items['add_extraction'].items():
        print(f"   {bank}: {info['transaction_count']:,} transactions may need extraction")
    
    print(f"\n3. ADD COMPLETE BANK MAPPINGS:")
    for bank_info in action_items['missing_banks']:
        bank_total = bank_info['transaction_count']
        total_affected_transactions += bank_total
        print(f"   {bank_info['bank']}: {bank_total:,} transactions (complete mapping needed)")
    
    print(f"\nTOTAL TRANSACTIONS AFFECTED: {total_affected_transactions:,}")
    print(f"PERCENTAGE OF ALL DATA: {(total_affected_transactions / all_transactions.count()) * 100:.1f}%")
    
    # Generate implementation code
    print(f"\n" + "=" * 80)
    print("IMPLEMENTATION CODE TEMPLATES")
    print("=" * 80)
    
    for bank, categories in action_items['add_mappings'].items():
        print(f"\n# Add to {bank} in TRANSACTION_MAPPINGS:")
        for category, items in categories.items():
            if items:
                print(f"'{category}': [")
                for item in items:
                    print(f"    '{item['type']}',  # {item['count']:,} transactions")
                print(f"],")
    
    return action_items

if __name__ == "__main__":
    try:
        results = complete_gap_analysis()
        print(f"\n" + "=" * 80)
        print("COMPLETE GAP ANALYSIS FINISHED")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()