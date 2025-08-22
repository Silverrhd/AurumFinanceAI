#!/usr/bin/env python3
"""
Populate missing fields with calculated values for report generation.
Run this script to calculate estimated_annual_income and face_value for existing positions.
"""

import os
import sys
import django

# Setup Django environment
sys.path.append('/Users/thomaskemeny/AurumFinance/aurum_backend')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aurum_backend.settings')
django.setup()

from portfolio.models import Position, Asset
from decimal import Decimal

def populate_position_fields():
    """Populate estimated_annual_income and face_value for positions."""
    
    positions_updated = 0
    
    for position in Position.objects.all():
        updated = False
        
        # Calculate estimated_annual_income if missing
        if not position.estimated_annual_income:
            market_value = position.market_value or Decimal('0')
            coupon_rate = Decimal(str(position.coupon_rate or 0)) if position.coupon_rate else (Decimal(str(position.asset.coupon_rate or 0)) if position.asset.coupon_rate else Decimal('0'))
            
            if coupon_rate > 0:
                position.estimated_annual_income = (market_value * coupon_rate / 100)
                updated = True
            else:
                position.estimated_annual_income = Decimal('0')
                updated = True
        
        # Set face_value (default to market_value for bonds)
        if not position.face_value:
            if position.asset.asset_type.upper() in ['BOND', 'FIXED_INCOME']:
                position.face_value = position.market_value
                updated = True
            else:
                position.face_value = position.market_value  # Default for all positions
                updated = True
        
        if updated:
            position.save()
            positions_updated += 1
            
            if positions_updated % 100 == 0:
                print(f"Updated {positions_updated} positions...")
    
    print(f"✅ Updated {positions_updated} positions with calculated fields")

def verify_data_population():
    """Verify that data was populated correctly."""
    
    total_positions = Position.objects.count()
    positions_with_income = Position.objects.filter(estimated_annual_income__isnull=False).count()
    positions_with_face_value = Position.objects.filter(face_value__isnull=False).count()
    
    print(f"\n📊 Data Population Summary:")
    print(f"Total positions: {total_positions}")
    print(f"Positions with estimated_annual_income: {positions_with_income}")
    print(f"Positions with face_value: {positions_with_face_value}")
    
    # Test JAV client data specifically
    from portfolio.models import Client, PortfolioSnapshot
    
    try:
        jav_client = Client.objects.get(code='JAV')
        jav_snapshot = PortfolioSnapshot.objects.get(client=jav_client, snapshot_date='2025-07-10')
        jav_positions = jav_snapshot.positions.all()
        
        total_annual_income = sum(pos.estimated_annual_income or Decimal('0') for pos in jav_positions)
        bond_positions = [pos for pos in jav_positions if pos.asset.asset_type.upper() in ['BOND', 'FIXED_INCOME']]
        
        print(f"\n🎯 JAV Client (2025-07-10) Verification:")
        print(f"Total positions: {jav_positions.count()}")
        print(f"Bond positions: {len(bond_positions)}")
        print(f"Total calculated annual income: ${total_annual_income:,.2f}")
        
        # Show sample positions with income
        positions_with_income = [pos for pos in jav_positions if pos.estimated_annual_income and pos.estimated_annual_income > 0]
        print(f"Positions with annual income > 0: {len(positions_with_income)}")
        
        if positions_with_income:
            print("\n📋 Sample positions with annual income:")
            for pos in positions_with_income[:5]:
                print(f"  {pos.asset.name}: ${pos.estimated_annual_income:,.2f}")
        
    except Exception as e:
        print(f"⚠️ Could not verify JAV client data: {e}")

if __name__ == "__main__":
    print("🚀 Starting field population...")
    populate_position_fields()
    verify_data_population()
    print("✅ Field population complete!")