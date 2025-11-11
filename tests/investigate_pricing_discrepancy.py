#!/usr/bin/env python3
"""
Investigate pricing discrepancy in fact_orders.csv
Check if paid_price + vouchers = original_unit_price and identify missing components
"""

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def investigate_pricing_discrepancy():
    """Investigate why pricing components don't add up correctly"""
    print("🔍 INVESTIGATING PRICING DISCREPANCY")
    print("=" * 50)
    
    try:
        # Load fact orders
        df = pd.read_csv('../app/Transformed/fact_orders.csv')
        print(f"✓ Loaded {len(df):,} fact order records")
        
        # Calculate the expected relationship:
        # paid_price + voucher_platform_amount + voucher_seller_amount = original_unit_price
        print(f"\n📊 PRICING RELATIONSHIP ANALYSIS")
        print("-" * 40)
        
        df['total_vouchers'] = df['voucher_platform_amount'] + df['voucher_seller_amount']
        df['calculated_original'] = df['paid_price'] + df['total_vouchers']
        df['price_difference'] = df['original_unit_price'] - df['calculated_original']
        
        # Overall statistics
        print(f"Original unit price range: ₱{df['original_unit_price'].min():.2f} - ₱{df['original_unit_price'].max():.2f}")
        print(f"Paid price range: ₱{df['paid_price'].min():.2f} - ₱{df['paid_price'].max():.2f}")
        print(f"Total vouchers range: ₱{df['total_vouchers'].min():.2f} - ₱{df['total_vouchers'].max():.2f}")
        
        # Check if formula holds: original = paid + vouchers
        print(f"\n🧮 FORMULA CHECK: original_unit_price = paid_price + vouchers")
        print("-" * 50)
        
        exact_matches = (abs(df['price_difference']) < 0.01).sum()
        discrepancies = (abs(df['price_difference']) >= 0.01).sum()
        
        print(f"Records with exact match (±₱0.01): {exact_matches:,} ({exact_matches/len(df)*100:.1f}%)")
        print(f"Records with discrepancies (>₱0.01): {discrepancies:,} ({discrepancies/len(df)*100:.1f}%)")
        
        if discrepancies > 0:
            print(f"Average discrepancy: ₱{abs(df['price_difference']).mean():.2f}")
            print(f"Max discrepancy: ₱{abs(df['price_difference']).max():.2f}")
            print(f"Total discrepancy: ₱{df['price_difference'].sum():.2f}")
        
        # Analyze discrepancy patterns
        if discrepancies > 0:
            print(f"\n🔍 DISCREPANCY PATTERN ANALYSIS")
            print("-" * 40)
            
            # Look at records with significant discrepancies
            significant_disc = df[abs(df['price_difference']) >= 1.0].copy()
            print(f"Records with discrepancy ≥₱1.00: {len(significant_disc):,}")
            
            if len(significant_disc) > 0:
                print(f"\nSample records with significant discrepancies:")
                sample_cols = ['order_item_key', 'platform_key', 'paid_price', 'original_unit_price', 
                             'voucher_platform_amount', 'voucher_seller_amount', 'price_difference']
                print(significant_disc[sample_cols].head(10).to_string())
                
                # Analyze by platform
                print(f"\nDiscrepancy by platform:")
                for platform in [1, 2]:
                    platform_name = "Lazada" if platform == 1 else "Shopee"
                    platform_disc = significant_disc[significant_disc['platform_key'] == platform]
                    if len(platform_disc) > 0:
                        print(f"{platform_name}: {len(platform_disc):,} records, avg discrepancy ₱{abs(platform_disc['price_difference']).mean():.2f}")
        
        # Check for missing pricing components
        print(f"\n🔍 MISSING PRICING COMPONENTS ANALYSIS")
        print("-" * 40)
        
        # Look for potential missing fields by checking patterns
        # 1. Check if there are cases where vouchers > discount
        df['apparent_discount'] = df['original_unit_price'] - df['paid_price']
        df['voucher_vs_discount'] = df['total_vouchers'] - df['apparent_discount']
        
        voucher_excess = (df['voucher_vs_discount'] > 0.01).sum()
        if voucher_excess > 0:
            print(f"⚠️  Records where vouchers > apparent discount: {voucher_excess:,}")
            print("   This suggests additional pricing components may exist")
        
        # 2. Look for zero voucher records with discounts
        zero_voucher_discounts = df[(df['total_vouchers'] == 0) & (df['apparent_discount'] > 0)]
        print(f"Records with discounts but zero vouchers: {len(zero_voucher_discounts):,}")
        
        if len(zero_voucher_discounts) > 0:
            print(f"   Total discount without voucher explanation: ₱{zero_voucher_discounts['apparent_discount'].sum():,.2f}")
        
        # 3. Check for potential additional discount types
        print(f"\n💡 POTENTIAL MISSING COMPONENTS:")
        print("-" * 40)
        
        missing_explanations = []
        
        # Cases where paid < original but no vouchers recorded
        unexplained_discounts = df[(df['paid_price'] < df['original_unit_price']) & 
                                 (df['total_vouchers'] == 0)]
        if len(unexplained_discounts) > 0:
            total_unexplained = unexplained_discounts['apparent_discount'].sum()
            missing_explanations.append(f"Unexplained discounts (no vouchers): {len(unexplained_discounts):,} records, ₱{total_unexplained:,.2f}")
        
        # Cases where formula doesn't balance
        unbalanced = df[abs(df['price_difference']) >= 0.01]
        if len(unbalanced) > 0:
            total_unbalanced = abs(unbalanced['price_difference']).sum()
            missing_explanations.append(f"Formula imbalances: {len(unbalanced):,} records, ₱{total_unbalanced:,.2f} total variance")
        
        if missing_explanations:
            for explanation in missing_explanations:
                print(f"• {explanation}")
        else:
            print("✅ No obvious missing components identified")
        
        # Detailed breakdown by platform
        print(f"\n📊 PLATFORM-SPECIFIC ANALYSIS")
        print("-" * 40)
        
        for platform_key, platform_name in [(1, "Lazada"), (2, "Shopee")]:
            platform_df = df[df['platform_key'] == platform_key]
            if len(platform_df) > 0:
                print(f"\n{platform_name}:")
                print(f"  Records: {len(platform_df):,}")
                print(f"  Exact matches: {(abs(platform_df['price_difference']) < 0.01).sum():,}")
                print(f"  Discrepancies: {(abs(platform_df['price_difference']) >= 0.01).sum():,}")
                print(f"  Total original: ₱{platform_df['original_unit_price'].sum():,.2f}")
                print(f"  Total paid: ₱{platform_df['paid_price'].sum():,.2f}")
                print(f"  Total vouchers: ₱{platform_df['total_vouchers'].sum():,.2f}")
                print(f"  Calculated vs Original: ₱{platform_df['price_difference'].sum():,.2f} difference")
        
        # Summary and recommendations
        print(f"\n📋 SUMMARY & RECOMMENDATIONS")
        print("=" * 50)
        
        if discrepancies == 0:
            print("✅ Perfect pricing consistency - all formulas balance")
        else:
            discrepancy_pct = discrepancies / len(df) * 100
            if discrepancy_pct < 1:
                print(f"✅ Excellent pricing consistency - {discrepancy_pct:.2f}% minor discrepancies")
            elif discrepancy_pct < 5:
                print(f"⚠️  Good pricing consistency - {discrepancy_pct:.2f}% discrepancies to investigate")
            else:
                print(f"❌ Poor pricing consistency - {discrepancy_pct:.2f}% discrepancies require attention")
        
        return discrepancies == 0
        
    except Exception as e:
        print(f"❌ Error investigating pricing discrepancy: {e}")
        return False

if __name__ == "__main__":
    investigate_pricing_discrepancy()