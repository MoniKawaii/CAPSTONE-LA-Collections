#!/usr/bin/env python3
"""
Pricing Validation Analysis
Analyzes if price_paid = original_unit_price - total_vouchers holds true in the transformed data
"""

import pandas as pd
import numpy as np

def analyze_pricing_formula():
    """Analyze if paid_price = original_unit_price - (voucher_platform_amount + voucher_seller_amount)"""
    
    # Load the fact_orders data
    print("Loading fact_orders.csv...")
    df = pd.read_csv('app/Transformed/fact_orders.csv')
    
    print(f"Total records: {len(df):,}")
    print("\nDataset columns:")
    print(df.columns.tolist())
    
    # Calculate total vouchers per record
    df['total_vouchers'] = df['voucher_platform_amount'] + df['voucher_seller_amount']
    
    # Calculate expected paid price based on formula
    df['expected_paid_price'] = df['original_unit_price'] - df['total_vouchers']
    
    # Calculate the difference between actual and expected
    df['price_difference'] = df['paid_price'] - df['expected_paid_price']
    
    # Analyze the results
    print("\n" + "="*80)
    print("PRICING FORMULA VALIDATION ANALYSIS")
    print("="*80)
    print(f"Formula: paid_price = original_unit_price - total_vouchers")
    print(f"Where: total_vouchers = voucher_platform_amount + voucher_seller_amount")
    
    # Basic statistics
    exact_matches = len(df[df['price_difference'] == 0])
    near_matches = len(df[abs(df['price_difference']) < 0.01])  # Within 1 cent
    total_records = len(df)
    
    print(f"\n📊 SUMMARY STATISTICS:")
    print(f"• Exact matches (difference = 0): {exact_matches:,} ({exact_matches/total_records*100:.2f}%)")
    print(f"• Near matches (|difference| < 0.01): {near_matches:,} ({near_matches/total_records*100:.2f}%)")
    print(f"• Records with discrepancies: {total_records - near_matches:,} ({(total_records - near_matches)/total_records*100:.2f}%)")
    
    # Analyze price differences
    print(f"\n📈 PRICE DIFFERENCE STATISTICS:")
    print(f"• Mean difference: {df['price_difference'].mean():.6f}")
    print(f"• Median difference: {df['price_difference'].median():.6f}")
    print(f"• Std deviation: {df['price_difference'].std():.6f}")
    print(f"• Min difference: {df['price_difference'].min():.6f}")
    print(f"• Max difference: {df['price_difference'].max():.6f}")
    
    # Show distribution of differences
    print(f"\n📋 DIFFERENCE DISTRIBUTION:")
    diff_counts = df['price_difference'].value_counts().head(10)
    for diff, count in diff_counts.items():
        print(f"• Difference {diff:.2f}: {count:,} records ({count/total_records*100:.2f}%)")
    
    # Analyze by platform
    print(f"\n🏪 ANALYSIS BY PLATFORM:")
    platform_analysis = df.groupby('platform_key').agg({
        'price_difference': ['count', 'mean', 'std'],
        'paid_price': 'mean',
        'original_unit_price': 'mean',
        'total_vouchers': 'mean'
    }).round(4)
    
    platform_analysis.columns = ['Count', 'Avg_Diff', 'Std_Diff', 'Avg_Paid', 'Avg_Original', 'Avg_Vouchers']
    print(platform_analysis)
    
    # Show examples of discrepancies
    discrepancies = df[abs(df['price_difference']) > 0.01].copy()
    
    if len(discrepancies) > 0:
        print(f"\n🚨 EXAMPLES OF PRICING DISCREPANCIES (|difference| > 0.01):")
        print(f"Found {len(discrepancies):,} records with significant discrepancies")
        
        # Show top 10 largest discrepancies
        largest_discrepancies = discrepancies.nlargest(10, 'price_difference')[
            ['order_item_key', 'paid_price', 'original_unit_price', 'voucher_platform_amount', 
             'voucher_seller_amount', 'total_vouchers', 'expected_paid_price', 'price_difference']
        ]
        
        print("\nTop 10 largest positive discrepancies:")
        print(largest_discrepancies.to_string(index=False))
        
        # Show top 10 most negative discrepancies
        smallest_discrepancies = discrepancies.nsmallest(10, 'price_difference')[
            ['order_item_key', 'paid_price', 'original_unit_price', 'voucher_platform_amount', 
             'voucher_seller_amount', 'total_vouchers', 'expected_paid_price', 'price_difference']
        ]
        
        print("\nTop 10 largest negative discrepancies:")
        print(smallest_discrepancies.to_string(index=False))
    
    # Analyze zero-priced items
    zero_paid = df[df['paid_price'] == 0]
    if len(zero_paid) > 0:
        print(f"\n💰 ZERO-PAID PRICE ANALYSIS:")
        print(f"• Records with paid_price = 0: {len(zero_paid):,}")
        print(f"• Average original_unit_price for zero-paid: {zero_paid['original_unit_price'].mean():.2f}")
        print(f"• Average total_vouchers for zero-paid: {zero_paid['total_vouchers'].mean():.2f}")
    
    # Analyze negative prices
    negative_paid = df[df['paid_price'] < 0]
    if len(negative_paid) > 0:
        print(f"\n⚠️  NEGATIVE PAID PRICE ANALYSIS:")
        print(f"• Records with paid_price < 0: {len(negative_paid):,}")
        print("Sample negative price records:")
        print(negative_paid[['order_item_key', 'paid_price', 'original_unit_price', 'total_vouchers']].head().to_string(index=False))
    
    # Formula validation conclusion
    print(f"\n🎯 CONCLUSION:")
    accuracy_rate = near_matches / total_records * 100
    
    if accuracy_rate >= 99:
        print(f"✅ Formula is HIGHLY ACCURATE ({accuracy_rate:.2f}% of records match)")
        print("   The pricing formula paid_price = original_unit_price - total_vouchers is correct!")
    elif accuracy_rate >= 95:
        print(f"⚠️  Formula is MOSTLY ACCURATE ({accuracy_rate:.2f}% of records match)")
        print("   Minor discrepancies may be due to rounding or special cases")
    elif accuracy_rate >= 80:
        print(f"❌ Formula has MODERATE ACCURACY ({accuracy_rate:.2f}% of records match)")
        print("   Significant discrepancies detected - formula may need adjustment")
    else:
        print(f"❌ Formula is INACCURATE ({accuracy_rate:.2f}% of records match)")
        print("   Major issues with pricing logic - requires investigation")
    
    print("\n" + "="*80)
    
    return df

if __name__ == "__main__":
    analysis_df = analyze_pricing_formula()