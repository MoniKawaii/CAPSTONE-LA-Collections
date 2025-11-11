#!/usr/bin/env python3
"""
Check voucher amounts in fact_orders.csv to see if payment details are being read correctly
"""

import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def check_voucher_amounts():
    """Check voucher amounts in fact orders"""
    try:
        print("Loading fact_orders.csv...")
        df = pd.read_csv('../app/Transformed/fact_orders.csv')
        
        print(f"\nTotal fact order records: {len(df):,}")
        
        # Check voucher columns exist
        voucher_cols = ['voucher_platform_amount', 'voucher_seller_amount']
        existing_cols = [col for col in voucher_cols if col in df.columns]
        print(f"Voucher columns found: {existing_cols}")
        
        if existing_cols:
            for col in existing_cols:
                non_zero = (df[col] != 0).sum()
                total_amount = df[col].sum()
                avg_amount = df[col].mean()
                max_amount = df[col].max()
                
                print(f"\n{col}:")
                print(f"  - Records with non-zero amounts: {non_zero:,}")
                print(f"  - Total amount: ₱{total_amount:,.2f}")
                print(f"  - Average amount: ₱{avg_amount:.2f}")
                print(f"  - Maximum amount: ₱{max_amount:.2f}")
        
        # Check by platform
        if 'platform_key' in df.columns:
            print(f"\nVoucher amounts by platform:")
            for platform in df['platform_key'].unique():
                platform_df = df[df['platform_key'] == platform]
                print(f"\n{platform}:")
                print(f"  - Total orders: {len(platform_df):,}")
                
                for col in existing_cols:
                    if col in df.columns:
                        non_zero = (platform_df[col] != 0).sum()
                        total = platform_df[col].sum()
                        print(f"  - {col}: {non_zero:,} non-zero (₱{total:,.2f} total)")
        
        # Sample records with vouchers
        if existing_cols:
            has_vouchers = df[df[existing_cols].sum(axis=1) > 0]
            if len(has_vouchers) > 0:
                print(f"\nSample records with vouchers (showing first 5):")
                print(has_vouchers[['order_sn', 'platform_key'] + existing_cols].head().to_string())
            else:
                print(f"\nNo records found with voucher amounts > 0")
        
    except Exception as e:
        print(f"Error checking voucher amounts: {e}")

if __name__ == "__main__":
    check_voucher_amounts()