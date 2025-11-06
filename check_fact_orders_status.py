#!/usr/bin/env python3
"""
Check Current Fact Orders Status
Shows the current state of fact_orders.csv harmonization
"""

import pandas as pd
import os

def check_fact_orders_status():
    fact_orders_path = 'app/Transformed/fact_orders.csv'
    
    if os.path.exists(fact_orders_path):
        df = pd.read_csv(fact_orders_path)
        print(f'=== FACT_ORDERS.CSV STATUS ===')
        print(f'Total records: {len(df):,}')
        print(f'Total revenue: ${df["paid_price"].sum():,.2f}')
        
        # Platform breakdown
        platform_counts = df['platform_key'].value_counts().sort_index()
        for platform_key, count in platform_counts.items():
            platform_name = 'Lazada' if platform_key == 1 else 'Shopee'
            revenue = df[df['platform_key'] == platform_key]['paid_price'].sum()
            print(f'{platform_name} (key {platform_key}): {count:,} records, ${revenue:,.2f} revenue')
        
        print(f'\nDate range: {df["time_key"].min()} to {df["time_key"].max()}')
        print(f'Average order value: ${df["paid_price"].mean():.2f}')
        
        # Check for missing values in foreign keys
        print(f'\n=== FOREIGN KEY COVERAGE ===')
        orders_coverage = df["orders_key"].notna().sum()
        customer_coverage = df["customer_key"].notna().sum()
        product_coverage = df["product_key"].notna().sum()
        
        print(f'Valid orders_key: {orders_coverage:,} / {len(df):,} ({orders_coverage/len(df)*100:.1f}%)')
        print(f'Valid customer_key: {customer_coverage:,} / {len(df):,} ({customer_coverage/len(df)*100:.1f}%)')
        print(f'Valid product_key: {product_coverage:,} / {len(df):,} ({product_coverage/len(df)*100:.1f}%)')
        
        # Check for any missing records
        missing_orders = orders_coverage != len(df)
        missing_customers = customer_coverage != len(df)
        missing_products = product_coverage != len(df)
        
        if missing_orders or missing_customers or missing_products:
            print(f'\n=== MISSING RECORDS ANALYSIS ===')
            if missing_orders:
                missing_orders_count = len(df) - orders_coverage
                print(f'Missing orders_key: {missing_orders_count:,} records')
            if missing_customers:
                missing_customers_count = len(df) - customer_coverage
                print(f'Missing customer_key: {missing_customers_count:,} records')
            if missing_products:
                missing_products_count = len(df) - product_coverage
                print(f'Missing product_key: {missing_products_count:,} records')
        else:
            print(f'\n✅ NO MISSING RECORDS - 100% coverage achieved!')
        
        # Check for any potential missing orders by looking for gaps
        print(f'\n=== SAMPLE DATA ===')
        sample_cols = ['order_item_key', 'orders_key', 'customer_key', 'platform_key', 'paid_price']
        print(df.head(3)[sample_cols].to_string(index=False))
        
    else:
        print('❌ fact_orders.csv not found!')

if __name__ == "__main__":
    check_fact_orders_status()