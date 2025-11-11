#!/usr/bin/env python3

from app.Transformation.harmonize_fact_orders import harmonize_fact_orders
import pandas as pd

print('🚀 Testing updated harmonize_fact_orders with flattened Shopee data...')
fact_df = harmonize_fact_orders()

print(f'\n📊 RESULTS SUMMARY:')
print(f'Total records: {len(fact_df)}')

if len(fact_df) > 0:
    platform_counts = fact_df['platform_key'].value_counts()
    print(f'Platform breakdown:')
    for platform, count in platform_counts.items():
        platform_name = 'Lazada' if platform == 1 else 'Shopee' if platform == 2 else f'Platform {platform}'
        print(f'  {platform_name}: {count:,} records')
    
    date_min = fact_df['time_key'].min()
    date_max = fact_df['time_key'].max()
    print(f'Date range: {date_min} to {date_max}')
    
    paid_records = len(fact_df[fact_df['paid_price'] > 0])
    voucher_records = len(fact_df[(fact_df['voucher_platform_amount'] > 0) | (fact_df['voucher_seller_amount'] > 0)])
    
    print(f'Pricing accuracy check:')
    print(f'  Records with paid_price > 0: {paid_records:,}')
    print(f'  Records with vouchers: {voucher_records:,}')
    
    # Show sample of each platform
    print(f'\n📋 Sample Lazada records:')
    lazada_sample = fact_df[fact_df['platform_key'] == 1].head(2)
    if not lazada_sample.empty:
        for col in ['order_item_key', 'orders_key', 'paid_price', 'voucher_platform_amount']:
            print(f'  {col}: {lazada_sample[col].tolist()}')
    
    print(f'\n📋 Sample Shopee records:')
    shopee_sample = fact_df[fact_df['platform_key'] == 2].head(2)
    if not shopee_sample.empty:
        for col in ['order_item_key', 'orders_key', 'paid_price', 'voucher_platform_amount']:
            print(f'  {col}: {shopee_sample[col].tolist()}')

print('✅ Test completed!')