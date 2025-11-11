import pandas as pd
import numpy as np

# Load data
dim_order = pd.read_csv('app/Transformed/dim_order.csv')
fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')

# Focus on Lazada COMPLETED orders
lazada_completed = dim_order[(dim_order['platform_key'] == 1) & (dim_order['order_status'] == 'COMPLETED')].copy()
fact_order_keys = set(fact_orders[fact_orders['platform_key'] == 1]['orders_key'])

# Split into successful vs missing
successful = lazada_completed[lazada_completed['orders_key'].isin(fact_order_keys)]
missing = lazada_completed[~lazada_completed['orders_key'].isin(fact_order_keys)]

print('📊 PRICE ANALYSIS FOR COMPLETED ORDERS:')
print(f'Successful orders - Total: {len(successful):,}')
print(f'Successful orders - NaN prices: {successful["price_total"].isna().sum():,}')
print(f'Successful orders - Zero prices: {(successful["price_total"] == 0).sum():,}')
print(f'Successful orders - Valid prices: {successful["price_total"].notna().sum():,}')

print(f'\nMissing orders - Total: {len(missing):,}')
print(f'Missing orders - NaN prices: {missing["price_total"].isna().sum():,}')
print(f'Missing orders - Zero prices: {(missing["price_total"] == 0).sum():,}')
print(f'Missing orders - Valid prices: {missing["price_total"].notna().sum():,}')

print(f'\n💡 KEY FINDING:')
if missing['price_total'].isna().all():
    print('🎯 ALL missing COMPLETED orders have NaN/null prices!')
    print('🔍 This explains why they are excluded from fact_orders')
    print('📋 fact_orders requires valid price data for analytical purposes')
else:
    print('Mixed price validity in missing orders')

print(f'\n📊 PERCENTAGE BREAKDOWN:')
missing_nan_pct = (missing['price_total'].isna().sum() / len(missing)) * 100 if len(missing) > 0 else 0
successful_nan_pct = (successful['price_total'].isna().sum() / len(successful)) * 100 if len(successful) > 0 else 0

print(f'Missing orders with NaN prices: {missing_nan_pct:.1f}%')
print(f'Successful orders with NaN prices: {successful_nan_pct:.1f}%')

if missing_nan_pct == 100.0 and successful_nan_pct < 5.0:
    print(f'\n✅ CONCLUSION: The fact_orders discrepancy is explained!')
    print(f'   - Missing orders have invalid/null prices and are filtered out')
    print(f'   - This is likely intentional business logic for analytical integrity')
    print(f'   - {len(missing):,} orders ({missing_nan_pct:.1f}%) have price validation issues')