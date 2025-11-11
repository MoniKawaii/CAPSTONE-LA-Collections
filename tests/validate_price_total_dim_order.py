#!/usr/bin/env python3
"""
Validate price_total in dim_order for Lazada orders (platform_key = 1)
Analyze the price data quality and identify patterns in NaN/null values
"""

import pandas as pd
import numpy as np
import json

print("=" * 80)
print("🔍 VALIDATING PRICE_TOTAL IN DIM_ORDER FOR LAZADA ORDERS")
print("=" * 80)

# Load dim_order data
dim_order = pd.read_csv('app/Transformed/dim_order.csv')
lazada_orders = dim_order[dim_order['platform_key'] == 1].copy()

print(f"📊 LAZADA ORDERS OVERVIEW:")
print(f"  Total Lazada orders: {len(lazada_orders):,}")
print(f"  Date range: {lazada_orders['order_date'].min()} to {lazada_orders['order_date'].max()}")

# Analyze price_total values
print(f"\n💰 PRICE_TOTAL ANALYSIS:")
print(f"  Total orders: {len(lazada_orders):,}")
print(f"  Orders with valid prices: {lazada_orders['price_total'].notna().sum():,}")
print(f"  Orders with NaN prices: {lazada_orders['price_total'].isna().sum():,}")
print(f"  Orders with zero prices: {(lazada_orders['price_total'] == 0).sum():,}")
print(f"  Orders with negative prices: {(lazada_orders['price_total'] < 0).sum():,}")

# Calculate percentages
total_orders = len(lazada_orders)
valid_price_pct = (lazada_orders['price_total'].notna().sum() / total_orders) * 100
nan_price_pct = (lazada_orders['price_total'].isna().sum() / total_orders) * 100
zero_price_pct = ((lazada_orders['price_total'] == 0).sum() / total_orders) * 100

print(f"\n📊 PRICE QUALITY PERCENTAGES:")
print(f"  Valid prices: {valid_price_pct:.1f}%")
print(f"  NaN prices: {nan_price_pct:.1f}%")
print(f"  Zero prices: {zero_price_pct:.1f}%")

# Analyze by order status
print(f"\n📋 PRICE QUALITY BY ORDER STATUS:")
status_price_analysis = lazada_orders.groupby('order_status').agg({
    'orders_key': 'count',
    'price_total': ['count', lambda x: x.isna().sum(), lambda x: (x == 0).sum()]
}).round(2)

status_price_analysis.columns = ['Total_Orders', 'Valid_Prices', 'NaN_Prices', 'Zero_Prices']
status_price_analysis['Valid_Price_Rate'] = (status_price_analysis['Valid_Prices'] / status_price_analysis['Total_Orders'] * 100).round(1)
status_price_analysis['NaN_Price_Rate'] = (status_price_analysis['NaN_Prices'] / status_price_analysis['Total_Orders'] * 100).round(1)

print(status_price_analysis)

# Analyze by date patterns
print(f"\n📅 PRICE QUALITY BY TIME PERIOD:")
lazada_orders['order_date'] = pd.to_datetime(lazada_orders['order_date'])
lazada_orders['order_month'] = lazada_orders['order_date'].dt.to_period('M')

monthly_price_analysis = lazada_orders.groupby('order_month').agg({
    'orders_key': 'count',
    'price_total': ['count', lambda x: x.isna().sum()]
}).round(2)

monthly_price_analysis.columns = ['Total_Orders', 'Valid_Prices', 'NaN_Prices']
monthly_price_analysis['Valid_Price_Rate'] = (monthly_price_analysis['Valid_Prices'] / monthly_price_analysis['Total_Orders'] * 100).round(1)

print(f"\n📈 MONTHLY PRICE QUALITY (Last 24 months):")
print(f"{'Month':<12} {'Total':<8} {'Valid':<8} {'NaN':<6} {'Rate':<8}")
print("-" * 50)

for month, row in monthly_price_analysis.tail(24).iterrows():
    total = int(row['Total_Orders'])
    valid = int(row['Valid_Prices'])
    nan_count = int(row['NaN_Prices'])
    rate = row['Valid_Price_Rate']
    
    print(f"{str(month):<12} {total:<8} {valid:<8} {nan_count:<6} {rate:.1f}%")

# Investigate orders with NaN prices
nan_price_orders = lazada_orders[lazada_orders['price_total'].isna()].copy()

if len(nan_price_orders) > 0:
    print(f"\n🔍 ANALYZING {len(nan_price_orders):,} ORDERS WITH NaN PRICES:")
    
    # Status breakdown of NaN price orders
    nan_status_breakdown = nan_price_orders['order_status'].value_counts()
    print(f"\n📋 NaN price orders by status:")
    for status, count in nan_status_breakdown.items():
        pct = (count / len(nan_price_orders)) * 100
        print(f"  {status}: {count:,} ({pct:.1f}%)")
    
    # Date range of NaN price orders
    print(f"\n📅 NaN price orders date range:")
    print(f"  Earliest: {nan_price_orders['order_date'].min().date()}")
    print(f"  Latest: {nan_price_orders['order_date'].max().date()}")
    
    # Sample NaN price orders
    print(f"\n📝 SAMPLE NaN PRICE ORDERS:")
    sample_nan = nan_price_orders.head(10)
    for _, order in sample_nan.iterrows():
        print(f"  Order key {order['orders_key']}: {order['platform_order_id']}")
        print(f"    Status: {order['order_status']}")
        print(f"    Date: {order['order_date'].strftime('%Y-%m-%d')}")
        print(f"    Price: {order['price_total']}")

# Compare with fact_orders to see if prices are available there
print(f"\n🔍 CROSS-REFERENCE WITH FACT_ORDERS:")

try:
    fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
    lazada_fact = fact_orders[fact_orders['platform_key'] == 1].copy()
    
    print(f"  Fact orders loaded: {len(lazada_fact):,} items")
    
    # For NaN price orders, check if they have prices in fact_orders
    if len(nan_price_orders) > 0:
        sample_nan_keys = nan_price_orders['orders_key'].head(10).tolist()
        
        print(f"\n📊 SAMPLE NaN PRICE ORDERS IN FACT_ORDERS:")
        for order_key in sample_nan_keys:
            fact_items = lazada_fact[lazada_fact['orders_key'] == order_key]
            if len(fact_items) > 0:
                total_price = fact_items['paid_price'].sum()
                print(f"  Order key {order_key}:")
                print(f"    Items in fact_orders: {len(fact_items)}")
                print(f"    Total paid_price: ₱{total_price:.2f}")
                print(f"    Average item price: ₱{fact_items['paid_price'].mean():.2f}")
            else:
                print(f"  Order key {order_key}: No items in fact_orders")
        
        # Check if NaN price orders have corresponding fact records
        nan_orders_with_facts = set(nan_price_orders['orders_key']).intersection(set(lazada_fact['orders_key']))
        nan_orders_without_facts = set(nan_price_orders['orders_key']) - set(lazada_fact['orders_key'])
        
        print(f"\n📊 NaN PRICE ORDERS vs FACT_ORDERS:")
        print(f"  NaN price orders with fact records: {len(nan_orders_with_facts):,}")
        print(f"  NaN price orders without fact records: {len(nan_orders_without_facts):,}")
        
        if len(nan_orders_with_facts) > 0:
            print(f"  ⚠️  {len(nan_orders_with_facts):,} orders have NaN prices in dim_order but valid prices in fact_orders!")
            print(f"  This suggests a problem in the price rollup from fact_orders to dim_order")

except Exception as e:
    print(f"  ❌ Could not load fact_orders: {e}")

# Investigate valid price orders for comparison
valid_price_orders = lazada_orders[lazada_orders['price_total'].notna()].copy()

print(f"\n📊 VALID PRICE ORDERS ANALYSIS:")
print(f"  Orders with valid prices: {len(valid_price_orders):,}")
print(f"  Price statistics:")
print(f"    Mean: ₱{valid_price_orders['price_total'].mean():.2f}")
print(f"    Median: ₱{valid_price_orders['price_total'].median():.2f}")
print(f"    Min: ₱{valid_price_orders['price_total'].min():.2f}")
print(f"    Max: ₱{valid_price_orders['price_total'].max():.2f}")

# Check if there's a pattern in the data structure
print(f"\n🔍 DATA STRUCTURE VALIDATION:")
print(f"  dim_order columns: {list(lazada_orders.columns)}")
print(f"  price_total data type: {lazada_orders['price_total'].dtype}")
print(f"  Unique price_total values (first 10): {sorted(lazada_orders['price_total'].dropna().unique())[:10]}")

# Check for any correlation with other fields
print(f"\n🔗 CORRELATION ANALYSIS:")
print(f"  Orders with NaN price_total and zero revenue: {((lazada_orders['price_total'].isna()) & (lazada_orders.get('revenue', 0) == 0)).sum()}")

# Load raw data to check if prices exist there
print(f"\n🔍 CHECKING RAW DATA FOR PRICE INFORMATION:")

try:
    with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
        raw_orders = json.load(f)
    
    raw_df = pd.DataFrame(raw_orders)
    print(f"  Raw orders loaded: {len(raw_df):,}")
    
    # Check price field in raw data
    if 'price' in raw_df.columns:
        raw_df['order_number'] = raw_df['order_number'].astype(str)
        print(f"  Raw orders with price field: {raw_df['price'].notna().sum():,}")
        print(f"  Raw orders with valid price: {(raw_df['price'] != '0.00').sum():,}")
        
        # Sample raw prices
        sample_raw_prices = raw_df['price'].head(10).tolist()
        print(f"  Sample raw prices: {sample_raw_prices}")
        
        # Cross-reference with NaN price orders
        if len(nan_price_orders) > 0:
            sample_platform_ids = nan_price_orders['platform_order_id'].head(5).astype(str).tolist()
            print(f"\n📊 RAW PRICES FOR SAMPLE NaN ORDERS:")
            
            for platform_id in sample_platform_ids:
                raw_order = raw_df[raw_df['order_number'] == platform_id]
                if len(raw_order) > 0:
                    raw_price = raw_order['price'].iloc[0]
                    print(f"    Order {platform_id}: Raw price = {raw_price}")
                else:
                    print(f"    Order {platform_id}: Not found in raw data")

except Exception as e:
    print(f"  ❌ Could not load raw orders: {e}")

print(f"\n💡 SUMMARY:")
print(f"  • Total Lazada orders: {len(lazada_orders):,}")
print(f"  • Orders with valid prices: {valid_price_pct:.1f}%")
print(f"  • Orders with NaN prices: {nan_price_pct:.1f}%")
print(f"  • Price quality varies by status and time period")

print(f"\n🔧 RECOMMENDED ACTIONS:")
print(f"  1. ✅ Investigate price calculation/rollup logic from fact_orders to dim_order")
print(f"  2. ✅ Check if NaN price orders have valid prices in fact_orders")
print(f"  3. ✅ Review transformation scripts for price aggregation")
print(f"  4. ✅ Validate raw data price fields")
print(f"  5. ✅ Fix price rollup for {len(nan_price_orders):,} orders with missing prices")