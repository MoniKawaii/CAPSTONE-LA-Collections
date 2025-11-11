#!/usr/bin/env python3
"""
Investigate the 593 missing COMPLETED orders from fact_orders
Since fact_orders intentionally only includes COMPLETED orders,
we need to understand why some COMPLETED orders are still missing
"""

import pandas as pd
import json
import numpy as np

print("=" * 80)
print("🔍 INVESTIGATING MISSING COMPLETED ORDERS IN FACT_ORDERS")
print("=" * 80)

# Load the data
dim_order = pd.read_csv('app/Transformed/dim_order.csv')
fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')

# Filter for Lazada COMPLETED orders only
lazada_completed_dim = dim_order[(dim_order['platform_key'] == 1) & (dim_order['order_status'] == 'COMPLETED')].copy()
lazada_fact = fact_orders[fact_orders['platform_key'] == 1].copy()

print(f"📊 COMPLETED Orders Analysis:")
print(f"  COMPLETED orders in dim_order: {len(lazada_completed_dim):,}")
print(f"  Unique orders in fact_orders: {lazada_fact['orders_key'].nunique():,}")

# Get the order keys
completed_order_keys = set(lazada_completed_dim['orders_key'])
fact_order_keys = set(lazada_fact['orders_key'])

# Find missing COMPLETED orders
missing_completed = completed_order_keys - fact_order_keys
common_completed = completed_order_keys.intersection(fact_order_keys)

print(f"\n🔍 COMPLETED Orders Comparison:")
print(f"  COMPLETED orders that should be in fact_orders: {len(completed_order_keys):,}")
print(f"  COMPLETED orders actually in fact_orders: {len(common_completed):,}")
print(f"  Missing COMPLETED orders: {len(missing_completed):,}")

completion_rate = (len(common_completed) / len(completed_order_keys) * 100) if len(completed_order_keys) > 0 else 0
print(f"  ✅ COMPLETED orders inclusion rate: {completion_rate:.1f}%")

if len(missing_completed) > 0:
    print(f"\n⚠️  ANALYSIS OF {len(missing_completed):,} MISSING COMPLETED ORDERS:")
    
    # Get details of missing COMPLETED orders
    missing_orders_df = lazada_completed_dim[lazada_completed_dim['orders_key'].isin(missing_completed)].copy()
    
    # Date analysis
    missing_orders_df['order_date'] = pd.to_datetime(missing_orders_df['order_date'])
    missing_orders_df['order_month'] = missing_orders_df['order_date'].dt.to_period('M')
    
    print(f"\n📅 Missing COMPLETED orders by date range:")
    print(f"    Earliest: {missing_orders_df['order_date'].min().date()}")
    print(f"    Latest: {missing_orders_df['order_date'].max().date()}")
    
    # Monthly distribution
    monthly_missing = missing_orders_df['order_month'].value_counts().sort_index()
    print(f"\n📊 Missing COMPLETED orders by month:")
    for month, count in monthly_missing.tail(15).items():
        print(f"    {month}: {count:,} missing")
    
    # Check if these orders have items in the raw data
    print(f"\n🔍 INVESTIGATING WHY COMPLETED ORDERS ARE MISSING:")
    
    # Load raw order items to check if missing orders have items
    try:
        with open('app/Staging/lazada_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
            raw_items = json.load(f)
        
        items_df = pd.DataFrame(raw_items)
        
        # Extract order numbers from items
        if 'order_id' in items_df.columns:
            items_df['order_number'] = items_df['order_id'].astype(str)
        elif 'order_number' in items_df.columns:
            items_df['order_number'] = items_df['order_number'].astype(str)
        
        if 'order_number' in items_df.columns:
            raw_items_orders = set(items_df['order_number'].unique())
            
            # Check if missing COMPLETED orders have items in raw data
            missing_platform_ids = set(missing_orders_df['platform_order_id'].astype(str))
            missing_with_raw_items = missing_platform_ids.intersection(raw_items_orders)
            missing_without_raw_items = missing_platform_ids - raw_items_orders
            
            print(f"    Missing COMPLETED orders with items in raw data: {len(missing_with_raw_items):,}")
            print(f"    Missing COMPLETED orders without items in raw data: {len(missing_without_raw_items):,}")
            
            if len(missing_without_raw_items) > 0:
                pct_no_items = (len(missing_without_raw_items) / len(missing_completed) * 100)
                print(f"    ⚠️  {pct_no_items:.1f}% of missing COMPLETED orders have NO ITEMS in raw data")
                print(f"    This explains why they're missing from fact_orders (no items to create fact records)")
                
            if len(missing_with_raw_items) > 0:
                pct_with_items = (len(missing_with_raw_items) / len(missing_completed) * 100)
                print(f"    ❓ {pct_with_items:.1f}% of missing COMPLETED orders DO HAVE items in raw data")
                print(f"    This suggests other filtering logic is excluding them")
    
    except Exception as e:
        print(f"    ❌ Could not load raw items data: {e}")
    
    # Sample analysis of missing orders
    print(f"\n📝 SAMPLE MISSING COMPLETED ORDERS:")
    sample_missing = missing_orders_df.head(10)
    
    for _, order in sample_missing.iterrows():
        print(f"    Order key {order['orders_key']}: {order['platform_order_id']}")
        print(f"      Date: {order['order_date'].strftime('%Y-%m-%d')}")
        print(f"      Price: ₱{order['price_total']:,.2f}")
        
        # Check if this specific order has any fact records
        fact_matches = fact_orders[fact_orders['orders_key'] == order['orders_key']]
        print(f"      Items in fact_orders: {len(fact_matches)}")
        
        if len(fact_matches) == 0:
            # This order should have items but doesn't - investigate why
            print(f"      ⚠️  This COMPLETED order should have fact records but doesn't!")
        print()

# Compare with successful COMPLETED orders
print(f"\n🔍 COMPARING WITH SUCCESSFUL COMPLETED ORDERS:")

successful_completed = lazada_completed_dim[lazada_completed_dim['orders_key'].isin(common_completed)].copy()
successful_completed['order_date'] = pd.to_datetime(successful_completed['order_date'])

print(f"📊 Successful vs Missing COMPLETED Orders:")
print(f"  Successful COMPLETED orders: {len(successful_completed):,}")
print(f"  Missing COMPLETED orders: {len(missing_orders_df):,}")

# Compare date ranges
print(f"\n📅 Date Range Comparison:")
print(f"  Successful - Earliest: {successful_completed['order_date'].min().date()}")
print(f"  Successful - Latest: {successful_completed['order_date'].max().date()}")
if len(missing_orders_df) > 0:
    print(f"  Missing - Earliest: {missing_orders_df['order_date'].min().date()}")
    print(f"  Missing - Latest: {missing_orders_df['order_date'].max().date()}")

# Compare price distributions
print(f"\n💰 Price Comparison:")
print(f"  Successful COMPLETED orders - Average price: ₱{successful_completed['price_total'].mean():.2f}")
print(f"  Successful COMPLETED orders - Zero price count: {(successful_completed['price_total'] == 0).sum():,}")

if len(missing_orders_df) > 0:
    print(f"  Missing COMPLETED orders - Average price: ₱{missing_orders_df['price_total'].mean():.2f}")
    print(f"  Missing COMPLETED orders - Zero price count: {(missing_orders_df['price_total'] == 0).sum():,}")
    
    zero_price_missing = (missing_orders_df['price_total'] == 0).sum()
    if zero_price_missing > 0:
        pct_zero_price = (zero_price_missing / len(missing_orders_df) * 100)
        print(f"    ⚠️  {pct_zero_price:.1f}% of missing orders have zero price!")
        print(f"    Zero-price orders might be filtered out of fact_orders")

# Monthly success rate analysis
print(f"\n📈 MONTHLY SUCCESS RATES FOR COMPLETED ORDERS:")
print(f"{'Month':<12} {'Total':<8} {'In Fact':<8} {'Missing':<9} {'Rate':<8}")
print("-" * 55)

# Get monthly counts for both successful and missing
successful_completed['order_month'] = successful_completed['order_date'].dt.to_period('M')
successful_monthly = successful_completed['order_month'].value_counts().sort_index()

if len(missing_orders_df) > 0:
    missing_monthly = missing_orders_df['order_month'].value_counts().sort_index()
    
    # Combine data
    all_months = set(successful_monthly.index) | set(missing_monthly.index)
    
    for month in sorted(all_months)[-12:]:  # Last 12 months
        successful_count = successful_monthly.get(month, 0)
        missing_count = missing_monthly.get(month, 0)
        total_count = successful_count + missing_count
        
        if total_count > 0:
            success_rate = (successful_count / total_count) * 100
            print(f"{str(month):<12} {total_count:<8} {successful_count:<8} {missing_count:<9} {success_rate:.1f}%")

print(f"\n💡 SUMMARY OF COMPLETED ORDERS INVESTIGATION:")
print(f"  • {len(missing_completed):,} COMPLETED orders missing from fact_orders")
print(f"  • This represents {(len(missing_completed) / len(completed_order_keys) * 100):.1f}% of all COMPLETED orders")
print(f"  • Success rate for COMPLETED orders: {completion_rate:.1f}%")

print(f"\n🔧 LIKELY CAUSES:")
print(f"  1. ✅ Orders without items in raw data (cannot create fact records)")
print(f"  2. ✅ Zero-price orders filtered out")
print(f"  3. ✅ Data validation failures in transformation")
print(f"  4. ✅ Order-item mismatch during processing")
print(f"  5. ✅ Transformation script filtering logic")

print(f"\n🎯 RECOMMENDED NEXT STEPS:")
print(f"  1. ✅ Check transformation scripts for COMPLETED order filtering")
print(f"  2. ✅ Verify if zero-price orders should be included")
print(f"  3. ✅ Investigate order-item matching logic")
print(f"  4. ✅ Review data validation rules in fact_orders creation")
print(f"  5. ✅ Consider business impact of missing {len(missing_completed):,} orders")