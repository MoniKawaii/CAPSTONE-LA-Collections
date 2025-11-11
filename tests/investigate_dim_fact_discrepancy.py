#!/usr/bin/env python3
"""
Investigate the discrepancy between dim_order and fact_orders
9,038 orders in dim_order vs 7,846 unique orders in fact_orders
"""

import pandas as pd
import numpy as np

print("=" * 80)
print("🔍 INVESTIGATING DIM_ORDER vs FACT_ORDERS DISCREPANCY")
print("=" * 80)

# Load the data
dim_order = pd.read_csv('app/Transformed/dim_order.csv')
fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')

# Filter for Lazada only
lazada_dim = dim_order[dim_order['platform_key'] == 1].copy()
lazada_fact = fact_orders[fact_orders['platform_key'] == 1].copy()

print(f"📊 Data Overview:")
print(f"  Lazada orders in dim_order: {len(lazada_dim):,}")
print(f"  Lazada items in fact_orders: {len(lazada_fact):,}")
print(f"  Unique orders in fact_orders: {lazada_fact['orders_key'].nunique():,}")

# Get the order keys from both tables
dim_order_keys = set(lazada_dim['orders_key'])
fact_order_keys = set(lazada_fact['orders_key'])

print(f"\n🔍 Order Key Analysis:")
print(f"  dim_order order_keys: {len(dim_order_keys):,}")
print(f"  fact_orders order_keys: {len(fact_order_keys):,}")

# Find missing orders
missing_from_fact = dim_order_keys - fact_order_keys
extra_in_fact = fact_order_keys - dim_order_keys
common_orders = dim_order_keys.intersection(fact_order_keys)

print(f"\n📊 Comparison Results:")
print(f"  Orders in both tables: {len(common_orders):,}")
print(f"  Orders only in dim_order: {len(missing_from_fact):,}")
print(f"  Orders only in fact_orders: {len(extra_in_fact):,}")

if len(missing_from_fact) > 0:
    print(f"\n⚠️  ORDERS MISSING FROM FACT_ORDERS:")
    
    # Analyze the missing orders
    missing_orders = lazada_dim[lazada_dim['orders_key'].isin(missing_from_fact)]
    
    print(f"  Total missing: {len(missing_orders):,}")
    
    # Status breakdown
    missing_status = missing_orders['order_status'].value_counts()
    print(f"\n📋 Missing orders by status:")
    for status, count in missing_status.items():
        pct = (count / len(missing_orders)) * 100
        print(f"    {status}: {count:,} ({pct:.1f}%)")
    
    # Date analysis
    missing_orders['order_date'] = pd.to_datetime(missing_orders['order_date'])
    print(f"\n📅 Missing orders date range:")
    print(f"    Earliest: {missing_orders['order_date'].min().date()}")
    print(f"    Latest: {missing_orders['order_date'].max().date()}")
    
    # Monthly breakdown
    missing_orders['order_month'] = missing_orders['order_date'].dt.to_period('M')
    missing_monthly = missing_orders['order_month'].value_counts().sort_index()
    print(f"\n📊 Missing orders by month (last 12):")
    for month, count in missing_monthly.tail(12).items():
        print(f"    {month}: {count:,}")
    
    # Sample missing orders
    print(f"\n📝 Sample missing orders:")
    sample_missing = missing_orders.head(10)
    for _, order in sample_missing.iterrows():
        order_date_str = order['order_date'].strftime('%Y-%m-%d') if pd.notna(order['order_date']) else 'N/A'
        print(f"    Order key {order['orders_key']}: {order['platform_order_id']} ({order['order_status']}) - {order_date_str}")

if len(extra_in_fact) > 0:
    print(f"\n⚠️  EXTRA ORDERS IN FACT_ORDERS:")
    print(f"  Count: {len(extra_in_fact):,}")
    print(f"  Sample extra order_keys: {list(extra_in_fact)[:10]}")

# Investigate by order status in detail
print(f"\n📊 DETAILED STATUS COMPARISON:")
print(f"{'Status':<20} {'dim_order':<10} {'fact_orders':<12} {'Missing':<8} {'Rate':<8}")
print("-" * 65)

status_comparison = []

for status in lazada_dim['order_status'].unique():
    dim_count = len(lazada_dim[lazada_dim['order_status'] == status])
    
    # Get unique orders in fact_orders with this status
    status_orders = lazada_dim[lazada_dim['order_status'] == status]['orders_key']
    fact_count = lazada_fact[lazada_fact['orders_key'].isin(status_orders)]['orders_key'].nunique()
    
    missing = dim_count - fact_count
    rate = (fact_count / dim_count * 100) if dim_count > 0 else 0
    
    print(f"{status:<20} {dim_count:<10} {fact_count:<12} {missing:<8} {rate:.1f}%")
    
    status_comparison.append({
        'status': status,
        'dim_count': dim_count,
        'fact_count': fact_count,
        'missing': missing,
        'rate': rate
    })

# Find the most problematic status
status_df = pd.DataFrame(status_comparison)
worst_status = status_df.loc[status_df['missing'].idxmax()]

print(f"\n🎯 WORST PERFORMING STATUS:")
print(f"  Status: {worst_status['status']}")
print(f"  Missing: {worst_status['missing']:,} orders ({worst_status['rate']:.1f}% completion)")

# Check if the issue is related to order items
if worst_status['missing'] > 0:
    print(f"\n🔍 INVESTIGATING {worst_status['status']} ORDERS:")
    
    problem_orders = lazada_dim[
        (lazada_dim['order_status'] == worst_status['status']) & 
        (lazada_dim['orders_key'].isin(missing_from_fact))
    ]
    
    print(f"  Sample problem orders:")
    for _, order in problem_orders.head(5).iterrows():
        print(f"    Order key {order['orders_key']}: {order['platform_order_id']}")
        print(f"      Status: {order['order_status']}")
        print(f"      Date: {str(order['order_date'])[:10] if pd.notna(order['order_date']) else 'N/A'}")
        print(f"      Price: ₱{order['price_total']:,.2f}")
        
        # Check if this order appears anywhere in fact_orders
        fact_matches = fact_orders[fact_orders['orders_key'] == order['orders_key']]
        print(f"      In fact_orders: {len(fact_matches)} items")

print(f"\n💡 SUMMARY:")
print(f"  • {len(missing_from_fact):,} orders from dim_order are missing from fact_orders")
print(f"  • This represents {(len(missing_from_fact) / len(lazada_dim) * 100):.1f}% of all Lazada orders")
print(f"  • Most missing orders have status: {worst_status['status']}")
print(f"  • This suggests fact_orders filtering logic excludes certain order statuses")

print(f"\n🔧 RECOMMENDED ACTIONS:")
print(f"  1. ✅ Review fact_orders creation logic in transformation scripts")
print(f"  2. ✅ Check if {worst_status['status']} orders should be included in fact_orders")
print(f"  3. ✅ Verify business rules for order inclusion in fact table")
print(f"  4. ✅ Document why certain order statuses are excluded from fact_orders")
print(f"  5. ✅ Consider if this is intentional or needs fixing")