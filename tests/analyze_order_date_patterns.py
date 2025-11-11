#!/usr/bin/env python3
"""
Analyze Order Date Patterns: Lazada vs Shopee
Investigate why Lazada has more unique order dates than Shopee
"""

import pandas as pd
import numpy as np
from datetime import datetime

def analyze_order_date_patterns():
    print("=== ORDER DATE PATTERN ANALYSIS ===")
    
    # Load the dimensional and fact data
    dim_order = pd.read_csv('app/Transformed/dim_order.csv')
    fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
    
    # Convert order_date to datetime
    dim_order['order_date'] = pd.to_datetime(dim_order['order_date'])
    
    print(f"Total orders in dim_order: {len(dim_order):,}")
    print(f"Total records in fact_orders: {len(fact_orders):,}")
    
    # Separate by platform
    lazada_orders = dim_order[dim_order['platform_key'] == 1].copy()
    shopee_orders = dim_order[dim_order['platform_key'] == 2].copy()
    
    print(f"\n=== BASIC ORDER COUNTS ===")
    print(f"Lazada total orders: {len(lazada_orders):,}")
    print(f"Shopee total orders: {len(shopee_orders):,}")
    
    # Analyze date ranges
    print(f"\n=== DATE RANGES ===")
    if len(lazada_orders) > 0:
        lazada_start = lazada_orders['order_date'].min()
        lazada_end = lazada_orders['order_date'].max()
        lazada_span = (lazada_end - lazada_start).days
        print(f"Lazada: {lazada_start.date()} to {lazada_end.date()} ({lazada_span:,} days)")
    
    if len(shopee_orders) > 0:
        shopee_start = shopee_orders['order_date'].min()
        shopee_end = shopee_orders['order_date'].max()
        shopee_span = (shopee_end - shopee_start).days
        print(f"Shopee: {shopee_start.date()} to {shopee_end.date()} ({shopee_span:,} days)")
    
    # Analyze unique dates
    print(f"\n=== UNIQUE ORDER DATES ===")
    lazada_unique_dates = lazada_orders['order_date'].dt.date.nunique()
    shopee_unique_dates = shopee_orders['order_date'].dt.date.nunique()
    
    print(f"Lazada unique order dates: {lazada_unique_dates:,}")
    print(f"Shopee unique order dates: {shopee_unique_dates:,}")
    
    if lazada_unique_dates > shopee_unique_dates:
        difference = lazada_unique_dates - shopee_unique_dates
        print(f"✅ Lazada has {difference:,} more unique order dates than Shopee")
    else:
        difference = shopee_unique_dates - lazada_unique_dates
        print(f"❌ Shopee has {difference:,} more unique order dates than Lazada")
    
    # Analyze date density (orders per unique date)
    print(f"\n=== ORDER DENSITY ===")
    if lazada_unique_dates > 0:
        lazada_density = len(lazada_orders) / lazada_unique_dates
        print(f"Lazada orders per unique date: {lazada_density:.2f}")
    
    if shopee_unique_dates > 0:
        shopee_density = len(shopee_orders) / shopee_unique_dates
        print(f"Shopee orders per unique date: {shopee_density:.2f}")
    
    # Analyze by year to see patterns
    print(f"\n=== ORDERS BY YEAR ===")
    lazada_orders['year'] = lazada_orders['order_date'].dt.year
    shopee_orders['year'] = shopee_orders['order_date'].dt.year
    
    lazada_by_year = lazada_orders.groupby('year').agg({
        'orders_key': 'count',
        'order_date': lambda x: x.dt.date.nunique()
    }).rename(columns={'orders_key': 'total_orders', 'order_date': 'unique_dates'})
    
    shopee_by_year = shopee_orders.groupby('year').agg({
        'orders_key': 'count',
        'order_date': lambda x: x.dt.date.nunique()
    }).rename(columns={'orders_key': 'total_orders', 'order_date': 'unique_dates'})
    
    print("\nLazada by year:")
    for year, row in lazada_by_year.iterrows():
        density = row['total_orders'] / row['unique_dates'] if row['unique_dates'] > 0 else 0
        print(f"  {year}: {row['total_orders']:,} orders, {row['unique_dates']:,} unique dates (avg {density:.1f} orders/day)")
    
    print("\nShopee by year:")
    for year, row in shopee_by_year.iterrows():
        density = row['total_orders'] / row['unique_dates'] if row['unique_dates'] > 0 else 0
        print(f"  {year}: {row['total_orders']:,} orders, {row['unique_dates']:,} unique dates (avg {density:.1f} orders/day)")
    
    # Analyze gaps in order dates
    print(f"\n=== ORDER DATE GAPS ANALYSIS ===")
    
    # Generate full date ranges
    if len(lazada_orders) > 0:
        lazada_date_range = pd.date_range(lazada_start, lazada_end, freq='D')
        lazada_actual_dates = set(lazada_orders['order_date'].dt.date)
        lazada_missing_dates = len(lazada_date_range) - len(lazada_actual_dates)
        lazada_coverage = len(lazada_actual_dates) / len(lazada_date_range) * 100
        print(f"Lazada date coverage: {len(lazada_actual_dates):,}/{len(lazada_date_range):,} days ({lazada_coverage:.1f}%)")
        print(f"Lazada missing dates: {lazada_missing_dates:,}")
    
    if len(shopee_orders) > 0:
        shopee_date_range = pd.date_range(shopee_start, shopee_end, freq='D')
        shopee_actual_dates = set(shopee_orders['order_date'].dt.date)
        shopee_missing_dates = len(shopee_date_range) - len(shopee_actual_dates)
        shopee_coverage = len(shopee_actual_dates) / len(shopee_date_range) * 100
        print(f"Shopee date coverage: {len(shopee_actual_dates):,}/{len(shopee_date_range):,} days ({shopee_coverage:.1f}%)")
        print(f"Shopee missing dates: {shopee_missing_dates:,}")
    
    # Analyze order frequency patterns
    print(f"\n=== ORDER FREQUENCY PATTERNS ===")
    
    # Group by date to see daily order counts
    lazada_daily = lazada_orders.groupby(lazada_orders['order_date'].dt.date).size()
    shopee_daily = shopee_orders.groupby(shopee_orders['order_date'].dt.date).size()
    
    print(f"Lazada daily order statistics:")
    print(f"  Days with orders: {len(lazada_daily):,}")
    print(f"  Min orders per day: {lazada_daily.min()}")
    print(f"  Max orders per day: {lazada_daily.max()}")
    print(f"  Average orders per day: {lazada_daily.mean():.1f}")
    print(f"  Median orders per day: {lazada_daily.median():.1f}")
    
    print(f"\nShopee daily order statistics:")
    print(f"  Days with orders: {len(shopee_daily):,}")
    print(f"  Min orders per day: {shopee_daily.min()}")
    print(f"  Max orders per day: {shopee_daily.max()}")
    print(f"  Average orders per day: {shopee_daily.mean():.1f}")
    print(f"  Median orders per day: {shopee_daily.median():.1f}")
    
    # Check for data quality issues
    print(f"\n=== DATA QUALITY CHECKS ===")
    
    # Check for weekend patterns
    lazada_orders['day_of_week'] = lazada_orders['order_date'].dt.day_name()
    shopee_orders['day_of_week'] = shopee_orders['order_date'].dt.day_name()
    
    print("Lazada orders by day of week:")
    lazada_dow = lazada_orders['day_of_week'].value_counts()
    for day, count in lazada_dow.items():
        print(f"  {day}: {count:,} orders")
    
    print("\nShopee orders by day of week:")
    shopee_dow = shopee_orders['day_of_week'].value_counts()
    for day, count in shopee_dow.items():
        print(f"  {day}: {count:,} orders")
    
    # Check for unusual date patterns
    print(f"\n=== POTENTIAL ISSUES ===")
    
    # Check if one platform has more consistent daily ordering
    lazada_consistent_days = (lazada_daily == 1).sum()
    shopee_consistent_days = (shopee_daily == 1).sum()
    
    print(f"Days with exactly 1 order:")
    print(f"  Lazada: {lazada_consistent_days:,} days")
    print(f"  Shopee: {shopee_consistent_days:,} days")
    
    # Check for platform-specific data collection periods
    if len(lazada_orders) > 0 and len(shopee_orders) > 0:
        print(f"\n=== TIMELINE COMPARISON ===")
        print(f"Who started first: {'Shopee' if shopee_start < lazada_start else 'Lazada'}")
        print(f"Who ended last: {'Shopee' if shopee_end > lazada_end else 'Lazada'}")
        
        # Check for overlapping periods
        overlap_start = max(lazada_start, shopee_start)
        overlap_end = min(lazada_end, shopee_end)
        
        if overlap_start <= overlap_end:
            overlap_days = (overlap_end - overlap_start).days
            print(f"Overlapping period: {overlap_start.date()} to {overlap_end.date()} ({overlap_days:,} days)")
            
            # Analyze overlapping period
            lazada_overlap = lazada_orders[
                (lazada_orders['order_date'] >= overlap_start) & 
                (lazada_orders['order_date'] <= overlap_end)
            ]
            shopee_overlap = shopee_orders[
                (shopee_orders['order_date'] >= overlap_start) & 
                (shopee_orders['order_date'] <= overlap_end)
            ]
            
            lazada_overlap_dates = lazada_overlap['order_date'].dt.date.nunique()
            shopee_overlap_dates = shopee_overlap['order_date'].dt.date.nunique()
            
            print(f"During overlap period:")
            print(f"  Lazada unique dates: {lazada_overlap_dates:,}")
            print(f"  Shopee unique dates: {shopee_overlap_dates:,}")
            print(f"  Lazada orders: {len(lazada_overlap):,}")
            print(f"  Shopee orders: {len(shopee_overlap):,}")

if __name__ == "__main__":
    analyze_order_date_patterns()