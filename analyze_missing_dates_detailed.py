#!/usr/bin/env python3
"""
Detailed Missing Dates Analysis
Show exactly which dates are missing (no orders) for each platform
"""

import pandas as pd
from datetime import datetime, timedelta
import calendar

def analyze_missing_dates():
    print("=== DETAILED MISSING DATES ANALYSIS ===")
    
    # Load the dimensional data
    dim_order = pd.read_csv('app/Transformed/dim_order.csv')
    
    # Convert order_date to datetime
    dim_order['order_date'] = pd.to_datetime(dim_order['order_date'])
    
    # Separate by platform
    lazada_orders = dim_order[dim_order['platform_key'] == 1].copy()
    shopee_orders = dim_order[dim_order['platform_key'] == 2].copy()
    
    print(f"=== BASIC INFO ===")
    print(f"Lazada orders: {len(lazada_orders):,}")
    print(f"Shopee orders: {len(shopee_orders):,}")
    
    # Get date ranges
    lazada_start = lazada_orders['order_date'].min()
    lazada_end = lazada_orders['order_date'].max()
    shopee_start = shopee_orders['order_date'].min()
    shopee_end = shopee_orders['order_date'].max()
    
    print(f"\nLazada period: {lazada_start.date()} to {lazada_end.date()}")
    print(f"Shopee period: {shopee_start.date()} to {shopee_end.date()}")
    
    # Generate complete date ranges
    lazada_full_range = pd.date_range(lazada_start, lazada_end, freq='D')
    shopee_full_range = pd.date_range(shopee_start, shopee_end, freq='D')
    
    # Get actual order dates
    lazada_actual_dates = set(lazada_orders['order_date'].dt.date)
    shopee_actual_dates = set(shopee_orders['order_date'].dt.date)
    
    # Find missing dates
    lazada_missing = []
    for date in lazada_full_range:
        if date.date() not in lazada_actual_dates:
            lazada_missing.append(date.date())
    
    shopee_missing = []
    for date in shopee_full_range:
        if date.date() not in shopee_actual_dates:
            shopee_missing.append(date.date())
    
    print(f"\n=== MISSING DATES SUMMARY ===")
    print(f"Lazada missing dates: {len(lazada_missing):,} out of {len(lazada_full_range):,} total days")
    print(f"Shopee missing dates: {len(shopee_missing):,} out of {len(shopee_full_range):,} total days")
    
    # Show first 20 missing dates for each platform
    print(f"\n=== FIRST 20 LAZADA MISSING DATES ===")
    for i, date in enumerate(sorted(lazada_missing)[:20]):
        day_name = calendar.day_name[date.weekday()]
        print(f"{i+1:2d}. {date} ({day_name})")
    
    if len(lazada_missing) > 20:
        print(f"... and {len(lazada_missing) - 20:,} more missing dates")
    
    print(f"\n=== FIRST 20 SHOPEE MISSING DATES ===")
    for i, date in enumerate(sorted(shopee_missing)[:20]):
        day_name = calendar.day_name[date.weekday()]
        print(f"{i+1:2d}. {date} ({day_name})")
    
    if len(shopee_missing) > 20:
        print(f"... and {len(shopee_missing) - 20:,} more missing dates")
    
    # Analyze missing date patterns
    print(f"\n=== MISSING DATE PATTERNS ===")
    
    # Lazada missing dates by day of week
    if lazada_missing:
        lazada_missing_dow = {}
        for date in lazada_missing:
            dow = calendar.day_name[date.weekday()]
            lazada_missing_dow[dow] = lazada_missing_dow.get(dow, 0) + 1
        
        print("Lazada missing dates by day of week:")
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            count = lazada_missing_dow.get(day, 0)
            print(f"  {day}: {count} missing dates")
    
    # Shopee missing dates by day of week
    if shopee_missing:
        shopee_missing_dow = {}
        for date in shopee_missing:
            dow = calendar.day_name[date.weekday()]
            shopee_missing_dow[dow] = shopee_missing_dow.get(dow, 0) + 1
        
        print("\nShopee missing dates by day of week:")
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            count = shopee_missing_dow.get(day, 0)
            print(f"  {day}: {count} missing dates")
    
    # Analyze consecutive missing periods
    print(f"\n=== CONSECUTIVE MISSING PERIODS ===")
    
    def find_consecutive_periods(missing_dates):
        if not missing_dates:
            return []
        
        sorted_dates = sorted(missing_dates)
        periods = []
        current_start = sorted_dates[0]
        current_end = sorted_dates[0]
        
        for i in range(1, len(sorted_dates)):
            if sorted_dates[i] == current_end + timedelta(days=1):
                current_end = sorted_dates[i]
            else:
                periods.append((current_start, current_end, (current_end - current_start).days + 1))
                current_start = sorted_dates[i]
                current_end = sorted_dates[i]
        
        periods.append((current_start, current_end, (current_end - current_start).days + 1))
        return periods
    
    # Lazada consecutive missing periods
    lazada_periods = find_consecutive_periods(lazada_missing)
    if lazada_periods:
        # Sort by length of period (longest first)
        lazada_periods.sort(key=lambda x: x[2], reverse=True)
        print(f"Lazada - Top 10 longest consecutive missing periods:")
        for i, (start, end, days) in enumerate(lazada_periods[:10]):
            if days == 1:
                print(f"  {i+1:2d}. {start} (1 day)")
            else:
                print(f"  {i+1:2d}. {start} to {end} ({days} days)")
    
    # Shopee consecutive missing periods
    shopee_periods = find_consecutive_periods(shopee_missing)
    if shopee_periods:
        # Sort by length of period (longest first)
        shopee_periods.sort(key=lambda x: x[2], reverse=True)
        print(f"\nShopee - Top 10 longest consecutive missing periods:")
        for i, (start, end, days) in enumerate(shopee_periods[:10]):
            if days == 1:
                print(f"  {i+1:2d}. {start} (1 day)")
            else:
                print(f"  {i+1:2d}. {start} to {end} ({days} days)")
    
    # Analyze missing dates by year and month
    print(f"\n=== MISSING DATES BY YEAR ===")
    
    # Lazada missing by year
    if lazada_missing:
        lazada_missing_by_year = {}
        for date in lazada_missing:
            year = date.year
            lazada_missing_by_year[year] = lazada_missing_by_year.get(year, 0) + 1
        
        print("Lazada missing dates by year:")
        for year in sorted(lazada_missing_by_year.keys()):
            count = lazada_missing_by_year[year]
            print(f"  {year}: {count} missing dates")
    
    # Shopee missing by year
    if shopee_missing:
        shopee_missing_by_year = {}
        for date in shopee_missing:
            year = date.year
            shopee_missing_by_year[year] = shopee_missing_by_year.get(year, 0) + 1
        
        print("\nShopee missing dates by year:")
        for year in sorted(shopee_missing_by_year.keys()):
            count = shopee_missing_by_year[year]
            print(f"  {year}: {count} missing dates")
    
    # Show some recent missing dates
    print(f"\n=== RECENT MISSING DATES (Last 30 days of operation) ===")
    
    # Last 30 days of Lazada operation
    lazada_recent_start = lazada_end - timedelta(days=29)
    lazada_recent_missing = [d for d in lazada_missing if d >= lazada_recent_start.date()]
    
    print(f"Lazada missing dates in last 30 days ({lazada_recent_start.date()} to {lazada_end.date()}):")
    if lazada_recent_missing:
        for date in sorted(lazada_recent_missing):
            day_name = calendar.day_name[date.weekday()]
            print(f"  {date} ({day_name})")
    else:
        print("  No missing dates in last 30 days")
    
    # Last 30 days of Shopee operation
    shopee_recent_start = shopee_end - timedelta(days=29)
    shopee_recent_missing = [d for d in shopee_missing if d >= shopee_recent_start.date()]
    
    print(f"\nShopee missing dates in last 30 days ({shopee_recent_start.date()} to {shopee_end.date()}):")
    if shopee_recent_missing:
        for date in sorted(shopee_recent_missing):
            day_name = calendar.day_name[date.weekday()]
            print(f"  {date} ({day_name})")
    else:
        print("  No missing dates in last 30 days")

if __name__ == "__main__":
    analyze_missing_dates()