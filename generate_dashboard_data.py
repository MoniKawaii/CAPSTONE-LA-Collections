#!/usr/bin/env python3
"""
Generate Dashboard-Ready Data Files
Creates CSV exports that can be imported into any dashboard platform
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_dashboard_data():
    print("=== GENERATING DASHBOARD DATA FILES ===")
    
    # Load the dimensional data
    dim_order = pd.read_csv('app/Transformed/dim_order.csv')
    
    # Convert order_date to datetime
    dim_order['order_date'] = pd.to_datetime(dim_order['order_date'])
    
    # Filter for COMPLETED orders only
    dim_order = dim_order[dim_order['order_status'] == 'COMPLETED'].copy()
    
    print(f"Processing {len(dim_order):,} completed orders...")
    
    # ================================================================
    # 1. KPI Summary Data
    # ================================================================
    
    kpi_data = []
    for platform_key in [1, 2]:
        platform_name = 'Lazada' if platform_key == 1 else 'Shopee'
        platform_orders = dim_order[dim_order['platform_key'] == platform_key]
        
        if len(platform_orders) > 0:
            start_date = platform_orders['order_date'].min()
            end_date = platform_orders['order_date'].max()
            total_days = (end_date - start_date).days + 1
            unique_dates = platform_orders['order_date'].dt.date.nunique()
            
            kpi_data.append({
                'platform_name': platform_name,
                'platform_key': platform_key,
                'total_orders': len(platform_orders),
                'unique_order_dates': unique_dates,
                'first_order_date': start_date.date(),
                'latest_order_date': end_date.date(),
                'total_possible_days': total_days,
                'date_coverage_percentage': round(unique_dates / total_days * 100, 2),
                'avg_orders_per_day': round(len(platform_orders) / unique_dates, 2),
                'total_revenue': platform_orders['price_total'].sum(),
                'avg_order_value': round(platform_orders['price_total'].mean(), 2)
            })
    
    kpi_df = pd.DataFrame(kpi_data)
    kpi_df.to_csv('app/Transformed/dashboard_kpi_summary.csv', index=False)
    print("✅ Created: dashboard_kpi_summary.csv")
    
    # ================================================================
    # 2. Daily Time Series Data
    # ================================================================
    
    daily_data = []
    for platform_key in [1, 2]:
        platform_name = 'Lazada' if platform_key == 1 else 'Shopee'
        platform_orders = dim_order[dim_order['platform_key'] == platform_key]
        
        if len(platform_orders) > 0:
            # Create full date range
            start_date = platform_orders['order_date'].min()
            end_date = platform_orders['order_date'].max()
            date_range = pd.date_range(start_date, end_date, freq='D')
            
            # Aggregate daily orders
            daily_orders = platform_orders.groupby(platform_orders['order_date'].dt.date).agg({
                'orders_key': 'count',
                'price_total': 'sum',
                'total_item_count': 'sum'
            }).rename(columns={
                'orders_key': 'daily_orders',
                'price_total': 'daily_revenue',
                'total_item_count': 'total_items'
            })
            
            # Fill in missing dates
            for date in date_range:
                date_only = date.date()
                if date_only in daily_orders.index:
                    row = daily_orders.loc[date_only]
                    daily_data.append({
                        'platform_name': platform_name,
                        'platform_key': platform_key,
                        'order_date': date_only,
                        'daily_orders': row['daily_orders'],
                        'daily_revenue': row['daily_revenue'],
                        'total_items': row['total_items'],
                        'is_missing_date': 0,
                        'day_of_week': date.dayofweek,
                        'day_name': date.strftime('%A'),
                        'month': date.month,
                        'year': date.year,
                        'quarter': f"Q{(date.month-1)//3 + 1}"
                    })
                else:
                    daily_data.append({
                        'platform_name': platform_name,
                        'platform_key': platform_key,
                        'order_date': date_only,
                        'daily_orders': 0,
                        'daily_revenue': 0,
                        'total_items': 0,
                        'is_missing_date': 1,
                        'day_of_week': date.dayofweek,
                        'day_name': date.strftime('%A'),
                        'month': date.month,
                        'year': date.year,
                        'quarter': f"Q{(date.month-1)//3 + 1}"
                    })
    
    daily_df = pd.DataFrame(daily_data)
    daily_df.to_csv('app/Transformed/dashboard_daily_trends.csv', index=False)
    print("✅ Created: dashboard_daily_trends.csv")
    
    # ================================================================
    # 3. Monthly Aggregations
    # ================================================================
    
    monthly_data = []
    for platform_key in [1, 2]:
        platform_name = 'Lazada' if platform_key == 1 else 'Shopee'
        platform_orders = dim_order[dim_order['platform_key'] == platform_key]
        
        if len(platform_orders) > 0:
            platform_orders['month_year'] = platform_orders['order_date'].dt.to_period('M')
            
            monthly_agg = platform_orders.groupby('month_year').agg({
                'orders_key': 'count',
                'order_date': lambda x: x.dt.date.nunique(),
                'price_total': 'sum',
                'total_item_count': 'sum'
            }).rename(columns={
                'orders_key': 'total_orders',
                'order_date': 'unique_dates',
                'price_total': 'total_revenue',
                'total_item_count': 'total_items'
            })
            
            for month_year, row in monthly_agg.iterrows():
                # Calculate days in month
                year = month_year.year
                month = month_year.month
                days_in_month = pd.Timestamp(year, month, 1).days_in_month
                
                monthly_data.append({
                    'platform_name': platform_name,
                    'platform_key': platform_key,
                    'month_year': str(month_year),
                    'year': year,
                    'month': month,
                    'total_orders': row['total_orders'],
                    'unique_dates': row['unique_dates'],
                    'total_revenue': row['total_revenue'],
                    'total_items': row['total_items'],
                    'days_in_month': days_in_month,
                    'date_coverage_pct': round(row['unique_dates'] / days_in_month * 100, 1),
                    'avg_orders_per_day': round(row['total_orders'] / row['unique_dates'], 1) if row['unique_dates'] > 0 else 0,
                    'avg_order_value': round(row['total_revenue'] / row['total_orders'], 2) if row['total_orders'] > 0 else 0
                })
    
    monthly_df = pd.DataFrame(monthly_data)
    monthly_df.to_csv('app/Transformed/dashboard_monthly_summary.csv', index=False)
    print("✅ Created: dashboard_monthly_summary.csv")
    
    # ================================================================
    # 4. Day of Week Analysis
    # ================================================================
    
    dow_data = []
    for platform_key in [1, 2]:
        platform_name = 'Lazada' if platform_key == 1 else 'Shopee'
        platform_orders = dim_order[dim_order['platform_key'] == platform_key]
        
        if len(platform_orders) > 0:
            platform_orders['day_of_week'] = platform_orders['order_date'].dt.dayofweek
            platform_orders['day_name'] = platform_orders['order_date'].dt.strftime('%A')
            
            dow_agg = platform_orders.groupby(['day_of_week', 'day_name']).agg({
                'orders_key': 'count',
                'order_date': lambda x: x.dt.date.nunique(),
                'price_total': ['sum', 'mean'],
                'total_item_count': 'sum'
            }).round(2)
            
            dow_agg.columns = ['total_orders', 'unique_dates', 'total_revenue', 'avg_order_value', 'total_items']
            
            for (dow, day_name), row in dow_agg.iterrows():
                dow_data.append({
                    'platform_name': platform_name,
                    'platform_key': platform_key,
                    'day_of_week': dow,
                    'day_name': day_name,
                    'total_orders': row['total_orders'],
                    'unique_dates': row['unique_dates'],
                    'total_revenue': row['total_revenue'],
                    'avg_order_value': row['avg_order_value'],
                    'total_items': row['total_items'],
                    'avg_orders_per_occurrence': round(row['total_orders'] / row['unique_dates'], 1) if row['unique_dates'] > 0 else 0
                })
    
    dow_df = pd.DataFrame(dow_data)
    dow_df.to_csv('app/Transformed/dashboard_day_of_week.csv', index=False)
    print("✅ Created: dashboard_day_of_week.csv")
    
    # ================================================================
    # 5. Missing Dates Analysis
    # ================================================================
    
    missing_periods = []
    for platform_key in [1, 2]:
        platform_name = 'Lazada' if platform_key == 1 else 'Shopee'
        platform_orders = dim_order[dim_order['platform_key'] == platform_key]
        
        if len(platform_orders) > 0:
            start_date = platform_orders['order_date'].min()
            end_date = platform_orders['order_date'].max()
            date_range = pd.date_range(start_date, end_date, freq='D')
            actual_dates = set(platform_orders['order_date'].dt.date)
            
            # Find missing dates
            missing_dates = []
            for date in date_range:
                if date.date() not in actual_dates:
                    missing_dates.append(date.date())
            
            # Group consecutive missing dates
            if missing_dates:
                missing_dates.sort()
                current_start = missing_dates[0]
                current_end = missing_dates[0]
                
                for i in range(1, len(missing_dates)):
                    if missing_dates[i] == current_end + timedelta(days=1):
                        current_end = missing_dates[i]
                    else:
                        # End of current period
                        period_length = (current_end - current_start).days + 1
                        missing_periods.append({
                            'platform_name': platform_name,
                            'platform_key': platform_key,
                            'period_start': current_start,
                            'period_end': current_end,
                            'days_missing': period_length,
                            'gap_category': 'Single Day' if period_length == 1 else 
                                          'Week or Less' if period_length <= 7 else
                                          'Month or Less' if period_length <= 30 else
                                          'More than Month'
                        })
                        current_start = missing_dates[i]
                        current_end = missing_dates[i]
                
                # Don't forget the last period
                period_length = (current_end - current_start).days + 1
                missing_periods.append({
                    'platform_name': platform_name,
                    'platform_key': platform_key,
                    'period_start': current_start,
                    'period_end': current_end,
                    'days_missing': period_length,
                    'gap_category': 'Single Day' if period_length == 1 else 
                                  'Week or Less' if period_length <= 7 else
                                  'Month or Less' if period_length <= 30 else
                                  'More than Month'
                })
    
    if missing_periods:
        missing_df = pd.DataFrame(missing_periods)
        missing_df = missing_df.sort_values('days_missing', ascending=False)
        missing_df.to_csv('app/Transformed/dashboard_missing_periods.csv', index=False)
        print("✅ Created: dashboard_missing_periods.csv")
    
    # ================================================================
    # 6. Recent Activity (Last 30 Days)
    # ================================================================
    
    cutoff_date = datetime.now().date() - timedelta(days=30)
    recent_orders = dim_order[dim_order['order_date'].dt.date >= cutoff_date].copy()
    
    if len(recent_orders) > 0:
        today = pd.Timestamp.now().normalize()
        recent_orders['days_ago'] = (today - recent_orders['order_date']).dt.days
        recent_orders['time_category'] = recent_orders['days_ago'].apply(
            lambda x: 'Today' if x == 0 else
                     'Yesterday' if x == 1 else
                     'This Week' if x <= 7 else
                     'Earlier'
        )
        
        recent_agg = recent_orders.groupby(['platform_key', 'order_date', 'time_category']).agg({
            'orders_key': 'count',
            'price_total': 'sum',
            'total_item_count': 'sum'
        }).rename(columns={
            'orders_key': 'total_orders',
            'price_total': 'total_revenue',
            'total_item_count': 'total_items'
        }).reset_index()
        
        recent_agg['platform_name'] = recent_agg['platform_key'].apply(lambda x: 'Lazada' if x == 1 else 'Shopee')
        recent_agg['order_date'] = recent_agg['order_date'].dt.date
        
        recent_agg.to_csv('app/Transformed/dashboard_recent_activity.csv', index=False)
        print("✅ Created: dashboard_recent_activity.csv")
    
    print(f"\n=== SUMMARY ===")
    print(f"Generated {len([f for f in ['kpi_summary', 'daily_trends', 'monthly_summary', 'day_of_week', 'missing_periods', 'recent_activity']])} dashboard data files")
    print(f"Files saved in: app/Transformed/")
    print(f"\n=== QUICK STATS ===")
    print(f"Total records in daily trends: {len(daily_df):,}")
    print(f"Date range covered: {daily_df['order_date'].min()} to {daily_df['order_date'].max()}")
    print(f"Platforms: {', '.join(daily_df['platform_name'].unique())}")

if __name__ == "__main__":
    generate_dashboard_data()