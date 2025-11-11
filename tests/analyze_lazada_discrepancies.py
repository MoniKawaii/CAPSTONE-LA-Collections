#!/usr/bin/env python3
"""
Lazada Data Discrepancy Analysis
Investigating the differences between raw data, dimensional model, and comparison file
"""

import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def analyze_lazada_discrepancies():
    """
    Comprehensive analysis of Lazada data discrepancies between:
    1. Raw JSON data (app/Staging/lazada_orders_raw.json)  
    2. Dimensional model (dim_order.csv, fact_orders.csv)
    3. Comparison file data (compare_lazada.csv)
    """
    
    print("=" * 80)
    print("🔍 LAZADA DATA DISCREPANCY ANALYSIS")
    print("=" * 80)
    
    # ================================================================
    # 1. Load and analyze raw Lazada orders data
    # ================================================================
    print("\n📁 1. ANALYZING RAW LAZADA DATA")
    print("-" * 50)
    
    try:
        with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
            raw_lazada = json.load(f)
        print(f"✅ Raw Lazada orders loaded: {len(raw_lazada):,} records")
        
        # Convert to DataFrame for analysis
        raw_df = pd.DataFrame(raw_lazada)
        raw_df['created_at'] = pd.to_datetime(raw_df['created_at'])
        raw_df['order_date'] = raw_df['created_at'].dt.date
        raw_df['order_month'] = raw_df['created_at'].dt.to_period('M')
        raw_df['price_numeric'] = pd.to_numeric(raw_df['price'], errors='coerce')
        
        print(f"📊 Date range: {raw_df['order_date'].min()} to {raw_df['order_date'].max()}")
        print(f"💰 Total orders: {len(raw_df):,}")
        print(f"💵 Total gross revenue: ₱{raw_df['price_numeric'].sum():,.2f}")
        
        # Monthly breakdown of raw data
        raw_monthly = raw_df.groupby('order_month').agg({
            'order_number': 'count',
            'price_numeric': 'sum'
        }).reset_index()
        raw_monthly['order_month_str'] = raw_monthly['order_month'].astype(str)
        
        print("\n📈 RAW DATA - Monthly Summary:")
        for _, row in raw_monthly.tail(10).iterrows():
            print(f"  {row['order_month_str']}: {row['order_number']:,} orders, ₱{row['price_numeric']:,.2f}")
            
    except Exception as e:
        print(f"❌ Error loading raw data: {e}")
        return
    
    # ================================================================
    # 2. Load and analyze dimensional model data  
    # ================================================================
    print(f"\n📁 2. ANALYZING DIMENSIONAL MODEL DATA")
    print("-" * 50)
    
    try:
        # Load dimensional data
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
        
        print(f"✅ dim_order loaded: {len(dim_order):,} records")
        print(f"✅ fact_orders loaded: {len(fact_orders):,} records")
        
        # Filter for Lazada only (assuming platform_key 1 = Lazada)
        # Let's check platform distribution first
        platform_dist = dim_order['platform_key'].value_counts()
        print(f"\n🏢 Platform distribution:")
        for platform, count in platform_dist.items():
            print(f"  Platform {platform}: {count:,} orders")
        
        # Determine which platform is Lazada by checking order IDs
        # Lazada order IDs are typically numeric, Shopee are alphanumeric
        dim_order['is_numeric_id'] = dim_order['platform_order_id'].str.isdigit()
        platform_id_analysis = dim_order.groupby('platform_key')['is_numeric_id'].agg(['count', 'sum'])
        print(f"\n🔢 Order ID patterns (numeric vs alphanumeric):")
        for platform in platform_id_analysis.index:
            numeric_count = platform_id_analysis.loc[platform, 'sum']
            total_count = platform_id_analysis.loc[platform, 'count']
            numeric_pct = (numeric_count / total_count) * 100
            print(f"  Platform {platform}: {numeric_pct:.1f}% numeric IDs ({numeric_count:,}/{total_count:,})")
        
        # Assume platform with highest % of numeric IDs is Lazada
        lazada_platform_key = platform_id_analysis['sum'].div(platform_id_analysis['count']).idxmax()
        print(f"\n🎯 Identified Lazada as platform_key: {lazada_platform_key}")
        
        # Filter for Lazada orders
        lazada_dim = dim_order[dim_order['platform_key'] == lazada_platform_key].copy()
        lazada_dim['order_date'] = pd.to_datetime(lazada_dim['order_date'])
        lazada_dim['order_month'] = lazada_dim['order_date'].dt.to_period('M')
        
        print(f"📊 Lazada in dim_order: {len(lazada_dim):,} records")
        print(f"📅 Date range: {lazada_dim['order_date'].min().date()} to {lazada_dim['order_date'].max().date()}")
        print(f"💰 Total revenue: ₱{lazada_dim['price_total'].sum():,.2f}")
        
        # Monthly breakdown of dimensional data
        dim_monthly = lazada_dim.groupby('order_month').agg({
            'orders_key': 'count',
            'price_total': 'sum'
        }).reset_index()
        dim_monthly['order_month_str'] = dim_monthly['order_month'].astype(str)
        
        print("\n📈 DIMENSIONAL MODEL - Monthly Summary:")
        for _, row in dim_monthly.tail(10).iterrows():
            print(f"  {row['order_month_str']}: {row['orders_key']:,} orders, ₱{row['price_total']:,.2f}")
            
    except Exception as e:
        print(f"❌ Error loading dimensional data: {e}")
        return
    
    # ================================================================
    # 3. Load comparison file data
    # ================================================================ 
    print(f"\n📁 3. ANALYZING COMPARISON FILE DATA")
    print("-" * 50)
    
    try:
        compare_df = pd.read_csv('data/compare_lazada.csv')
        
        # Find the comparison section
        comparison_section = compare_df[compare_df.iloc[:, 0].str.contains('COMPARE LAZADA', na=False)].index
        if len(comparison_section) > 0:
            comparison_start = comparison_section[0] + 1
            comparison_data = compare_df.iloc[comparison_start:].dropna(subset=[compare_df.columns[0]])
            
            # Clean up the comparison data
            comparison_clean = comparison_data.iloc[:, [0, 1, 2, 3, 4, 5, 6]].copy()
            comparison_clean.columns = ['date', 'orders_db', 'orders_csv', 'missing_orders', 'gross_db', 'gross_csv', 'missing_gross']
            
            # Remove rows with NaN in key columns
            comparison_clean = comparison_clean.dropna(subset=['date', 'orders_db', 'orders_csv'])
            
            print(f"✅ Comparison data loaded: {len(comparison_clean):,} monthly records")
            print("\n📊 COMPARISON FILE - Sample data:")
            for _, row in comparison_clean.head(5).iterrows():
                print(f"  {row['date']}: DB={row['orders_db']}, CSV={row['orders_csv']}, Missing={row['missing_orders']}")
                
        else:
            print("❌ Could not find comparison section in compare_lazada.csv")
            return
            
    except Exception as e:
        print(f"❌ Error loading comparison data: {e}")
        return
    
    # ================================================================
    # 4. Cross-reference and identify discrepancies
    # ================================================================
    print(f"\n🔍 4. CROSS-REFERENCE ANALYSIS")
    print("-" * 50)
    
    # Convert comparison dates to match our period format
    comparison_clean['date_parsed'] = pd.to_datetime(comparison_clean['date'])
    comparison_clean['order_period'] = comparison_clean['date_parsed'].dt.to_period('M')
    
    # Merge with dimensional data
    merged_analysis = comparison_clean.merge(
        dim_monthly, 
        left_on='order_period', 
        right_on='order_month', 
        how='outer',
        suffixes=('_comparison', '_dim')
    )
    
    print("\n📋 DISCREPANCY SUMMARY:")
    print("=" * 80)
    print(f"{'Month':<12} {'DB Orders':<10} {'Dim Orders':<11} {'Diff':<8} {'DB Revenue':<15} {'Dim Revenue':<15} {'Revenue Diff':<15}")
    print("-" * 80)
    
    total_order_diff = 0
    total_revenue_diff = 0
    
    for _, row in merged_analysis.iterrows():
        if pd.notna(row['orders_db']) and pd.notna(row['orders_key']):
            month = str(row['order_period']) if pd.notna(row['order_period']) else 'N/A'
            db_orders = int(row['orders_db']) if pd.notna(row['orders_db']) else 0
            dim_orders = int(row['orders_key']) if pd.notna(row['orders_key']) else 0
            order_diff = db_orders - dim_orders
            
            # Parse revenue strings (remove currency symbols and commas)
            db_revenue_str = str(row['gross_db']).replace('?', '').replace(',', '') if pd.notna(row['gross_db']) else '0'
            dim_revenue = row['price_total'] if pd.notna(row['price_total']) else 0
            
            try:
                db_revenue = float(db_revenue_str)
            except:
                db_revenue = 0
                
            revenue_diff = db_revenue - dim_revenue
            
            print(f"{month:<12} {db_orders:<10} {dim_orders:<11} {order_diff:<8} {db_revenue:<15,.0f} {dim_revenue:<15,.0f} {revenue_diff:<15,.0f}")
            
            total_order_diff += order_diff
            total_revenue_diff += revenue_diff
    
    print("-" * 80)
    print(f"{'TOTALS':<12} {'':>10} {'':>11} {total_order_diff:<8} {'':>15} {'':>15} {total_revenue_diff:<15,.0f}")
    
    # ================================================================
    # 5. Detailed analysis of potential causes
    # ================================================================
    print(f"\n🔬 5. ROOT CAUSE ANALYSIS")
    print("-" * 50)
    
    print("\n📊 Potential Discrepancy Causes:")
    
    # 5.1 Order Status Analysis
    print("\n🔸 Order Status Distribution in Dimensional Model:")
    status_dist = lazada_dim['order_status'].value_counts()
    for status, count in status_dist.items():
        pct = (count / len(lazada_dim)) * 100
        print(f"  {status}: {count:,} orders ({pct:.1f}%)")
    
    # 5.2 Date Range Analysis
    print(f"\n🔸 Date Coverage Analysis:")
    raw_date_range = (raw_df['order_date'].min(), raw_df['order_date'].max())
    dim_date_range = (lazada_dim['order_date'].min().date(), lazada_dim['order_date'].max().date())
    
    print(f"  Raw data range: {raw_date_range[0]} to {raw_date_range[1]}")
    print(f"  Dim model range: {dim_date_range[0]} to {dim_date_range[1]}")
    
    if raw_date_range != dim_date_range:
        print("  ⚠️  DATE RANGE MISMATCH detected!")
    
    # 5.3 Order ID Mapping Analysis  
    print(f"\n🔸 Order ID Mapping Analysis:")
    
    # Check if order IDs from raw data match dimensional model
    raw_order_ids = set(raw_df['order_number'].astype(str))
    dim_order_ids = set(lazada_dim['platform_order_id'])
    
    common_orders = raw_order_ids.intersection(dim_order_ids)
    raw_only = raw_order_ids - dim_order_ids
    dim_only = dim_order_ids - raw_order_ids
    
    print(f"  Raw data orders: {len(raw_order_ids):,}")
    print(f"  Dimensional orders: {len(dim_order_ids):,}")
    print(f"  Common orders: {len(common_orders):,}")
    print(f"  Raw only: {len(raw_only):,}")
    print(f"  Dim only: {len(dim_only):,}")
    
    if len(raw_only) > 0:
        print(f"  ⚠️  {len(raw_only):,} orders in raw data NOT in dimensional model!")
    if len(dim_only) > 0:
        print(f"  ⚠️  {len(dim_only):,} orders in dimensional model NOT in raw data!")
    
    # ================================================================
    # 6. Transformation pipeline analysis
    # ================================================================
    print(f"\n🔄 6. TRANSFORMATION PIPELINE ANALYSIS")
    print("-" * 50)
    
    # Check for data in staging/transformation files
    staging_files = [
        'app/Transformation/lazada_orders_transformed.json',
        'app/Transformation/lazada_multiple_order_items_transformed.json'
    ]
    
    for file_path in staging_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                staging_data = json.load(f)
            print(f"✅ {file_path}: {len(staging_data):,} records")
        except FileNotFoundError:
            print(f"❌ {file_path}: File not found")
        except Exception as e:
            print(f"❌ {file_path}: Error - {e}")
    
    # ================================================================
    # 7. Recommendations
    # ================================================================
    print(f"\n💡 7. FINDINGS & RECOMMENDATIONS")
    print("=" * 80)
    
    print(f"\n🔍 KEY FINDINGS:")
    print(f"  • Total order discrepancy: {total_order_diff:,} orders")
    print(f"  • Total revenue discrepancy: ₱{total_revenue_diff:,.2f}")
    print(f"  • Raw data coverage: {len(raw_df):,} orders")
    print(f"  • Dimensional model coverage: {len(lazada_dim):,} orders")
    
    print(f"\n🎯 LIKELY CAUSES:")
    if len(raw_only) > 0:
        print(f"  • Missing orders in harmonization: {len(raw_only):,} orders not transformed")
    if len(dim_only) > 0:
        print(f"  • Extra orders in dimensional model: {len(dim_only):,} orders from unknown source")
    
    cancelled_orders = len(lazada_dim[lazada_dim['order_status'] == 'CANCELLED'])
    if cancelled_orders > 0:
        print(f"  • Cancelled orders included: {cancelled_orders:,} orders may affect totals")
    
    print(f"\n🔧 RECOMMENDATIONS:")
    print("  1. Verify order status filtering - exclude CANCELLED/FAILED orders")
    print("  2. Check transformation pipeline for missing order mapping")  
    print("  3. Validate date range consistency across all data sources")
    print("  4. Audit order ID mapping between raw and dimensional data")
    print("  5. Review aggregation logic in comparison reports")
    
    print(f"\n📊 Next Steps:")
    print("  • Run order-level reconciliation to identify specific missing records")
    print("  • Check transformation logs for any filtering/exclusion logic")
    print("  • Validate comparison file data sources and calculation methods")

if __name__ == "__main__":
    analyze_lazada_discrepancies()