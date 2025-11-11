#!/usr/bin/env python3
"""
Focused Lazada Discrepancy Analysis
Analyzing the harmonization process and comparing with the comparison file
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def analyze_harmonization_discrepancies():
    """
    Focused analysis of Lazada data discrepancies in the harmonization process
    """
    
    print("=" * 80)
    print("🔍 LAZADA HARMONIZATION DISCREPANCY ANALYSIS")
    print("=" * 80)
    
    # ================================================================
    # 1. Load dimensional model data and identify Lazada
    # ================================================================
    print("\n📊 1. ANALYZING DIMENSIONAL MODEL")
    print("-" * 50)
    
    try:
        # Load dimensional data
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
        
        print(f"✅ dim_order: {len(dim_order):,} total orders")
        print(f"✅ fact_orders: {len(fact_orders):,} order items")
        
        # Identify platform keys
        platform_dist = dim_order['platform_key'].value_counts().sort_index()
        print(f"\n🏢 Platform distribution:")
        for platform, count in platform_dist.items():
            print(f"  Platform {platform}: {count:,} orders")
        
        # Check order ID patterns to identify Lazada
        sample_orders = dim_order.groupby('platform_key')['platform_order_id'].head(5)
        print(f"\n🔍 Sample order IDs by platform:")
        for platform in platform_dist.index:
            platform_samples = dim_order[dim_order['platform_key'] == platform]['platform_order_id'].head(3).tolist()
            print(f"  Platform {platform}: {platform_samples}")
        
        # Determine Lazada platform key (typically has longer numeric IDs)
        # Check average length and numeric patterns
        platform_analysis = []
        for platform in platform_dist.index:
            platform_orders = dim_order[dim_order['platform_key'] == platform]['platform_order_id']
            avg_length = platform_orders.str.len().mean()
            numeric_count = platform_orders.str.isdigit().sum()
            numeric_pct = (numeric_count / len(platform_orders)) * 100
            
            platform_analysis.append({
                'platform_key': platform,
                'count': len(platform_orders),
                'avg_id_length': avg_length,
                'numeric_pct': numeric_pct
            })
            
            print(f"  Platform {platform}: Avg ID length={avg_length:.1f}, Numeric={numeric_pct:.1f}%")
        
        # Lazada typically has longer numeric IDs, Shopee has mixed alphanumeric
        lazada_platform = max(platform_analysis, key=lambda x: x['avg_id_length'])['platform_key']
        print(f"\n🎯 Identified Lazada as platform_key: {lazada_platform}")
        
        # Filter for Lazada data
        lazada_orders = dim_order[dim_order['platform_key'] == lazada_platform].copy()
        lazada_orders['order_date'] = pd.to_datetime(lazada_orders['order_date'])
        lazada_orders['order_month'] = lazada_orders['order_date'].dt.to_period('M')
        
        print(f"\n📈 Lazada dimensional data:")
        print(f"  Total orders: {len(lazada_orders):,}")
        print(f"  Date range: {lazada_orders['order_date'].min().date()} to {lazada_orders['order_date'].max().date()}")
        print(f"  Total revenue: ₱{lazada_orders['price_total'].sum():,.2f}")
        
        # Status breakdown
        status_counts = lazada_orders['order_status'].value_counts()
        print(f"\n📋 Order status breakdown:")
        for status, count in status_counts.items():
            pct = (count / len(lazada_orders)) * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")
        
    except Exception as e:
        print(f"❌ Error loading dimensional data: {e}")
        return
    
    # ================================================================
    # 2. Load and clean comparison file data
    # ================================================================
    print(f"\n📊 2. ANALYZING COMPARISON FILE")
    print("-" * 50)
    
    try:
        # Load comparison file and extract the relevant section
        with open('data/compare_lazada.csv', 'r') as f:
            lines = f.readlines()
        
        # Find the comparison section
        comparison_start = None
        for i, line in enumerate(lines):
            if 'COMPARE LAZADA' in line:
                comparison_start = i + 2  # Skip header row
                break
        
        if comparison_start is None:
            print("❌ Could not find comparison section")
            return
        
        # Parse comparison data manually
        comparison_data = []
        for line in lines[comparison_start:]:
            parts = line.strip().split(',')
            if len(parts) >= 7 and parts[0] and parts[0] != '':
                try:
                    date = parts[0]
                    orders_db = int(parts[1]) if parts[1] else 0
                    orders_csv = int(parts[2]) if parts[2] else 0
                    missing_orders = int(parts[3]) if parts[3] else 0
                    
                    # Clean revenue data (remove currency symbols)
                    gross_db_clean = parts[4].replace('"', '').replace(',', '') if parts[4] else '0'
                    gross_csv_clean = parts[5].replace('"', '').replace('?', '').replace(',', '') if parts[5] else '0'
                    
                    try:
                        gross_db = float(gross_db_clean)
                        gross_csv = float(gross_csv_clean)
                    except:
                        gross_db = 0
                        gross_csv = 0
                    
                    comparison_data.append({
                        'date': date,
                        'orders_db': orders_db,
                        'orders_csv': orders_csv,
                        'missing_orders': missing_orders,
                        'gross_db': gross_db,
                        'gross_csv': gross_csv
                    })
                except:
                    continue
        
        comparison_df = pd.DataFrame(comparison_data)
        comparison_df['date_parsed'] = pd.to_datetime(comparison_df['date'])
        comparison_df['order_month'] = comparison_df['date_parsed'].dt.to_period('M')
        
        print(f"✅ Comparison data loaded: {len(comparison_df)} monthly records")
        print(f"📅 Date range: {comparison_df['date'].min()} to {comparison_df['date'].max()}")
        
        # Summary statistics
        total_missing_orders = comparison_df['missing_orders'].sum()
        total_db_orders = comparison_df['orders_db'].sum()
        total_csv_orders = comparison_df['orders_csv'].sum()
        
        print(f"\n📊 Comparison summary:")
        print(f"  Total DB orders: {total_db_orders:,}")
        print(f"  Total CSV orders: {total_csv_orders:,}")
        print(f"  Total missing orders: {total_missing_orders:,}")
        print(f"  Missing order rate: {(total_missing_orders/total_csv_orders)*100:.1f}%")
        
    except Exception as e:
        print(f"❌ Error processing comparison data: {e}")
        return
    
    # ================================================================
    # 3. Monthly cross-analysis
    # ================================================================
    print(f"\n🔍 3. MONTHLY CROSS-ANALYSIS")
    print("-" * 50)
    
    # Get monthly totals from dimensional data
    dim_monthly = lazada_orders.groupby('order_month').agg({
        'orders_key': 'count',
        'price_total': 'sum'
    }).reset_index()
    dim_monthly['month_str'] = dim_monthly['order_month'].astype(str)
    
    print(f"\n📈 DIMENSIONAL MODEL - Recent Monthly Data:")
    for _, row in dim_monthly.tail(10).iterrows():
        print(f"  {row['month_str']}: {row['orders_key']:,} orders, ₱{row['price_total']:,.2f}")
    
    print(f"\n📊 COMPARISON FILE - Recent Monthly Data:")
    for _, row in comparison_df.tail(10).iterrows():
        print(f"  {row['date']}: DB={row['orders_db']:,}, CSV={row['orders_csv']:,}, Missing={row['missing_orders']:,}")
    
    # ================================================================
    # 4. Detailed discrepancy matching
    # ================================================================
    print(f"\n🎯 4. DISCREPANCY ANALYSIS")
    print("=" * 80)
    
    # Merge the datasets for comparison
    merged = comparison_df.merge(
        dim_monthly, 
        left_on='order_month', 
        right_on='order_month', 
        how='left'
    )
    
    print(f"{'Month':<12} {'Comp DB':<10} {'Comp CSV':<11} {'Dim Model':<11} {'DB-Dim':<9} {'CSV-Dim':<10} {'Missing%':<10}")
    print("-" * 80)
    
    total_db_dim_diff = 0
    total_csv_dim_diff = 0
    
    for _, row in merged.iterrows():
        month = row['date'][:7] if row['date'] else 'N/A'
        comp_db = int(row['orders_db']) if pd.notna(row['orders_db']) else 0
        comp_csv = int(row['orders_csv']) if pd.notna(row['orders_csv']) else 0
        dim_orders = int(row['orders_key']) if pd.notna(row['orders_key']) else 0
        
        db_dim_diff = comp_db - dim_orders
        csv_dim_diff = comp_csv - dim_orders
        missing_pct = (row['missing_orders'] / comp_csv * 100) if comp_csv > 0 else 0
        
        print(f"{month:<12} {comp_db:<10} {comp_csv:<11} {dim_orders:<11} {db_dim_diff:<9} {csv_dim_diff:<10} {missing_pct:<10.1f}%")
        
        total_db_dim_diff += db_dim_diff
        total_csv_dim_diff += csv_dim_diff
    
    print("-" * 80)
    print(f"{'TOTALS':<12} {'':>10} {'':>11} {'':>11} {total_db_dim_diff:<9} {total_csv_dim_diff:<10}")
    
    # ================================================================
    # 5. Root cause analysis
    # ================================================================
    print(f"\n🔬 5. ROOT CAUSE ANALYSIS")
    print("-" * 50)
    
    # Check order status impact
    completed_orders = len(lazada_orders[lazada_orders['order_status'] == 'COMPLETED'])
    cancelled_orders = len(lazada_orders[lazada_orders['order_status'] == 'CANCELLED'])
    other_orders = len(lazada_orders[~lazada_orders['order_status'].isin(['COMPLETED', 'CANCELLED'])])
    
    print(f"\n🔸 Order Status Impact:")
    print(f"  COMPLETED: {completed_orders:,} ({completed_orders/len(lazada_orders)*100:.1f}%)")
    print(f"  CANCELLED: {cancelled_orders:,} ({cancelled_orders/len(lazada_orders)*100:.1f}%)")
    print(f"  OTHER: {other_orders:,} ({other_orders/len(lazada_orders)*100:.1f}%)")
    
    if cancelled_orders > 0:
        print(f"  ⚠️  CANCELLED orders may explain some discrepancies")
    
    # Date coverage analysis
    print(f"\n🔸 Date Coverage Analysis:")
    dim_date_range = (lazada_orders['order_date'].min(), lazada_orders['order_date'].max())
    comp_date_range = (comparison_df['date_parsed'].min(), comparison_df['date_parsed'].max())
    
    print(f"  Dimensional model: {dim_date_range[0].date()} to {dim_date_range[1].date()}")
    print(f"  Comparison file: {comp_date_range[0].date()} to {comp_date_range[1].date()}")
    
    # Check for missing months
    dim_months = set(lazada_orders['order_month'])
    comp_months = set(comparison_df['order_month'])
    
    missing_in_dim = comp_months - dim_months
    missing_in_comp = dim_months - comp_months
    
    if missing_in_dim:
        print(f"  ⚠️  Months in comparison but missing in dim model: {len(missing_in_dim)}")
    if missing_in_comp:
        print(f"  ⚠️  Months in dim model but missing in comparison: {len(missing_in_comp)}")
    
    # ================================================================
    # 6. Recommendations
    # ================================================================
    print(f"\n💡 6. FINDINGS & RECOMMENDATIONS")
    print("=" * 80)
    
    print(f"\n🎯 KEY FINDINGS:")
    print(f"  • Dimensional model has {len(lazada_orders):,} Lazada orders")
    print(f"  • Comparison shows {total_missing_orders:,} missing orders overall")
    print(f"  • {cancelled_orders:,} CANCELLED orders in dimensional model")
    print(f"  • {(cancelled_orders/len(lazada_orders)*100):.1f}% of orders are CANCELLED")
    
    print(f"\n🔍 LIKELY CAUSES:")
    print(f"  1. CANCELLED ORDERS: {cancelled_orders:,} cancelled orders may be excluded from comparison")
    print(f"  2. STATUS FILTERING: Different filtering criteria between systems")
    print(f"  3. DATE BOUNDARIES: Different date range interpretations")
    print(f"  4. AGGREGATION LOGIC: Different grouping methods")
    
    print(f"\n🔧 RECOMMENDED ACTIONS:")
    print("  1. ✅ Filter dimensional model to COMPLETED orders only")
    print("  2. ✅ Verify date range alignment between data sources")
    print("  3. ✅ Check if comparison file excludes cancelled orders")
    print("  4. ✅ Investigate transformation pipeline for order filtering")
    print("  5. ✅ Validate raw data sources and extraction dates")
    
    # ================================================================
    # 7. Generate filtered comparison
    # ================================================================
    print(f"\n📊 7. FILTERED ANALYSIS (COMPLETED ORDERS ONLY)")
    print("-" * 50)
    
    # Filter to completed orders only
    completed_lazada = lazada_orders[lazada_orders['order_status'] == 'COMPLETED'].copy()
    
    completed_monthly = completed_lazada.groupby('order_month').agg({
        'orders_key': 'count',
        'price_total': 'sum'
    }).reset_index()
    
    print(f"\n✅ COMPLETED ORDERS ONLY:")
    print(f"  Total completed orders: {len(completed_lazada):,}")
    print(f"  Total completed revenue: ₱{completed_lazada['price_total'].sum():,.2f}")
    print(f"  Reduction from cancelled: {cancelled_orders:,} orders")
    
    print(f"\n📋 REVISED COMPARISON (Completed vs Comparison DB):")
    revised_merge = comparison_df.merge(
        completed_monthly, 
        left_on='order_month', 
        right_on='order_month', 
        how='left'
    )
    
    print(f"{'Month':<12} {'Comp DB':<10} {'Completed':<11} {'Difference':<11} {'% Match':<10}")
    print("-" * 60)
    
    total_revised_diff = 0
    matches = 0
    
    for _, row in revised_merge.iterrows():
        month = row['date'][:7] if row['date'] else 'N/A'
        comp_db = int(row['orders_db']) if pd.notna(row['orders_db']) else 0
        completed = int(row['orders_key']) if pd.notna(row['orders_key']) else 0
        
        diff = comp_db - completed
        match_pct = (min(comp_db, completed) / max(comp_db, completed) * 100) if max(comp_db, completed) > 0 else 0
        
        if match_pct > 95:
            matches += 1
            
        print(f"{month:<12} {comp_db:<10} {completed:<11} {diff:<11} {match_pct:<10.1f}%")
        total_revised_diff += diff
    
    print("-" * 60)
    print(f"{'TOTALS':<12} {'':>10} {'':>11} {total_revised_diff:<11}")
    print(f"\n🎯 Months with >95% match: {matches}/{len(revised_merge)} ({matches/len(revised_merge)*100:.1f}%)")
    
    if abs(total_revised_diff) < abs(total_db_dim_diff):
        print(f"✅ FILTERING TO COMPLETED ORDERS IMPROVED ACCURACY!")
        print(f"   Original difference: {total_db_dim_diff:,} orders")
        print(f"   Revised difference: {total_revised_diff:,} orders") 
        print(f"   Improvement: {total_db_dim_diff - total_revised_diff:,} orders")

if __name__ == "__main__":
    analyze_harmonization_discrepancies()