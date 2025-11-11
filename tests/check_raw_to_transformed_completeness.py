#!/usr/bin/env python3
"""
Lazada Raw Data to Transformed Completeness Check
Traces order numbers from raw JSON through to final transformed CSV
to identify any losses during the harmonization process
"""

import pandas as pd
import json
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def check_lazada_order_completeness():
    """
    Comprehensive check of Lazada order number completeness through the data pipeline:
    Raw JSON → Staging → Transformation → Final CSV
    """
    
    print("=" * 80)
    print("🔍 LAZADA RAW TO TRANSFORMED COMPLETENESS CHECK")
    print("=" * 80)
    
    # ================================================================
    # 1. Load and analyze raw Lazada orders
    # ================================================================
    print("\n📁 1. ANALYZING RAW LAZADA DATA")
    print("-" * 50)
    
    try:
        # Load raw Lazada orders
        print("Loading raw Lazada orders JSON...")
        with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
            raw_orders = json.load(f)
        
        print(f"✅ Raw orders loaded: {len(raw_orders):,} records")
        
        # Extract order numbers and key info
        raw_order_df = pd.DataFrame(raw_orders)
        raw_order_df['order_number'] = raw_order_df['order_number'].astype(str)
        raw_order_df['created_at'] = pd.to_datetime(raw_order_df['created_at'])
        raw_order_df['order_date'] = raw_order_df['created_at'].dt.date
        raw_order_df['order_month'] = raw_order_df['created_at'].dt.to_period('M')
        
        # Get unique order numbers
        raw_unique_orders = set(raw_order_df['order_number'].unique())
        
        print(f"📊 Raw data summary:")
        print(f"  Total records: {len(raw_order_df):,}")
        print(f"  Unique order numbers: {len(raw_unique_orders):,}")
        print(f"  Date range: {raw_order_df['order_date'].min()} to {raw_order_df['order_date'].max()}")
        
        # Status breakdown - handle statuses field (array)
        raw_order_df['status'] = raw_order_df['statuses'].apply(lambda x: x[0] if x and len(x) > 0 else 'unknown')
        status_counts = raw_order_df['status'].value_counts()
        print(f"\n📋 Order status distribution:")
        for status, count in status_counts.head(10).items():
            pct = (count / len(raw_order_df)) * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")
        
        # Sample order numbers
        sample_orders = raw_order_df['order_number'].head(10).tolist()
        print(f"\n📝 Sample order numbers: {sample_orders[:5]}...")
        
    except Exception as e:
        print(f"❌ Error loading raw data: {e}")
        return
    
    # ================================================================
    # 2. Check raw order items data
    # ================================================================
    print(f"\n📁 2. ANALYZING RAW ORDER ITEMS")
    print("-" * 50)
    
    try:
        # Load raw Lazada order items
        print("Loading raw Lazada order items JSON...")
        with open('app/Staging/lazada_multiple_order_items_raw.json', 'r', encoding='utf-8') as f:
            raw_items = json.load(f)
        
        print(f"✅ Raw order items loaded: {len(raw_items):,} records")
        
        # Extract order numbers from items
        items_df = pd.DataFrame(raw_items)
        
        # Handle different possible structures
        if 'order_id' in items_df.columns:
            items_df['order_number'] = items_df['order_id'].astype(str)
        elif 'order_number' in items_df.columns:
            items_df['order_number'] = items_df['order_number'].astype(str)
        else:
            print("⚠️  Could not find order number column in items data")
            print(f"Available columns: {items_df.columns.tolist()}")
            
        if 'order_number' in items_df.columns:
            items_unique_orders = set(items_df['order_number'].unique())
            
            print(f"📊 Order items summary:")
            print(f"  Total item records: {len(items_df):,}")
            print(f"  Unique orders in items: {len(items_unique_orders):,}")
            
            # Check overlap between orders and items
            orders_with_items = raw_unique_orders.intersection(items_unique_orders)
            orders_without_items = raw_unique_orders - items_unique_orders
            items_without_orders = items_unique_orders - raw_unique_orders
            
            print(f"\n🔗 Orders vs Items relationship:")
            print(f"  Orders with items: {len(orders_with_items):,}")
            print(f"  Orders without items: {len(orders_without_items):,}")
            print(f"  Items without matching orders: {len(items_without_orders):,}")
            
            if len(orders_without_items) > 0:
                print(f"  ⚠️  {len(orders_without_items):,} orders have no items!")
                # Sample orders without items
                sample_no_items = list(orders_without_items)[:5]
                print(f"  Sample orders without items: {sample_no_items}")
                
                # Check status of orders without items
                no_items_orders = raw_order_df[raw_order_df['order_number'].isin(orders_without_items)]
                no_items_status = no_items_orders['status'].value_counts()
                print(f"  Status of orders without items:")
                for status, count in no_items_status.head(5).items():
                    pct = (count / len(no_items_orders)) * 100
                    print(f"    {status}: {count:,} ({pct:.1f}%)")
                    
            if len(items_without_orders) > 0:
                print(f"  ⚠️  {len(items_without_orders):,} items have no matching orders!")
        
    except Exception as e:
        print(f"❌ Error loading items data: {e}")
        items_unique_orders = set()
    
    # ================================================================
    # 3. Load dimensional model data
    # ================================================================
    print(f"\n📁 3. ANALYZING DIMENSIONAL MODEL")
    print("-" * 50)
    
    try:
        # Load dim_order
        dim_order = pd.read_csv('app/Transformed/dim_order.csv')
        lazada_dim = dim_order[dim_order['platform_key'] == 1].copy()
        
        # Get Lazada order IDs from dimensional model
        dim_order_ids = set(lazada_dim['platform_order_id'].astype(str))
        
        print(f"✅ Dimensional model loaded")
        print(f"📊 Dimensional model summary:")
        print(f"  Total Lazada orders in dim_order: {len(lazada_dim):,}")
        print(f"  Unique platform_order_ids: {len(dim_order_ids):,}")
        
        # Date range in dimensional model
        lazada_dim['order_date'] = pd.to_datetime(lazada_dim['order_date'])
        print(f"  Date range: {lazada_dim['order_date'].min().date()} to {lazada_dim['order_date'].max().date()}")
        
        # Status breakdown
        dim_status_counts = lazada_dim['order_status'].value_counts()
        print(f"\n📋 Dimensional model status distribution:")
        for status, count in dim_status_counts.items():
            pct = (count / len(lazada_dim)) * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")
        
    except Exception as e:
        print(f"❌ Error loading dimensional model: {e}")
        return
    
    # ================================================================
    # 4. Load fact_orders data
    # ================================================================
    print(f"\n📁 4. ANALYZING FACT ORDERS")
    print("-" * 50)
    
    try:
        # Load fact_orders
        fact_orders = pd.read_csv('app/Transformed/fact_orders.csv')
        lazada_fact = fact_orders[fact_orders['platform_key'] == 1].copy()
        
        # Get unique orders from fact table
        fact_order_keys = set(lazada_fact['orders_key'].astype(str))
        
        print(f"✅ Fact orders loaded")
        print(f"📊 Fact orders summary:")
        print(f"  Total Lazada order items: {len(lazada_fact):,}")
        print(f"  Unique orders in fact_orders: {len(fact_order_keys):,}")
        print(f"  Average items per order: {len(lazada_fact)/len(fact_order_keys) if len(fact_order_keys) > 0 else 0:.2f}")
        
    except Exception as e:
        print(f"❌ Error loading fact orders: {e}")
        return
    
    # ================================================================
    # 5. Cross-reference analysis
    # ================================================================
    print(f"\n🔍 5. CROSS-REFERENCE ANALYSIS")
    print("=" * 80)
    
    # Compare raw orders with dimensional model
    print(f"\n📊 RAW vs DIMENSIONAL MODEL:")
    raw_in_dim = raw_unique_orders.intersection(dim_order_ids)
    raw_not_in_dim = raw_unique_orders - dim_order_ids
    dim_not_in_raw = dim_order_ids - raw_unique_orders
    
    print(f"  Raw orders: {len(raw_unique_orders):,}")
    print(f"  Dimensional orders: {len(dim_order_ids):,}")
    print(f"  Common orders: {len(raw_in_dim):,}")
    print(f"  Raw only: {len(raw_not_in_dim):,}")
    print(f"  Dimensional only: {len(dim_not_in_raw):,}")
    
    completion_rate = (len(raw_in_dim) / len(raw_unique_orders)) * 100 if len(raw_unique_orders) > 0 else 0
    print(f"  ✅ Completion rate: {completion_rate:.2f}%")
    
    if len(raw_not_in_dim) > 0:
        print(f"\n⚠️  ORDERS LOST IN HARMONIZATION:")
        print(f"  {len(raw_not_in_dim):,} orders from raw data not found in dimensional model")
        
        # Analyze the missing orders
        missing_orders_df = raw_order_df[raw_order_df['order_number'].isin(raw_not_in_dim)]
        missing_by_status = missing_orders_df['status'].value_counts()
        
        print(f"\n📋 Missing orders by status:")
        for status, count in missing_by_status.head(10).items():
            pct = (count / len(missing_orders_df)) * 100
            print(f"    {status}: {count:,} ({pct:.1f}%)")
        
        # Missing orders by date
        missing_by_month = missing_orders_df['order_month'].value_counts().sort_index()
        print(f"\n📅 Missing orders by month (last 12):")
        for month, count in missing_by_month.tail(12).items():
            print(f"    {month}: {count:,} missing orders")
            
        # Check if missing orders have items
        if 'order_number' in items_df.columns:
            missing_with_items = raw_not_in_dim.intersection(items_unique_orders)
            missing_without_items = raw_not_in_dim - items_unique_orders
            
            print(f"\n📦 Missing orders item status:")
            print(f"  Missing orders with items: {len(missing_with_items):,}")
            print(f"  Missing orders without items: {len(missing_without_items):,}")
            
            if len(missing_without_items) > 0:
                print(f"  ⚠️  {len(missing_without_items):,} missing orders have no items in raw data!")
    
    if len(dim_not_in_raw) > 0:
        print(f"\n⚠️  EXTRA ORDERS IN DIMENSIONAL MODEL:")
        print(f"  {len(dim_not_in_raw):,} orders in dimensional model not found in raw data")
        
        # Sample of extra order IDs
        sample_extra = list(dim_not_in_raw)[:10]
        print(f"  Sample extra order IDs: {sample_extra[:5]}...")
    
    # ================================================================
    # 6. Monthly completeness analysis
    # ================================================================
    print(f"\n📊 6. MONTHLY COMPLETENESS ANALYSIS")
    print("-" * 50)
    
    try:
        # Get monthly counts from raw data
        raw_monthly = raw_order_df.groupby('order_month').agg({
            'order_number': 'count',
            'status': lambda x: (x == 'delivered').sum()  # Count delivered orders
        }).rename(columns={'order_number': 'raw_total', 'status': 'raw_delivered'})
        
        # Get monthly counts from dimensional data
        lazada_dim['order_month'] = lazada_dim['order_date'].dt.to_period('M')
        dim_monthly = lazada_dim.groupby('order_month').agg({
            'orders_key': 'count',
            'order_status': lambda x: (x == 'COMPLETED').sum()
        }).rename(columns={'orders_key': 'dim_total', 'order_status': 'dim_completed'})
        
        # Merge for comparison
        monthly_comparison = raw_monthly.merge(dim_monthly, left_index=True, right_index=True, how='outer').fillna(0)
        monthly_comparison['completion_rate'] = np.where(
            monthly_comparison['raw_total'] > 0,
            (monthly_comparison['dim_total'] / monthly_comparison['raw_total'] * 100).round(2),
            0
        )
        monthly_comparison['missing_count'] = monthly_comparison['raw_total'] - monthly_comparison['dim_total']
        
        print(f"\n📈 MONTHLY COMPLETION RATES:")
        print(f"{'Month':<12} {'Raw':<8} {'Dim':<8} {'Missing':<9} {'Rate':<8} {'Raw Del':<9} {'Dim Comp':<9}")
        print("-" * 75)
        
        total_raw = 0
        total_dim = 0
        total_missing = 0
        
        for month, row in monthly_comparison.tail(12).iterrows():
            raw_count = int(row['raw_total'])
            dim_count = int(row['dim_total'])
            missing = int(row['missing_count'])
            rate = row['completion_rate']
            raw_delivered = int(row['raw_delivered'])
            dim_completed = int(row['dim_completed'])
            
            total_raw += raw_count
            total_dim += dim_count
            total_missing += missing
            
            rate_str = f"{rate:.1f}%" if rate < 999 else "N/A"
            
            print(f"{str(month):<12} {raw_count:<8} {dim_count:<8} {missing:<9} {rate_str:<8} {raw_delivered:<9} {dim_completed:<9}")
        
        print("-" * 75)
        overall_rate = (total_dim / total_raw * 100) if total_raw > 0 else 0
        print(f"{'TOTAL':<12} {total_raw:<8} {total_dim:<8} {total_missing:<9} {overall_rate:.1f}%")
        
    except Exception as e:
        print(f"❌ Error in monthly analysis: {e}")
    
    # ================================================================
    # 7. Sample missing orders investigation
    # ================================================================
    print(f"\n🔍 7. SAMPLE MISSING ORDERS INVESTIGATION")
    print("-" * 50)
    
    if len(raw_not_in_dim) > 0:
        # Take a sample of missing orders for detailed investigation
        sample_missing = list(raw_not_in_dim)[:20]
        
        print(f"📋 Investigating {len(sample_missing)} sample missing orders:")
        
        for order_id in sample_missing[:10]:  # Show first 10
            order_details = raw_order_df[raw_order_df['order_number'] == order_id].iloc[0]
            
            print(f"  Order {order_id}:")
            print(f"    Status: {order_details['status']}")
            print(f"    Date: {order_details['order_date']}")
            print(f"    Price: {order_details.get('price', 'N/A')}")
            
            # Check if it has items
            if 'order_number' in items_df.columns:
                has_items = order_id in items_unique_orders
                print(f"    Has items: {has_items}")
            
            print()
    
    # ================================================================
    # 8. Filter analysis - why orders are excluded
    # ================================================================
    print(f"\n🔍 8. FILTER ANALYSIS - WHY ORDERS ARE EXCLUDED")
    print("-" * 50)
    
    if len(raw_not_in_dim) > 0:
        missing_orders = raw_order_df[raw_order_df['order_number'].isin(raw_not_in_dim)]
        
        print(f"📊 Analysis of {len(missing_orders):,} missing orders:")
        
        # Status exclusions
        print(f"\n📋 Status analysis (likely filtering reason):")
        status_analysis = missing_orders['status'].value_counts()
        total_missing = len(missing_orders)
        
        for status, count in status_analysis.items():
            pct = (count / total_missing) * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")
            
        # Date analysis
        print(f"\n📅 Date range analysis:")
        print(f"  Earliest missing order: {missing_orders['order_date'].min()}")
        print(f"  Latest missing order: {missing_orders['order_date'].max()}")
        print(f"  Most common month: {missing_orders['order_month'].mode().iloc[0] if len(missing_orders) > 0 else 'N/A'}")
        
        # Price/value analysis
        if 'price' in missing_orders.columns:
            print(f"\n💰 Price analysis:")
            price_stats = missing_orders['price'].describe()
            print(f"  Mean price: {price_stats['mean']:.2f}")
            print(f"  Zero price orders: {(missing_orders['price'] == 0).sum():,}")
            
    # ================================================================
    # 9. Recommendations
    # ================================================================
    print(f"\n💡 9. COMPLETENESS SUMMARY & RECOMMENDATIONS")
    print("=" * 80)
    
    print(f"\n🎯 KEY FINDINGS:")
    print(f"  • Raw orders extracted: {len(raw_unique_orders):,}")
    print(f"  • Orders in dimensional model: {len(dim_order_ids):,}")
    print(f"  • Completion rate: {completion_rate:.2f}%")
    print(f"  • Orders lost in pipeline: {len(raw_not_in_dim):,}")
    
    if 'order_number' in items_df.columns:
        print(f"  • Raw orders with items: {len(orders_with_items):,}")
        print(f"  • Raw orders without items: {len(orders_without_items):,}")
    
    if completion_rate >= 95:
        print(f"  ✅ EXCELLENT: >95% completion rate achieved!")
    elif completion_rate >= 90:
        print(f"  ✅ GOOD: >90% completion rate achieved")
    elif completion_rate >= 85:
        print(f"  ⚠️  ACCEPTABLE: >85% completion rate")
    else:
        print(f"  ❌ NEEDS IMPROVEMENT: <85% completion rate")
    
    print(f"\n🔍 LIKELY CAUSES OF MISSING ORDERS:")
    if len(raw_not_in_dim) > 0:
        # Analyze missing orders patterns
        missing_df = raw_order_df[raw_order_df['order_number'].isin(raw_not_in_dim)]
        top_missing_statuses = missing_df['status'].value_counts().head(3)
        
        for status, count in top_missing_statuses.items():
            pct_of_missing = (count / len(missing_df)) * 100
            print(f"  • {status}: {count:,} orders ({pct_of_missing:.1f}% of missing)")
            
        # Check if orders without items are being filtered
        if 'order_number' in items_df.columns:
            missing_no_items = raw_not_in_dim - items_unique_orders
            if len(missing_no_items) > 0:
                pct_no_items = (len(missing_no_items) / len(raw_not_in_dim)) * 100
                print(f"  • Orders without items: {len(missing_no_items):,} ({pct_no_items:.1f}% of missing)")
                print(f"    This suggests filtering logic requires order items")
    
    print(f"\n🔧 RECOMMENDED ACTIONS:")
    if len(raw_not_in_dim) > 0:
        print(f"  1. ✅ Review transformation filtering logic for order status exclusions")
        print(f"  2. ✅ Check if orders without items are intentionally excluded")
        print(f"  3. ✅ Verify date range filtering in harmonization scripts")
        print(f"  4. ✅ Document intentional exclusions with business justification")
        print(f"  5. ✅ Add validation checkpoints in transformation pipeline")
        
        if 'order_number' in items_df.columns and len(orders_without_items) > 0:
            print(f"  6. ✅ Consider whether orders without items should be included")
            print(f"  7. ✅ Implement order-item relationship validation")
    else:
        print(f"  1. ✅ Maintain current harmonization process - excellent completion!")
        print(f"  2. ✅ Add automated completion rate monitoring")
        print(f"  3. ✅ Implement daily/weekly validation checks")
    
    print(f"\n📊 NEXT STEPS:")
    print(f"  • Review app/Transformation/ scripts for filtering logic")
    print(f"  • Check loading_script.py for order inclusion criteria")
    print(f"  • Implement order completion rate monitoring dashboard")
    print(f"  • Document business rules for order exclusions")
    print(f"  • Set up alerts for completion rate drops below threshold")

if __name__ == "__main__":
    check_lazada_order_completeness()