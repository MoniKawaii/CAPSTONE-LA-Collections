"""
Sales Aggregate Harmonization Script
Creates Fact_Sales_Aggregate table with proper data cube granularity:
- Grain: Time, Platform, Customer, Product
- Allows slicing by date, customer segment/city, product category, etc.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys

# Add the app directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import FACT_SALES_AGGREGATE_COLUMNS, COLUMN_DATA_TYPES, apply_data_types

def load_dimension_tables():
    """Load all required dimension and fact tables"""
    base_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed')
    
    # Load dimension tables
    dim_time = pd.read_csv(os.path.join(base_path, 'dim_time.csv'))
    dim_customer = pd.read_csv(os.path.join(base_path, 'dim_customer.csv'))
    dim_product = pd.read_csv(os.path.join(base_path, 'dim_product.csv'))
    
    # Load fact orders
    fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
    
    print(f"✅ Loaded dimension tables:")
    print(f"   📅 Dim Time: {len(dim_time):,} records")
    print(f"   👥 Dim Customer: {len(dim_customer):,} records") 
    print(f"   📦 Dim Product: {len(dim_product):,} records")
    print(f"   📋 Fact Orders: {len(fact_orders):,} records")
    
    return dim_time, dim_customer, dim_product, fact_orders

def generate_sales_aggregate(dim_time, dim_customer, dim_product, fact_orders):
    """Generate sales aggregate with proper grain: Time x Platform x Customer x Product"""
    print(f"\n🔄 Generating Sales Aggregate with granularity: Time x Platform x Customer x Product")
    
    # Validate fact_orders has expected columns
    required_cols = ['time_key', 'platform_key', 'customer_key', 'product_key', 'orders_key', 
                    'item_quantity', 'paid_price', 'voucher_platform_amount', 'voucher_seller_amount']
    missing_cols = [col for col in required_cols if col not in fact_orders.columns]
    if missing_cols:
        print(f"⚠️  Warning: Missing columns in fact_orders: {missing_cols}")
        print(f"Available columns: {list(fact_orders.columns)}")
    
    # Group fact_orders by the grain dimensions and aggregate metrics
    # Note: With unit-level granularity, item_quantity should always be 1, but we sum for robustness
    sales_agg = fact_orders.groupby([
        'time_key', 'platform_key', 'customer_key', 'product_key'
    ]).agg({
        'orders_key': 'nunique',  # Count unique orders (handles multiple units from same order)
        'item_quantity': 'sum',   # Sum quantities (should equal record count due to unit-level granularity)
        'paid_price': 'sum',      # Total revenue after all discounts
        'voucher_platform_amount': 'sum',  # Platform-provided discounts
        'voucher_seller_amount': 'sum'     # Seller-provided discounts
    }).reset_index()
    
    # Rename columns to match schema
    sales_agg.columns = [
        'time_key', 'platform_key', 'customer_key', 'product_key',
        'total_orders', 'total_items_sold', 'net_sales', 
        'platform_discounts', 'seller_discounts'
    ]
    
    # Calculate derived metrics
    sales_agg['total_discounts'] = sales_agg['platform_discounts'] + sales_agg['seller_discounts']
    
    # Calculate gross revenue (revenue before discounts)
    # gross_revenue = net_sales + total_discounts
    sales_agg['gross_revenue'] = sales_agg['net_sales'] + sales_agg['total_discounts']
    
    # Define the columns we want in final output (matching expected schema)
    final_columns = [
        'time_key', 'platform_key', 'customer_key', 'product_key',
        'total_orders', 'total_items_sold', 'gross_revenue',
        'total_discounts', 'net_sales'
    ]
    
    # Reorder columns
    sales_agg = sales_agg[final_columns]
    
    print(f"✅ Generated {len(sales_agg):,} sales aggregate records")
    print(f"   📊 Covering {sales_agg['time_key'].nunique():,} unique dates")
    print(f"   👥 Covering {sales_agg['customer_key'].nunique():,} unique customers")  
    print(f"   📦 Covering {sales_agg['product_key'].nunique():,} unique products")
    print(f"   🏪 Covering {sales_agg['platform_key'].nunique():,} platforms")
    
    # Show unit-level validation
    total_records = len(fact_orders)
    total_items_agg = sales_agg['total_items_sold'].sum()
    print(f"   🔍 Unit-level validation: {total_records:,} records → {total_items_agg:,} items")
    if total_records == total_items_agg:
        print(f"      ✅ Perfect unit-level granularity maintained")
    else:
        print(f"      ⚠️  Granularity discrepancy detected")
    
    return sales_agg

def validate_sales_aggregate(sales_agg, fact_orders):
    """Validate the sales aggregate against source data"""
    print(f"\n🔍 Validating Sales Aggregate...")
    
    # Validation 1: Net revenue should match (paid_price is already net after discounts)
    source_net_revenue = fact_orders['paid_price'].sum()
    agg_net_revenue = sales_agg['net_sales'].sum()
    
    print(f"   💰 Net revenue validation:")
    print(f"      Source total (paid_price): ${source_net_revenue:,.2f}")
    print(f"      Aggregate total (net_sales): ${agg_net_revenue:,.2f}")
    print(f"      Match: {'✅' if abs(source_net_revenue - agg_net_revenue) < 0.01 else '❌'}")
    
    # Validation 2: Gross revenue calculation validation
    source_vouchers = fact_orders['voucher_platform_amount'].sum() + fact_orders['voucher_seller_amount'].sum()
    agg_gross_revenue = sales_agg['gross_revenue'].sum()
    agg_total_discounts = sales_agg['total_discounts'].sum()
    calculated_gross = source_net_revenue + source_vouchers
    
    print(f"   💵 Gross revenue validation:")
    print(f"      Calculated gross (net + vouchers): ${calculated_gross:,.2f}")
    print(f"      Aggregate gross_revenue: ${agg_gross_revenue:,.2f}")
    print(f"      Aggregate total_discounts: ${agg_total_discounts:,.2f}")
    print(f"      Match: {'✅' if abs(calculated_gross - agg_gross_revenue) < 0.01 else '❌'}")
    
    # Validation 3: Total items should match (with unit-level granularity)
    source_items = fact_orders['item_quantity'].sum()
    source_records = len(fact_orders)
    agg_items = sales_agg['total_items_sold'].sum()
    
    print(f"   📦 Items/Units validation:")
    print(f"      Source records (unit-level): {source_records:,}")
    print(f"      Source item_quantity sum: {source_items:,}")
    print(f"      Aggregate total_items_sold: {agg_items:,}")
    print(f"      Unit-level consistency: {'✅' if source_items == source_records else '❌'}")
    print(f"      Aggregate match: {'✅' if source_items == agg_items else '❌'}")
    
    # Validation 4: Orders count validation  
    source_unique_orders = fact_orders['orders_key'].nunique()
    agg_total_orders = sales_agg['total_orders'].sum()
    
    print(f"   📋 Orders validation:")
    print(f"      Source unique orders: {source_unique_orders:,}")
    print(f"      Aggregate total_orders sum: {agg_total_orders:,}")
    print(f"      Note: Aggregate sum may be higher due to cross-dimensional counting")
    
    # Validation 5: Check for null values
    null_counts = sales_agg.isnull().sum()
    print(f"   🔍 Null value check:")
    for col, count in null_counts.items():
        if count > 0:
            print(f"      ⚠️  {col}: {count} nulls")
    if null_counts.sum() == 0:
        print(f"      ✅ No null values found")
    
    # Validation 6: Revenue consistency check
    revenue_diff = abs(agg_gross_revenue - agg_total_discounts - agg_net_revenue)
    print(f"   🧮 Revenue math validation:")
    print(f"      Gross - Discounts - Net = {revenue_diff:.2f}")
    print(f"      Math consistency: {'✅' if revenue_diff < 0.01 else '❌'}")

def main():
    """Main execution function"""
    print("=" * 60)
    print("🚀 FACT SALES AGGREGATE HARMONIZATION")
    print("=" * 60)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Grain: Time x Platform x Customer x Product")
    
    try:
        # Step 1: Load source data
        print(f"\n📂 Step 1: Loading source tables...")
        dim_time, dim_customer, dim_product, fact_orders = load_dimension_tables()
        
        # Step 2: Generate sales aggregate
        print(f"\n🔄 Step 2: Generating sales aggregate...")
        sales_agg = generate_sales_aggregate(dim_time, dim_customer, dim_product, fact_orders)
        
        # Step 3: Validate results
        print(f"\n🔍 Step 3: Validating results...")
        validate_sales_aggregate(sales_agg, fact_orders)
        
        # Step 4: Save to CSV
        output_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed', 'fact_sales_aggregate.csv')
        sales_agg.to_csv(output_path, index=False)
        print(f"\n💾 Step 4: Saved to {output_path}")
        
        # Summary statistics
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"   📋 Total records: {len(sales_agg):,}")
        print(f"   📅 Date range: {sales_agg['time_key'].min()} to {sales_agg['time_key'].max()}")
        print(f"   💰 Total gross revenue: ${sales_agg['gross_revenue'].sum():,.2f}")
        print(f"   💸 Total discounts: ${sales_agg['total_discounts'].sum():,.2f}")
        print(f"   💵 Total net sales: ${sales_agg['net_sales'].sum():,.2f}")
        print(f"   📦 Total items sold: {sales_agg['total_items_sold'].sum():,}")
        
        print(f"\n✅ Sales aggregate harmonization completed successfully!")
        print(f"📅 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ Error during harmonization: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
