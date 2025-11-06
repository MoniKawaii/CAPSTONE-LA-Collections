#!/usr/bin/env python3
"""
Sales Analytics - Python Version
Cross-references with transformed CSV files to understand discrepancies
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_transformed_data():
    """Load all transformed CSV files"""
    base_path = r'c:\Users\alyss\Desktop\CAPSTONE-LA-Collections\app\Transformed'
    
    # Load all CSV files
    fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
    dim_order = pd.read_csv(os.path.join(base_path, 'dim_order.csv'))
    dim_product = pd.read_csv(os.path.join(base_path, 'dim_product.csv'))
    dim_time = pd.read_csv(os.path.join(base_path, 'dim_time.csv'))
    dim_customer = pd.read_csv(os.path.join(base_path, 'dim_customer.csv'))
    
    print("📊 Data loaded successfully!")
    print(f"📦 Fact Orders: {len(fact_orders):,} records")
    print(f"📋 Dim Order: {len(dim_order):,} records")
    print(f"🛍️ Dim Product: {len(dim_product):,} records")
    print(f"⏰ Dim Time: {len(dim_time):,} records")
    print(f"👥 Dim Customer: {len(dim_customer):,} records")
    print("=" * 60)
    
    return fact_orders, dim_order, dim_product, dim_time, dim_customer

def analyze_financial_discrepancy(fact_orders, dim_order):
    """Analyze the financial discrepancy between different price fields"""
    
    # Merge fact_orders with dim_order to get completed orders only
    merged = pd.merge(fact_orders, dim_order, on='orders_key', how='inner')
    completed_orders = merged[merged['order_status'] == 'COMPLETED'].copy()
    
    print("🔍 FINANCIAL DISCREPANCY ANALYSIS")
    print("=" * 60)
    
    # Calculate various price metrics
    total_records = len(completed_orders)
    
    # Method 1: Using original_unit_price from fact_orders (this should be the "gross")
    gross_from_original_price = (completed_orders['original_unit_price'] * completed_orders['item_quantity']).sum()
    
    # Method 2: Using price_total from dim_order (this might be aggregated differently)
    gross_from_order_total = completed_orders['price_total'].sum()
    
    # Method 3: Using paid_price from fact_orders (net amount)
    net_from_paid_price = completed_orders['paid_price'].sum()
    
    # Discounts
    platform_discounts = completed_orders['voucher_platform_amount'].sum()
    seller_discounts = completed_orders['voucher_seller_amount'].sum()
    total_discounts = platform_discounts + seller_discounts
    
    # Shipping fees
    shipping_fees = completed_orders['shipping_fee_paid_by_buyer'].sum()
    
    print(f"📊 Total COMPLETED Records: {total_records:,}")
    print(f"💰 Gross Revenue (original_unit_price × quantity): ₱ {gross_from_original_price:,.2f}")
    print(f"💰 Gross Revenue (price_total from dim_order): ₱ {gross_from_order_total:,.2f}")
    print(f"🏷️ Platform Discounts: ₱ {platform_discounts:,.2f}")
    print(f"🏷️ Seller Discounts: ₱ {seller_discounts:,.2f}")
    print(f"🏷️ Total Discounts: ₱ {total_discounts:,.2f}")
    print(f"🚚 Shipping Fees Paid by Buyers: ₱ {shipping_fees:,.2f}")
    print(f"💵 Net Sales (paid_price): ₱ {net_from_paid_price:,.2f}")
    
    print("\n🧮 CALCULATION ANALYSIS:")
    print("=" * 60)
    
    # Different calculation approaches
    calc1 = gross_from_original_price - total_discounts
    calc2 = gross_from_order_total - total_discounts
    
    print(f"📐 Method 1: (original_unit_price × qty) - discounts = ₱ {calc1:,.2f}")
    print(f"📐 Method 2: price_total - discounts = ₱ {calc2:,.2f}")
    print(f"💸 Actual paid_price total = ₱ {net_from_paid_price:,.2f}")
    
    print(f"\n⚠️ DISCREPANCIES:")
    print(f"   Method 1 vs Actual: ₱ {calc1 - net_from_paid_price:,.2f}")
    print(f"   Method 2 vs Actual: ₱ {calc2 - net_from_paid_price:,.2f}")
    print(f"   Original vs Order Total: ₱ {gross_from_original_price - gross_from_order_total:,.2f}")
    
    # Check if shipping might explain the difference
    potential_with_shipping = net_from_paid_price + shipping_fees
    print(f"\n🚚 WITH SHIPPING ANALYSIS:")
    print(f"   Net + Shipping = ₱ {potential_with_shipping:,.2f}")
    print(f"   Diff from Method 1: ₱ {calc1 - potential_with_shipping:,.2f}")
    print(f"   Diff from Method 2: ₱ {calc2 - potential_with_shipping:,.2f}")
    
    return completed_orders

def platform_breakdown(completed_orders):
    """Platform-specific analysis"""
    print("\n🏪 PLATFORM BREAKDOWN")
    print("=" * 60)
    
    # Check available columns first
    platform_col = None
    if 'platform_key_x' in completed_orders.columns:
        platform_col = 'platform_key_x'
    elif 'platform_key_y' in completed_orders.columns:
        platform_col = 'platform_key_y'
    elif 'platform_key' in completed_orders.columns:
        platform_col = 'platform_key'
    else:
        print("⚠️ Platform key column not found. Available columns:")
        print(completed_orders.columns.tolist())
        return
    
    print(f"Using platform column: {platform_col}")
    
    platform_stats = completed_orders.groupby(platform_col).agg({
        'orders_key': 'nunique',
        'original_unit_price': lambda x: (x * completed_orders.loc[x.index, 'item_quantity']).sum(),
        'price_total': 'sum',
        'paid_price': 'sum',
        'voucher_platform_amount': 'sum',
        'voucher_seller_amount': 'sum',
        'shipping_fee_paid_by_buyer': 'sum',
        'item_quantity': 'sum'
    }).round(2)
    
    platform_stats.columns = ['Orders', 'Gross_Original', 'Gross_OrderTotal', 'Net_Paid', 
                              'Platform_Discounts', 'Seller_Discounts', 'Shipping_Fees', 'Items_Sold']
    
    # Add platform names
    platform_names = {1: 'Lazada', 2: 'Shopee'}
    platform_stats['Platform'] = platform_stats.index.map(platform_names)
    
    for idx, row in platform_stats.iterrows():
        platform_name = row['Platform']
        print(f"\n{platform_name} (Platform {idx}):")
        print(f"   📦 Orders: {row['Orders']:,}")
        print(f"   💰 Gross (Original): ₱ {row['Gross_Original']:,.2f}")
        print(f"   💰 Gross (Order Total): ₱ {row['Gross_OrderTotal']:,.2f}")
        print(f"   💵 Net Paid: ₱ {row['Net_Paid']:,.2f}")
        print(f"   🏷️ Platform Discounts: ₱ {row['Platform_Discounts']:,.2f}")
        print(f"   🏷️ Seller Discounts: ₱ {row['Seller_Discounts']:,.2f}")
        print(f"   🚚 Shipping Fees: ₱ {row['Shipping_Fees']:,.2f}")
        print(f"   📊 Items Sold: {row['Items_Sold']:,}")
        
        # Calculate revenue share
        total_net = platform_stats['Net_Paid'].sum()
        revenue_share = (row['Net_Paid'] / total_net) * 100
        print(f"   📈 Revenue Share: {revenue_share:.1f}%")

def top_products_analysis(completed_orders, dim_product):
    """Top products analysis"""
    print("\n🏆 TOP PRODUCTS ANALYSIS")
    print("=" * 60)
    
    # Merge with product data
    products_data = pd.merge(completed_orders, dim_product, on='product_key', how='left')
    
    # Top by units sold
    top_by_units = products_data.groupby(['product_key', 'product_name']).agg({
        'item_quantity': 'sum',
        'paid_price': 'sum'
    }).sort_values('item_quantity', ascending=False).head(10)
    
    print("🔝 TOP 10 PRODUCTS BY UNITS SOLD:")
    for idx, (product_info, row) in enumerate(top_by_units.iterrows(), 1):
        product_key, product_name = product_info
        print(f"   {idx:2d}. {product_name[:50]}...")
        print(f"       Units: {row['item_quantity']:,} | Revenue: ₱ {row['paid_price']:,.2f}")
    
    # Top by revenue
    top_by_revenue = products_data.groupby(['product_key', 'product_name']).agg({
        'item_quantity': 'sum',
        'paid_price': 'sum'
    }).sort_values('paid_price', ascending=False).head(10)
    
    print("\n💰 TOP 10 PRODUCTS BY REVENUE:")
    for idx, (product_info, row) in enumerate(top_by_revenue.iterrows(), 1):
        product_key, product_name = product_info
        print(f"   {idx:2d}. {product_name[:50]}...")
        print(f"       Revenue: ₱ {row['paid_price']:,.2f} | Units: {row['item_quantity']:,}")

def monthly_trends_analysis(completed_orders, dim_time):
    """Monthly trends analysis"""
    print("\n📅 MONTHLY TRENDS ANALYSIS")
    print("=" * 60)
    
    # Merge with time data
    time_data = pd.merge(completed_orders, dim_time, on='time_key', how='left')
    
    # Monthly aggregation
    monthly_stats = time_data.groupby(['year', 'month_name']).agg({
        'orders_key': 'nunique',
        'paid_price': 'sum',
        'item_quantity': 'sum',
        'original_unit_price': lambda x: (x * time_data.loc[x.index, 'item_quantity']).sum()
    }).round(2)
    
    monthly_stats.columns = ['Orders', 'Net_Revenue', 'Items_Sold', 'Gross_Revenue']
    
    # Sort by year and month
    month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    print("📊 MONTHLY PERFORMANCE (Last 12 months):")
    for (year, month), row in monthly_stats.iterrows():
        print(f"   {year} {month}: ₱ {row['Net_Revenue']:,.2f} ({row['Orders']:,} orders, {row['Items_Sold']:,} items)")

def main():
    """Main analysis function"""
    print("🚀 SALES ANALYTICS - PYTHON VERSION")
    print("📋 Cross-referencing with Transformed CSV Files")
    print("=" * 60)
    
    try:
        # Load data
        fact_orders, dim_order, dim_product, dim_time, dim_customer = load_transformed_data()
        
        # Analyze financial discrepancy
        completed_orders = analyze_financial_discrepancy(fact_orders, dim_order)
        
        # Platform breakdown
        platform_breakdown(completed_orders)
        
        # Top products analysis
        top_products_analysis(completed_orders, dim_product)
        
        # Monthly trends
        monthly_trends_analysis(completed_orders, dim_time)
        
        print(f"\n✅ ANALYSIS COMPLETED at {datetime.now()}")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()