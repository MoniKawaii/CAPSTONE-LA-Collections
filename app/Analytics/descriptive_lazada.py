"""
KPI Analytics Script
Calculates key descriptive metrics using the harmonized dimensional and fact CSV files.
Updated to use files from the Transformed folder with the correct schema.
"""
import pandas as pd
import numpy as np
import os

# --- 1. File Loading and Setup ---

def load_data():
    """Loads necessary CSV files from the Transformed folder into pandas DataFrames."""
    try:
        # Get the path to the Transformed folder
        base_path = os.path.join(os.path.dirname(__file__), '..', 'Transformed')
        
        # Load core files for most calculations
        df_order = pd.read_csv(os.path.join(base_path, 'dim_order.csv'))
        df_fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
        
        # Load dimension tables for joins
        df_product = pd.read_csv(os.path.join(base_path, 'dim_product.csv'))
        df_customer = pd.read_csv(os.path.join(base_path, 'dim_customer.csv'))
        df_time = pd.read_csv(os.path.join(base_path, 'dim_time.csv'))
        
        print(f"✅ Loaded data from {base_path}")
        print(f"   📊 Dim Order: {len(df_order):,} records")
        print(f"   📋 Fact Orders: {len(df_fact_orders):,} records")
        print(f"   📦 Dim Product: {len(df_product):,} records")
        print(f"   👥 Dim Customer: {len(df_customer):,} records")
        print(f"   📅 Dim Time: {len(df_time):,} records")
        
        return df_order, df_fact_orders, df_product, df_customer, df_time
    except FileNotFoundError as e:
        print(f"Error: Required file not found in Transformed folder. Missing file: {e}")
        return None, None, None, None, None

def clean_and_merge(df_order, df_fact_orders, df_product, df_customer, df_time):
    """Cleans data and merges necessary tables for unified analysis."""
    
    # --- Cleaning / Type Casting ---
    # Fact_Orders: Ensure core numeric columns are float
    df_fact_orders['paid_price'] = pd.to_numeric(df_fact_orders['paid_price'], errors='coerce')
    df_fact_orders['item_quantity'] = pd.to_numeric(df_fact_orders['item_quantity'], errors='coerce')
    
    # Dim_Order: Ensure total price is float
    df_order['price_total'] = pd.to_numeric(df_order['price_total'], errors='coerce')
    
    # Convert keys to ensure proper joining (they should be float with .1 suffix)
    df_time['time_key'] = pd.to_numeric(df_time['time_key'], errors='coerce')
    df_fact_orders['time_key'] = pd.to_numeric(df_fact_orders['time_key'], errors='coerce')
    
    # --- Merging for Comprehensive Sales Data ---
    # Step 1: Join Fact_Orders with Dim_Product to get product names
    df_sales = df_fact_orders.merge(
        df_product[['product_key', 'product_name', 'product_category']], 
        on='product_key', 
        how='left'
    )
    
    # Step 2: Join with Dim_Order to get status and full order price
    # Note: Use 'order_key' (not 'orders_key') as per our harmonized schema
    df_sales = df_sales.merge(
        df_order[['order_key', 'order_status', 'shipping_city', 'price_total']], 
        on='order_key', 
        how='left',
        suffixes=('_item', '_order')
    )
    
    # Step 3: Join with Dim_Customer for customer details
    df_sales = df_sales.merge(
        df_customer[['customer_key', 'customer_city', 'buyer_segment', 'platform_customer_id']],
        on='customer_key',
        how='left'
    )
    
    # Step 4: Join with Dim_Time for temporal analysis
    df_sales = df_sales.merge(
        df_time[['time_key', 'date', 'year', 'month_name']],
        on='time_key',
        how='left'
    )
    
    print(f"✅ Merged data: {len(df_sales):,} sales records")
    
    return df_sales, df_order

# --- 2. KPI Calculation Functions ---

def calculate_summary_kpis(df_sales, df_order):
    """Calculates Total Sales, Orders, Units Sold, and AOV."""
    
    # Filter for completed orders
    df_completed_orders = df_order[df_order['order_status'] == 'confirmed']
    df_completed_sales = df_sales[df_sales['order_status'] == 'confirmed']
    
    # 1. Total Sales Revenue (Sum of all completed order price_total)
    total_sales_revenue = df_completed_orders['price_total'].sum()
    
    # 2. Number of Orders (Count of distinct completed orders)
    number_of_orders = df_completed_orders['order_key'].nunique()
    
    # 3. Units Sold (Sum of item_quantity from completed sales)
    units_sold = df_completed_sales['item_quantity'].sum()
    
    # 4. Average Order Value (AOV)
    aov = total_sales_revenue / number_of_orders if number_of_orders > 0 else 0
    
    results = {
        "Total Sales Revenue": total_sales_revenue,
        "Number of Orders (Completed)": number_of_orders,
        "Total Units Sold": units_sold,
        "Average Order Value (AOV)": aov,
    }
    return results

def calculate_rate_kpis(df_order):
    """Calculates Cancelled Orders Rate and Fulfillment Rate."""
    
    total_orders = df_order['order_key'].nunique()
    
    # 1. Cancelled Orders Rate
    cancelled_orders = df_order[df_order['order_status'] == 'canceled']['order_key'].nunique()
    cancelled_rate = (cancelled_orders / total_orders) * 100 if total_orders > 0 else 0
    
    # 2. Fulfillment Rate (Assuming 'confirmed' = fulfilled)
    fulfilled_orders = df_order[df_order['order_status'] == 'confirmed']['order_key'].nunique()
    fulfillment_rate = (fulfilled_orders / total_orders) * 100 if total_orders > 0 else 0
    
    # 3. Order Status Distribution
    status_distribution = df_order['order_status'].value_counts()
    
    results = {
        "Total Orders (All Statuses)": total_orders,
        "Cancelled Orders Rate": cancelled_rate,
        "Fulfillment Rate (Confirmed)": fulfillment_rate,
        "Status Distribution": status_distribution,
    }
    return results

def calculate_top_selling_products(df_sales):
    """Identifies and ranks top-selling products by units and revenue."""
    
    df_completed_sales = df_sales[df_sales['order_status'] == 'confirmed']
    
    # Rank by Units Sold
    top_by_units = df_completed_sales.groupby(['product_name', 'product_category']) \
        .agg(Total_Units_Sold=('item_quantity', 'sum')) \
        .reset_index() \
        .sort_values(by='Total_Units_Sold', ascending=False) \
        .head(5)
    
    # Rank by Revenue
    top_by_revenue = df_completed_sales.groupby(['product_name', 'product_category']) \
        .agg(Total_Revenue=('paid_price', 'sum')) \
        .reset_index() \
        .sort_values(by='Total_Revenue', ascending=False) \
        .head(5)
        
    return top_by_units, top_by_revenue

def analyze_customer_location(df_order):
    """Analyzes sales distribution by customer shipping city."""
    
    # Use all orders to see total location distribution
    df_locations = df_order.groupby('shipping_city')['order_key'].nunique() \
        .reset_index(name='Number_of_Orders') \
        .sort_values(by='Number_of_Orders', ascending=False) \
        .head(10)
        
    return df_locations

def analyze_sales_by_time(df_sales):
    """Groups completed sales revenue by month and year for trend analysis."""
    
    df_completed_sales = df_sales[df_sales['order_status'] == 'confirmed'].copy()
    
    # Ensure 'date' column is in datetime format for robust grouping
    df_completed_sales['date'] = pd.to_datetime(df_completed_sales['date'], errors='coerce')
    
    # Calculate item-level revenue (paid_price)
    df_completed_sales['item_revenue'] = df_completed_sales['paid_price']
    
    # Group by Year and Month Name
    df_time_series = df_completed_sales.groupby(['year', 'month_name']) \
        .agg(Total_Monthly_Revenue=('item_revenue', 'sum'), Number_of_Orders=('order_key', 'nunique')) \
        .reset_index()
    
    # Sort for presentation
    df_time_series = df_time_series.sort_values(by=['year'], ascending=[True])
    
    return df_time_series

def analyze_foreign_key_coverage(df_sales):
    """Analyzes the coverage and quality of foreign key relationships."""
    
    total_records = len(df_sales)
    
    # Check null foreign keys
    null_order_key = df_sales['order_key'].isnull().sum()
    null_product_key = df_sales['product_key'].isnull().sum()
    null_customer_key = df_sales['customer_key'].isnull().sum()
    null_time_key = df_sales['time_key'].isnull().sum()
    
    # Check successful joins (non-null values after merge)
    valid_product_name = df_sales['product_name'].notna().sum()
    valid_order_status = df_sales['order_status'].notna().sum()
    valid_customer_city = df_sales['customer_city'].notna().sum()
    valid_date = df_sales['date'].notna().sum()
    
    coverage_stats = {
        "Total Records": total_records,
        "Order Key Coverage": f"{((total_records - null_order_key) / total_records * 100):.1f}%",
        "Product Key Coverage": f"{((total_records - null_product_key) / total_records * 100):.1f}%",
        "Customer Key Coverage": f"{((total_records - null_customer_key) / total_records * 100):.1f}%",
        "Time Key Coverage": f"{((total_records - null_time_key) / total_records * 100):.1f}%",
        "Product Join Success": f"{(valid_product_name / total_records * 100):.1f}%",
        "Order Join Success": f"{(valid_order_status / total_records * 100):.1f}%",
        "Customer Join Success": f"{(valid_customer_city / total_records * 100):.1f}%",
        "Time Join Success": f"{(valid_date / total_records * 100):.1f}%",
    }
    
    return coverage_stats

# --- 3. Main Execution and Display ---

def main():
    """Executes the KPI calculation workflow."""
    print("=" * 70)
    print("🚀 LAZADA SALES ANALYTICS - DIMENSIONAL MODEL")
    print("=" * 70)
    
    df_order, df_fact_orders, df_product, df_customer, df_time = load_data()

    if df_order is None:
        return

    df_sales, df_all_orders = clean_and_merge(df_order, df_fact_orders, df_product, df_customer, df_time)
    
    # --- DATA QUALITY CHECK ---
    coverage_stats = analyze_foreign_key_coverage(df_sales)
    print("\n" + "=" * 70)
    print("📊 DATA QUALITY & FOREIGN KEY COVERAGE")
    print("=" * 70)
    for key, value in coverage_stats.items():
        print(f"{key:.<30} {value}")
    
    # --- CORE KPIS ---
    summary_kpis = calculate_summary_kpis(df_sales, df_all_orders)
    
    print("\n" + "=" * 70)
    print("💰 SUMMARY SALES KPIs")
    print("=" * 70)
    print(f"Total Sales Revenue (Completed):...... ₱ {summary_kpis['Total Sales Revenue']:,.2f}")
    print(f"Number of Completed Orders:........... {summary_kpis['Number of Orders (Completed)']:,}")
    print(f"Total Units Sold:..................... {summary_kpis['Total Units Sold']:,}")
    print(f"Average Order Value (AOV):............ ₱ {summary_kpis['Average Order Value (AOV)']:,.2f}")
    
    # --- RATE KPIS ---
    rate_kpis = calculate_rate_kpis(df_all_orders)
    print("\n" + "=" * 70)
    print("📈 ORDER RATE KPIs")
    print("=" * 70)
    print(f"Total Orders (All Statuses):.......... {rate_kpis['Total Orders (All Statuses)']:,}")
    print(f"Cancelled Orders Rate:................ {rate_kpis['Cancelled Orders Rate']:.2f}%")
    print(f"Fulfillment Rate (Confirmed):......... {rate_kpis['Fulfillment Rate (Confirmed)']:.2f}%")
    
    print(f"\nOrder Status Distribution:")
    for status, count in rate_kpis['Status Distribution'].items():
        percentage = (count / rate_kpis['Total Orders (All Statuses)']) * 100
        print(f"  {status:.<25} {count:,} ({percentage:.1f}%)")
    
    # --- RANKING KPIS ---
    top_units, top_revenue = calculate_top_selling_products(df_sales)
    print("\n" + "=" * 70)
    print("🏆 TOP 5 SELLING PRODUCTS")
    print("=" * 70)
    
    print("\n[A] Top 5 Products by Total Units Sold:")
    if not top_units.empty:
        for idx, row in top_units.iterrows():
            print(f"  {row['product_name'][:40]:.<40} {row['Total_Units_Sold']:,} units")
    else:
        print("  No data available")
    
    print("\n[B] Top 5 Products by Total Revenue:")
    if not top_revenue.empty:
        for idx, row in top_revenue.iterrows():
            print(f"  {row['product_name'][:40]:.<40} ₱ {row['Total_Revenue']:,.2f}")
    else:
        print("  No data available")

    # --- SEGMENTATION KPIS ---
    top_locations = analyze_customer_location(df_all_orders)
    print("\n" + "=" * 70)
    print("🌍 TOP 10 CUSTOMER LOCATIONS (by Orders)")
    print("=" * 70)
    if not top_locations.empty:
        for idx, row in top_locations.iterrows():
            print(f"  {row['shipping_city'][:35]:.<35} {row['Number_of_Orders']:,} orders")
    else:
        print("  No location data available")

    # --- TIME SERIES KPIS ---
    sales_by_time = analyze_sales_by_time(df_sales)
    print("\n" + "=" * 70)
    print("📅 SALES BY TIME PERIOD")
    print("=" * 70)
    if not sales_by_time.empty:
        for idx, row in sales_by_time.iterrows():
            print(f"  {row['year']} {row['month_name'][:12]:.<12} ₱ {row['Total_Monthly_Revenue']:,.2f} ({row['Number_of_Orders']:,} orders)")
    else:
        print("  No time series data available")
    
    print("\n" + "=" * 70)
    print("✅ ANALYTICS COMPLETED SUCCESSFULLY")
    print("=" * 70)
    
if __name__ == "__main__":
    main()
