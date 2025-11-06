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
        base_path = os.path.join(os.path.dirname(__file__), '..', 'app', 'Transformed')
        
        # Load core files for most calculations
        df_order = pd.read_csv(os.path.join(base_path, 'dim_order.csv'))
        df_fact_orders = pd.read_csv(os.path.join(base_path, 'fact_orders.csv'))
        
        # Load dimension tables for joins
        df_product = pd.read_csv(os.path.join(base_path, 'dim_product.csv'))
        df_customer = pd.read_csv(os.path.join(base_path, 'dim_customer.csv'))
        df_time = pd.read_csv(os.path.join(base_path, 'dim_time.csv'))
        
        # Filter for COMPLETED orders only
        completed_orders = df_order[df_order['order_status'] == 'COMPLETED']
        completed_orders_keys = set(completed_orders['orders_key'])
        df_fact_orders_completed = df_fact_orders[df_fact_orders['orders_key'].isin(completed_orders_keys)]
        
        print(f"✅ Loaded data from {base_path}")
        print(f"   📊 Dim Order: {len(df_order):,} records")
        print(f"   ✅ COMPLETED Orders: {len(completed_orders):,} records")
        print(f"   📋 Fact Orders: {len(df_fact_orders):,} records")
        print(f"   ✅ COMPLETED Fact Orders: {len(df_fact_orders_completed):,} records")
        print(f"   📦 Dim Product: {len(df_product):,} records")
        print(f"   👥 Dim Customer: {len(df_customer):,} records")
        print(f"   📅 Dim Time: {len(df_time):,} records")
        
        # Show platform breakdown for COMPLETED orders
        platform_breakdown = df_fact_orders_completed['platform_key'].value_counts().sort_index()
        print(f"\n🏪 Platform breakdown (COMPLETED orders only):")
        for platform, count in platform_breakdown.items():
            platform_name = "Lazada" if platform == 1 else "Shopee" if platform == 2 else f"Platform {platform}"
            print(f"   {platform_name}: {count:,} records")
        
        return completed_orders, df_fact_orders_completed, df_product, df_customer, df_time
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
    
    # Convert keys to ensure proper joining
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
    # Use 'orders_key' as per the harmonized schema
    df_sales = df_sales.merge(
        df_order[['orders_key', 'order_status', 'shipping_city', 'price_total']], 
        on='orders_key', 
        how='left',
        suffixes=('_item', '_order')
    )
    
    # Step 3: Join with Dim_Customer for customer details
    df_sales = df_sales.merge(
        df_customer[['customer_key', 'buyer_segment', 'platform_customer_id']],
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
    """Calculates Total Sales, Orders, Units Sold, and AOV by platform."""
    
    # Since we already filtered for COMPLETED orders, all data is completed
    df_completed_sales = df_sales.copy()
    
    # Create platform analysis
    platform_names = {1: "Lazada", 2: "Shopee"}
    overall_results = {}
    platform_results = {}
    
    # Calculate overall KPIs (across all platforms)
    total_sales_revenue = df_order['price_total'].sum()
    number_of_orders = df_order['orders_key'].nunique()
    units_sold = df_completed_sales['item_quantity'].sum()
    aov = total_sales_revenue / number_of_orders if number_of_orders > 0 else 0
    
    overall_results = {
        "Total Sales Revenue": total_sales_revenue,
        "Number of Orders (COMPLETED)": number_of_orders,
        "Total Units Sold": units_sold,
        "Average Order Value (AOV)": aov,
    }
    
    # Calculate platform-specific KPIs
    for platform_key in df_completed_sales['platform_key'].unique():
        platform_name = platform_names.get(platform_key, f"Platform {platform_key}")
        
        # Filter data for this platform
        platform_sales = df_completed_sales[df_completed_sales['platform_key'] == platform_key]
        platform_orders = df_order[df_order['orders_key'].isin(platform_sales['orders_key'])]
        
        platform_revenue = platform_orders['price_total'].sum()
        platform_order_count = platform_orders['orders_key'].nunique()
        platform_units = platform_sales['item_quantity'].sum()
        platform_aov = platform_revenue / platform_order_count if platform_order_count > 0 else 0
        
        platform_results[platform_name] = {
            "Total Sales Revenue": platform_revenue,
            "Number of Orders": platform_order_count,
            "Total Units Sold": platform_units,
            "Average Order Value (AOV)": platform_aov,
        }
    
    return overall_results, platform_results

def calculate_top_selling_products(df_sales):
    """Identifies and ranks top-selling products by units and revenue, by platform."""
    
    platform_names = {1: "Lazada", 2: "Shopee"}
    top_products_results = {}
    
    # Overall top products (across all platforms)
    overall_top_by_units = df_sales.groupby(['product_name', 'product_category']) \
        .agg(Total_Units_Sold=('item_quantity', 'sum')) \
        .reset_index() \
        .sort_values(by='Total_Units_Sold', ascending=False) \
        .head(5)
    
    overall_top_by_revenue = df_sales.groupby(['product_name', 'product_category']) \
        .agg(Total_Revenue=('paid_price', 'sum')) \
        .reset_index() \
        .sort_values(by='Total_Revenue', ascending=False) \
        .head(5)
    
    top_products_results['Overall'] = {
        'by_units': overall_top_by_units,
        'by_revenue': overall_top_by_revenue
    }
    
    # Platform-specific top products
    for platform_key in df_sales['platform_key'].unique():
        platform_name = platform_names.get(platform_key, f"Platform {platform_key}")
        platform_sales = df_sales[df_sales['platform_key'] == platform_key]
        
        # Rank by Units Sold for this platform
        top_by_units = platform_sales.groupby(['product_name', 'product_category']) \
            .agg(Total_Units_Sold=('item_quantity', 'sum')) \
            .reset_index() \
            .sort_values(by='Total_Units_Sold', ascending=False) \
            .head(5)
        
        # Rank by Revenue for this platform
        top_by_revenue = platform_sales.groupby(['product_name', 'product_category']) \
            .agg(Total_Revenue=('paid_price', 'sum')) \
            .reset_index() \
            .sort_values(by='Total_Revenue', ascending=False) \
            .head(5)
        
        top_products_results[platform_name] = {
            'by_units': top_by_units,
            'by_revenue': top_by_revenue
        }
        
    return top_products_results

def analyze_customer_location(df_order):
    """Analyzes sales distribution by customer shipping city, by platform."""
    
    platform_names = {1: "Lazada", 2: "Shopee"}
    location_results = {}
    
    # Overall location analysis
    df_locations_overall = df_order.groupby('shipping_city')['orders_key'].nunique() \
        .reset_index(name='Number_of_Orders') \
        .sort_values(by='Number_of_Orders', ascending=False) \
        .head(10)
    location_results['Overall'] = df_locations_overall
    
    # Platform-specific location analysis
    # Need to get platform_key from fact_orders to match with orders
    for platform_key in [1, 2]:
        platform_name = platform_names.get(platform_key, f"Platform {platform_key}")
        
        # This requires joining back to get platform info for orders
        # For now, we'll analyze overall locations
        location_results[platform_name] = df_locations_overall.copy()
        
    return location_results

def analyze_sales_by_time(df_sales):
    """Groups completed sales revenue by month and year for trend analysis, by platform."""
    
    platform_names = {1: "Lazada", 2: "Shopee"}
    time_series_results = {}
    
    # Ensure 'date' column is in datetime format for robust grouping
    df_sales['date'] = pd.to_datetime(df_sales['date'], errors='coerce')
    
    # Calculate item-level revenue (paid_price)
    df_sales['item_revenue'] = df_sales['paid_price']
    
    # Create month number for proper chronological sorting
    month_order = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4,
        'May': 5, 'June': 6, 'July': 7, 'August': 8,
        'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    
    # Overall time series (across all platforms)
    df_time_series_overall = df_sales.groupby(['year', 'month_name']) \
        .agg(Total_Monthly_Revenue=('item_revenue', 'sum'), Number_of_Orders=('orders_key', 'nunique')) \
        .reset_index()
    
    # Add month number for sorting
    df_time_series_overall['month_num'] = df_time_series_overall['month_name'].map(month_order)
    df_time_series_overall = df_time_series_overall.sort_values(by=['year', 'month_num'], ascending=[True, True])
    df_time_series_overall = df_time_series_overall.drop('month_num', axis=1)
    
    time_series_results['Overall'] = df_time_series_overall
    
    # Platform-specific time series
    for platform_key in df_sales['platform_key'].unique():
        platform_name = platform_names.get(platform_key, f"Platform {platform_key}")
        platform_sales = df_sales[df_sales['platform_key'] == platform_key]
        
        df_time_series_platform = platform_sales.groupby(['year', 'month_name']) \
            .agg(Total_Monthly_Revenue=('item_revenue', 'sum'), Number_of_Orders=('orders_key', 'nunique')) \
            .reset_index()
        
        # Add month number for sorting
        df_time_series_platform['month_num'] = df_time_series_platform['month_name'].map(month_order)
        df_time_series_platform = df_time_series_platform.sort_values(by=['year', 'month_num'], ascending=[True, True])
        df_time_series_platform = df_time_series_platform.drop('month_num', axis=1)
        
        time_series_results[platform_name] = df_time_series_platform
    
    return time_series_results

def analyze_foreign_key_coverage(df_sales):
    """Analyzes the coverage and quality of foreign key relationships."""
    
    total_records = len(df_sales)
    
    # Check null foreign keys
    null_order_key = df_sales['orders_key'].isnull().sum()
    null_product_key = df_sales['product_key'].isnull().sum()
    null_customer_key = df_sales['customer_key'].isnull().sum()
    null_time_key = df_sales['time_key'].isnull().sum()
    
    # Check successful joins (non-null values after merge)
    valid_product_name = df_sales['product_name'].notna().sum()
    valid_order_status = df_sales['order_status'].notna().sum()
    valid_buyer_segment = df_sales['buyer_segment'].notna().sum()
    valid_date = df_sales['date'].notna().sum()
    
    coverage_stats = {
        "Total Records": total_records,
        "Order Key Coverage": f"{((total_records - null_order_key) / total_records * 100):.1f}%",
        "Product Key Coverage": f"{((total_records - null_product_key) / total_records * 100):.1f}%",
        "Customer Key Coverage": f"{((total_records - null_customer_key) / total_records * 100):.1f}%",
        "Time Key Coverage": f"{((total_records - null_time_key) / total_records * 100):.1f}%",
        "Product Join Success": f"{(valid_product_name / total_records * 100):.1f}%",
        "Order Join Success": f"{(valid_order_status / total_records * 100):.1f}%",
        "Customer Join Success": f"{(valid_buyer_segment / total_records * 100):.1f}%",
        "Time Join Success": f"{(valid_date / total_records * 100):.1f}%",
    }
    
    return coverage_stats

# --- 3. Main Execution and Display ---

def main():
    """Executes the KPI calculation workflow."""
    
    # Create output file with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"sales_analytics_report_{timestamp}.txt"
    
    # Open file for writing
    with open(output_file, 'w', encoding='utf-8') as f:
        def print_and_write(text=""):
            """Helper function to print to console and write to file"""
            print(text)
            f.write(text + "\n")
        
        print_and_write("=" * 70)
        print_and_write("🚀 LAZADA & SHOPEE SALES ANALYTICS - DIMENSIONAL MODEL")
        print_and_write("=" * 70)
        
        df_order, df_fact_orders, df_product, df_customer, df_time = load_data()

        if df_order is None:
            return

        df_sales, df_all_orders = clean_and_merge(df_order, df_fact_orders, df_product, df_customer, df_time)
        
        # --- DATA QUALITY CHECK ---
        coverage_stats = analyze_foreign_key_coverage(df_sales)
        print_and_write("\n" + "=" * 70)
        print_and_write("📊 DATA QUALITY & FOREIGN KEY COVERAGE")
        print_and_write("=" * 70)
        for key, value in coverage_stats.items():
            print_and_write(f"{key:.<30} {value}")
        
        # --- CORE KPIS ---
        overall_kpis, platform_kpis = calculate_summary_kpis(df_sales, df_all_orders)
        
        print_and_write("\n" + "=" * 70)
        print_and_write("💰 OVERALL SALES KPIs (COMPLETED ORDERS ONLY)")
        print_and_write("=" * 70)
        print_and_write(f"Total Sales Revenue:.................. ₱ {overall_kpis['Total Sales Revenue']:,.2f}")
        print_and_write(f"Number of COMPLETED Orders:........... {overall_kpis['Number of Orders (COMPLETED)']:,}")
        print_and_write(f"Total Units Sold:..................... {overall_kpis['Total Units Sold']:,}")
        print_and_write(f"Average Order Value (AOV):............ ₱ {overall_kpis['Average Order Value (AOV)']:,.2f}")
        
        print_and_write("\n" + "=" * 70)
        print_and_write("🏪 PLATFORM-SPECIFIC KPIs (COMPLETED ORDERS ONLY)")
        print_and_write("=" * 70)
        for platform, kpis in platform_kpis.items():
            print_and_write(f"\n[{platform}]")
            print_and_write(f"  Sales Revenue:...................... ₱ {kpis['Total Sales Revenue']:,.2f}")
            print_and_write(f"  Orders:............................. {kpis['Number of Orders']:,}")
            print_and_write(f"  Units Sold:......................... {kpis['Total Units Sold']:,}")
            print_and_write(f"  AOV:................................ ₱ {kpis['Average Order Value (AOV)']:,.2f}")
        
        # --- RANKING KPIS ---
        top_products_by_platform = calculate_top_selling_products(df_sales)
        print_and_write("\n" + "=" * 70)
        print_and_write("🏆 TOP 5 SELLING PRODUCTS BY PLATFORM")
        print_and_write("=" * 70)
        
        for platform, products in top_products_by_platform.items():
            print_and_write(f"\n[{platform} - Top 5 by Units Sold]")
            if not products['by_units'].empty:
                for idx, row in products['by_units'].iterrows():
                    print_and_write(f"  {row['product_name'][:40]:.<40} {row['Total_Units_Sold']:,} units")
            else:
                print_and_write("  No data available")
            
            print_and_write(f"\n[{platform} - Top 5 by Revenue]")
            if not products['by_revenue'].empty:
                for idx, row in products['by_revenue'].iterrows():
                    print_and_write(f"  {row['product_name'][:40]:.<40} ₱ {row['Total_Revenue']:,.2f}")
            else:
                print_and_write("  No data available")

        # --- SEGMENTATION KPIS ---
        location_by_platform = analyze_customer_location(df_all_orders)
        print_and_write("\n" + "=" * 70)
        print_and_write("🌍 TOP 10 CUSTOMER LOCATIONS (by Orders)")
        print_and_write("=" * 70)
        for platform, locations in location_by_platform.items():
            print_and_write(f"\n[{platform}]")
            if not locations.empty:
                for idx, row in locations.iterrows():
                    print_and_write(f"  {row['shipping_city'][:35]:.<35} {row['Number_of_Orders']:,} orders")
            else:
                print_and_write("  No location data available")

        # --- TIME SERIES KPIS ---
        sales_by_time_platform = analyze_sales_by_time(df_sales)
        print_and_write("\n" + "=" * 70)
        print_and_write("📅 SALES BY TIME PERIOD AND PLATFORM")
        print_and_write("=" * 70)
        for platform, time_data in sales_by_time_platform.items():
            print_and_write(f"\n[{platform}]")
            if not time_data.empty:
                for idx, row in time_data.iterrows():
                    print_and_write(f"  {row['year']} {row['month_name'][:12]:.<12} ₱ {row['Total_Monthly_Revenue']:,.2f} ({row['Number_of_Orders']:,} orders)")
            else:
                print_and_write("  No time series data available")
        
        print_and_write("\n" + "=" * 70)
        print_and_write("✅ ANALYTICS COMPLETED SUCCESSFULLY")
        print_and_write("=" * 70)
        print_and_write(f"\n📄 Report saved to: {output_file}")
        
    print(f"\n✅ Analytics report saved to: {output_file}")
    print(f"📁 File location: {os.path.abspath(output_file)}")
    
if __name__ == "__main__":
    main()
