"""
Lazada Dimensional ETL System - Complete Guide
==============================================================================

This system provides a comprehensive ETL solution for Lazada data extraction 
and transformation into a dimensional data warehouse schema.

SYSTEM COMPONENTS:
==============================================================================

1. 📡 API Functions (lazada_api_functions.py)
   - get_orders(): Fetch orders with pagination and date filtering
   - get_order_items(): Fetch order items for specific orders
   - get_products(): Fetch product catalog with pagination
   - get_seller_vouchers(): Fetch seller vouchers/promotions
   - get_voucher_products(): Fetch products associated with vouchers

2. 🔄 Dimensional Transformer (lazada_dimensional_transformer.py)
   - Transforms raw API data into dimensional schema format
   - Creates fact_sales table with proper Lazada pricing calculations
   - Generates dimension tables: customer, product, time, promotion
   - Handles data cleansing and business logic transformations

3. ⚙️ Main ETL Processor (lazada_dimensional_etl.py)
   - Orchestrates full ETL pipeline
   - Handles batch API processing with rate limiting
   - Manages token authentication and error handling
   - Saves output to CSV files for data warehouse loading

4. 🧪 Test Suite (test_dimensional_etl.py)
   - Tests transformation logic with sample data
   - Validates dimensional schema mapping
   - Demonstrates expected output format

DIMENSIONAL SCHEMA OUTPUT:
==============================================================================

✅ FACT_SALES Table:
   - Sales transactions with calculated pricing components
   - Links to all dimension tables via foreign keys
   - Includes Lazada-specific fields (vouchers, tracking, etc.)

✅ DIM_CUSTOMER Table:
   - Customer profiles with billing/shipping addresses
   - Deduplicated customer records

✅ DIM_PRODUCT Table:
   - Product catalog with categories, brands, SKUs
   - Enhanced with Lazada product metadata

✅ DIM_TIME Table:
   - Complete time dimension with date hierarchies
   - Business calendar support (weekends, holidays)

✅ DIM_PROMOTION Table:
   - Voucher and promotion details
   - Discount types and values

USAGE INSTRUCTIONS:
==============================================================================

🔑 STEP 1: Ensure Valid Tokens
   Make sure you have valid Lazada API tokens in lazada_tokens.json

🚀 STEP 2: Run Complete ETL
   python -m app.lazada.lazada_dimensional_etl
   
   This will:
   - Fetch last 30 days of orders, order items, products, vouchers
   - Transform to dimensional schema
   - Save CSV files to data/lazada_dimensional/

🧪 STEP 3: Test with Sample Data (Optional)
   python -m app.lazada.test_dimensional_etl
   
   This will:
   - Use sample data to test transformation logic
   - Validate schema mapping
   - Save test output to data/test_dimensional_output/

📊 STEP 4: Load to Data Warehouse
   Use the generated CSV files to load into your data warehouse:
   - fact_sales.csv
   - dim_customer.csv
   - dim_product.csv
   - dim_time.csv
   - dim_promotion.csv

CONFIGURATION OPTIONS:
==============================================================================

📅 Date Range:
   etl.run_full_etl(days_back=60)  # Fetch last 60 days

📁 Output Directory:
   etl.run_full_etl(output_dir="custom/path")

⚡ Batch Size:
   etl.batch_size = 50  # Reduce for rate limiting

PRICING CALCULATIONS:
==============================================================================

The system calculates Lazada pricing components according to the dimensional schema:

💰 unit_price = item_price / quantity
💰 item_price = Original item price from API
💰 paid_price = Actual amount paid after discounts
💰 original_price = Price before any discounts
💰 wallet_credits = item_price - paid_price (credits used)
💰 total_discount = original_price - paid_price
💰 gross_revenue = item_price (before discounts)
💰 net_revenue = paid_price (actual revenue)

ERROR HANDLING:
==============================================================================

⚠️ Token Expiration: 
   System automatically attempts token refresh

⚠️ API Rate Limits:
   Built-in delays between API calls (0.5-1 second)

⚠️ Network Issues:
   Retry logic for failed API calls

⚠️ Data Validation:
   Handles missing/null values gracefully

MONITORING AND LOGGING:
==============================================================================

📝 All operations are logged with timestamps and status
📊 Progress indicators show current processing status
✅ Success/failure indicators for each API endpoint
📈 Record counts for validation

NEXT STEPS:
==============================================================================

1. 🔗 Set up your data warehouse tables using Enhanced_Lazada_Sales_Schema_Dimensional.sql
2. 🔄 Run the ETL on a schedule (daily/weekly)
3. 📊 Build reports and dashboards on the dimensional data
4. 📈 Add more API endpoints as needed
5. 🔍 Implement data quality checks and alerts

==============================================================================
🎯 READY FOR PRODUCTION USE!
==============================================================================
"""

import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def check_system_status():
    """Check if all components are ready"""
    print("🔍 LAZADA ETL SYSTEM STATUS CHECK")
    print("="*60)
    
    base_path = os.path.dirname(__file__)
    
    # Check files
    files_to_check = [
        ('lazada_api_functions.py', '📡 API Functions'),
        ('lazada_dimensional_transformer.py', '🔄 Transformer'),
        ('lazada_dimensional_etl.py', '⚙️ Main ETL'),
        ('test_dimensional_etl.py', '🧪 Test Suite'),
        ('get_lazada_tokens.py', '🔑 Token Manager')
    ]
    
    all_ready = True
    
    for filename, description in files_to_check:
        filepath = os.path.join(base_path, filename)
        if os.path.exists(filepath):
            print(f"✅ {description}: Ready")
        else:
            print(f"❌ {description}: Missing ({filename})")
            all_ready = False
    
    # Check schema file
    schema_path = os.path.join(base_path, 'Enhanced_Lazada_Sales_Schema_Dimensional.sql')
    if os.path.exists(schema_path):
        print(f"✅ 🗃️ Schema File: Ready")
    else:
        print(f"⚠️ 🗃️ Schema File: Missing (Enhanced_Lazada_Sales_Schema_Dimensional.sql)")
    
    # Check tokens
    tokens_path = os.path.join(base_path, 'lazada_tokens.json')
    if os.path.exists(tokens_path):
        print(f"✅ 🔑 Tokens: Available")
    else:
        print(f"⚠️ 🔑 Tokens: Need to generate tokens first")
    
    print("="*60)
    
    if all_ready:
        print("🎉 SYSTEM READY FOR ETL OPERATIONS!")
        return True
    else:
        print("⚠️ Some components missing. Check installation.")
        return False

def show_usage_examples():
    """Show usage examples"""
    print("\n🚀 QUICK START EXAMPLES")
    print("="*60)
    
    print("\n1️⃣ Test with Sample Data:")
    print("   cd CAPSTONE-LA-Collections")
    print("   python -m app.lazada.test_dimensional_etl")
    
    print("\n2️⃣ Run Full ETL (with valid tokens):")
    print("   cd CAPSTONE-LA-Collections")  
    print("   python -m app.lazada.lazada_dimensional_etl")
    
    print("\n3️⃣ Custom ETL Options:")
    print("   # In Python:")
    print("   from app.lazada.lazada_dimensional_etl import LazadaDimensionalETL")
    print("   etl = LazadaDimensionalETL()")
    print("   etl.run_full_etl(days_back=60, output_dir='custom/path')")
    
    print("\n4️⃣ Test API Functions:")
    print("   python -m app.lazada.lazada_api_functions")

def main():
    """Main information display"""
    print(__doc__)
    
    # System status check
    system_ready = check_system_status()
    
    # Usage examples
    show_usage_examples()
    
    print(f"\n📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

if __name__ == "__main__":
    main()