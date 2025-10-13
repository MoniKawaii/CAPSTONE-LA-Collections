#!/usr/bin/env python3
"""
Lazada Complete Historical Data Extraction Script
Optimized for 10,000 daily API call limit
"""

import sys
import os

# Add parent directories to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # app directory
root_dir = os.path.dirname(parent_dir)  # project root
sys.path.append(root_dir)

from app.Extraction.lazada_api_calls import LazadaDataExtractor
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def main():
    """Run complete historical data extraction"""
    print("="*60)
    print("🚀 LAZADA COMPLETE HISTORICAL DATA EXTRACTION")
    print("="*60)
    
    extractor = LazadaDataExtractor()
    
    print("📋 Extraction Menu:")
    print("1. Extract ALL historical data (products + orders from April 2020)")
    print("2. Extract recent data only (last 30 days)")
    print("3. Extract products only")
    print("4. Extract orders only (custom date range)")
    print("5. Extract order items only (requires existing orders)")
    print("6. Extract traffic metrics (2020-04-01 to 2025-04-30)")
    print("7. Extract product details (requires existing products)")
    print("8. Extract product reviews - complete process")
    print("9. Extract review history IDs only (Step 1)")
    print("10. Extract review details only (Step 2)")
    print("11. Check existing data")
    
    choice = input("\n👆 Choose option (1-11): ").strip()
    
    if choice == "1":
        print("\n🎯 FULL HISTORICAL EXTRACTION STARTING...")
        print("⚠️  This will use many API calls. Continue? (y/n)")
        confirm = input().strip().lower()
        if confirm != 'y':
            print("❌ Extraction cancelled")
            return
        
        # Extract all products
        print("\n📦 Step 1: Extracting all products...")
        products = extractor.extract_all_products(start_fresh=True)
        
        # Extract all orders (automatically chunks into 90-day periods)
        print("\n🛒 Step 2: Extracting all orders from April 2020 to April 2025...")
        print("📊 Orders will be automatically chunked into 90-day periods (API requirement)")
        orders = extractor.extract_all_orders(start_fresh=True)
        
        print(f"\n✅ EXTRACTION COMPLETE!")
        print(f"📦 Products extracted: {len(products)}")
        print(f"🛒 Orders extracted: {len(orders)}")
        print(f"📞 Total API calls used: {extractor.api_calls_made}")
        
    elif choice == "2":
        print("\n🎯 RECENT DATA EXTRACTION (30 days)...")
        
        # Extract all products
        products = extractor.extract_all_products(start_fresh=True)
        
        # Extract recent orders
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        orders = extractor.extract_all_orders(start_date, end_date, start_fresh=True)
        
        print(f"\n✅ RECENT EXTRACTION COMPLETE!")
        print(f"📦 Products: {len(products)}")
        print(f"🛒 Orders (30 days): {len(orders)}")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "3":
        print("\n📦 PRODUCTS ONLY EXTRACTION...")
        products = extractor.extract_all_products(start_fresh=True)
        print(f"✅ Products extracted: {len(products)}")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "4":
        print("\n🛒 ORDERS EXTRACTION WITH CUSTOM DATE RANGE...")
        
        # Get date range from user
        print("📅 Enter start date (YYYY-MM-DD) or press Enter for 2020-04-01:")
        start_input = input().strip()
        if start_input:
            try:
                start_date = datetime.strptime(start_input, '%Y-%m-%d')
            except ValueError:
                print("❌ Invalid date format. Using 2020-04-01")
                start_date = datetime(2020, 4, 1)
        else:
            start_date = datetime(2020, 4, 1)
        
        print("📅 Enter end date (YYYY-MM-DD) or press Enter for 2025-04-30:")
        end_input = input().strip()
        if end_input:
            try:
                end_date = datetime.strptime(end_input, '%Y-%m-%d')
            except ValueError:
                print("❌ Invalid date format. Using 2025-04-30")
                end_date = datetime(2025, 4, 30)
        else:
            end_date = datetime(2025, 4, 30)
        
        # Check if date range is large and inform user about automatic chunking
        date_diff = end_date - start_date
        if date_diff.days > 90:
            print(f"\n📊 Large date range detected ({date_diff.days} days)")
            print("✅ Orders will be automatically chunked into 90-day periods (API requirement)")
        
        orders = extractor.extract_all_orders(start_date, end_date, start_fresh=True)
        print(f"✅ Orders extracted: {len(orders)}")
        
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "5":
        print("\n📦 ORDER ITEMS EXTRACTION...")
        print("📋 This will extract detailed item information for all existing orders")
        
        # Check if orders exist
        orders = extractor.extract_all_orders(start_fresh=False)
        if not orders:
            print("❌ No orders found. Please extract orders first (option 1 or 4)")
            return
        
        print(f"📊 Found {len(orders)} orders to process")
        confirm = input("Continue with order items extraction? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Extraction cancelled")
            return
        
        order_items = extractor.extract_all_order_items(orders_data=orders, start_fresh=True)
        print(f"✅ Order items extracted: {len(order_items)}")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "6":
        print("\n📊 TRAFFIC METRICS EXTRACTION...")
        print("📅 Choose extraction mode:")
        print("1. Monthly aggregates (2022-10-01 to 2025-04-30) - Recommended")
        print("2. Single period (2020-04-01 to 2025-04-30) - Legacy")
        
        mode = input("Choose mode (1 or 2): ").strip()
        
        if mode == "1":
            print("📅 Extracting monthly traffic metrics from 2022-10-01 to 2025-04-30")
            traffic_data = extractor.extract_traffic_metrics(
                start_date='2022-10-01', 
                end_date='2025-04-30', 
                start_fresh=True,
                monthly_aggregate=True
            )
        else:
            print("📅 Extracting single period traffic metrics from 2020-04-01 to 2025-04-30")
            traffic_data = extractor.extract_traffic_metrics(
                start_date='2020-04-01', 
                end_date='2025-04-30', 
                start_fresh=True,
                monthly_aggregate=False
            )
        
        print(f"✅ Traffic metrics extracted: {len(traffic_data)} records")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "7":
        print("\n📦 PRODUCT DETAILS EXTRACTION...")
        print("📋 This will extract detailed information for all existing products")
        
        # Check if products exist
        products = extractor.extract_all_products(start_fresh=False)
        if not products:
            print("❌ No products found. Please extract products first (option 1 or 3)")
            return
        
        print(f"📊 Found {len(products)} products to process")
        confirm = input("Continue with product details extraction? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Extraction cancelled")
            return
        
        product_details = extractor.extract_product_details(start_fresh=True)
        print(f"✅ Product details extracted: {len(product_details)}")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "8":
        print("\n⭐ PRODUCT REVIEWS EXTRACTION (Complete Process)...")
        print("📋 This will run both steps:")
        print("   Step 1: Extract review IDs from /review/seller/history/list")
        print("   Step 2: Extract review details from /review/seller/list/v2")
        print("📅 Reviews are extracted in 7-day chunks (API requirement)")
        print("🔍 Each review ID batch is processed with max 10 reviews (API limit)")
        
        confirm = input("Continue with complete review extraction? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Extraction cancelled")
            return
        
        reviews = extractor.extract_product_reviews(start_fresh=True, limit_products=10)
        print(f"✅ Product reviews extracted: {len(reviews)}")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "9":
        print("\n📋 REVIEW HISTORY IDs EXTRACTION (Step 1)...")
        print("🔍 This will extract review IDs using /review/seller/history/list")
        print("💾 IDs will be saved to lazada_reviewhistorylist_raw.json")
        print("📅 Reviews are extracted in 7-day chunks (API requirement)")
        
        confirm = input("Continue with review IDs extraction? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Extraction cancelled")
            return
        
        review_ids = extractor.extract_review_history_list(start_fresh=True, limit_products=10)
        print(f"✅ Review IDs extracted: {len(review_ids)}")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "10":
        print("\n📃 REVIEW DETAILS EXTRACTION (Step 2)...")
        print("🔍 This will extract detailed reviews using /review/seller/list/v2")
        print("📋 Will use IDs from lazada_reviewhistorylist_raw.json")
        print("🔍 Each batch processes max 10 review IDs (API limit)")
        
        confirm = input("Continue with review details extraction? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Extraction cancelled")
            return
        
        reviews = extractor.extract_review_details(start_fresh=True)
        print(f"✅ Detailed reviews extracted: {len(reviews)}")
        print(f"📞 API calls used: {extractor.api_calls_made}")
        
    elif choice == "11":
        print("\n CHECKING EXISTING DATA...")
        
        # Check products
        products = extractor.extract_all_products(start_fresh=False)
        print(f"📦 Existing products: {len(products)}")
        
        # Check orders
        orders = extractor.extract_all_orders(start_fresh=False)
        print(f"🛒 Existing orders: {len(orders)}")
        
        # Check review IDs
        review_ids = extractor._load_from_json('lazada_reviewhistorylist_raw.json')
        print(f"📋 Existing review IDs: {len(review_ids)}")
        
        # Check reviews
        reviews = extractor._load_from_json('lazada_productreview_raw.json')
        print(f"⭐ Existing detailed reviews: {len(reviews)}")
        
        print(f"📞 API calls used today: {extractor.api_calls_made}")
        
    else:
        print("❌ Invalid choice")
        return
    
    print("\n🎉 All data saved to app/Staging/ directory")
    print("📁 Files created:")
    print("   - lazada_products_raw.json")
    print("   - lazada_orders_raw.json")
    print("   - lazada_multiple_order_items_raw.json")
    print("   - lazada_productitem_raw.json")
    print("   - lazada_reportoverview_raw.json")
    print("   - lazada_productreview_raw.json")
    print("\n✨ You can now run your ETL pipeline to process this data!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n❌ Extraction interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Error during extraction: {e}")
        sys.exit(1)