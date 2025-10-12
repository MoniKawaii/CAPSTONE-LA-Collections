"""
Test Script for Shopee API Integration

This script demonstrates the usage of the shopee_api_calls module
to interact with the Shopee API for data extraction.

Usage:
    python test_shopee_api.py
"""

import os
import json
import logging
from datetime import datetime, timedelta
import time

from app.Extraction.shopee_api_calls import (
    test_api_connection,
    get_shop_info,
    get_orders,
    get_all_orders,
    get_order_details,
    get_products,
    get_all_products,
    get_product_details,
    get_traffic,
    standardize_order_data,
    standardize_product_data
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_shop_info():
    """Test fetching shop information"""
    logger.info("Testing shop info retrieval...")
    
    try:
        shop_info = get_shop_info()
        print(f"\n=== Shop Information ===")
        print(json.dumps(shop_info, indent=2))
        return True
    except Exception as e:
        logger.error(f"Error getting shop info: {e}")
        return False

def test_get_orders():
    """Test fetching orders"""
    logger.info("Testing order retrieval...")
    
    try:
        # Get orders from the last 30 days
        time_from = int((datetime.now() - timedelta(days=30)).timestamp())
        time_to = int(time.time())
        
        orders = get_orders(time_from=time_from, time_to=time_to, page_size=10)
        print(f"\n=== Recent Orders (First 10) ===")
        print(json.dumps(orders, indent=2))
        
        # If orders exist, get details for the first order
        if "response" in orders and "order_list" in orders["response"] and orders["response"]["order_list"]:
            first_order = orders["response"]["order_list"][0]
            order_sn = first_order["order_sn"]
            
            print(f"\n=== Order Details for {order_sn} ===")
            order_details = get_order_details([order_sn])
            print(json.dumps(order_details, indent=2))
            
            # Show standardized order data
            print(f"\n=== Standardized Order Data ===")
            std_order = standardize_order_data(first_order)
            print(json.dumps(std_order, indent=2))
        
        return True
    except Exception as e:
        logger.error(f"Error testing orders: {e}")
        return False

def test_get_products():
    """Test fetching products"""
    logger.info("Testing product retrieval...")
    
    try:
        products = get_products(page_size=10)
        print(f"\n=== Products (First 10) ===")
        print(json.dumps(products, indent=2))
        
        # If products exist, get details for the first product
        if "response" in products and "item" in products["response"] and products["response"]["item"]:
            first_product = products["response"]["item"][0]
            item_id = first_product["item_id"]
            
            print(f"\n=== Product Details for Item {item_id} ===")
            product_details = get_product_details([item_id])
            print(json.dumps(product_details, indent=2))
            
            # Show standardized product data
            print(f"\n=== Standardized Product Data ===")
            std_product = standardize_product_data(first_product)
            print(json.dumps(std_product, indent=2))
        
        return True
    except Exception as e:
        logger.error(f"Error testing products: {e}")
        return False

def test_get_traffic():
    """Test fetching traffic data"""
    logger.info("Testing traffic data retrieval...")
    
    try:
        # Get traffic data for the last 7 days
        start_time = int((datetime.now() - timedelta(days=7)).timestamp())
        end_time = int(time.time())
        
        traffic_data = get_traffic(start_time=start_time, end_time=end_time)
        print(f"\n=== Traffic Data (Last 7 Days) ===")
        print(json.dumps(traffic_data, indent=2))
        
        return True
    except Exception as e:
        logger.error(f"Error testing traffic data: {e}")
        return False

def main():
    """Run all tests"""
    print("===== Testing Shopee API Integration =====\n")
    
    # First test API connection
    print("Testing API connection...")
    if test_api_connection():
        print("✅ API connection successful!\n")
        
        # Run all tests
        tests = [
            ("Shop Information", test_shop_info),
            ("Orders", test_get_orders),
            ("Products", test_get_products),
            ("Traffic", test_get_traffic)
        ]
        
        for test_name, test_func in tests:
            print(f"\n----- Testing {test_name} -----")
            if test_func():
                print(f"✅ {test_name} test passed!")
            else:
                print(f"❌ {test_name} test failed!")
    else:
        print("❌ API connection failed! Please check your credentials.")

if __name__ == "__main__":
    main()