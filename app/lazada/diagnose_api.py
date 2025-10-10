"""
Lazada API Diagnostic Script
Debug actual API responses to understand data structure
"""

import json
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from app.lazada.lazada_api_functions import (
    get_orders,
    get_products,
    get_seller_vouchers
)
from app.lazada.get_lazada_tokens import get_valid_token

def diagnose_api_responses():
    """Diagnose actual API responses"""
    print("🔍 LAZADA API DIAGNOSTIC")
    print("="*60)
    
    # Get token
    token = get_valid_token()
    if not token:
        print("❌ No valid token available")
        return
    
    print("✅ Token obtained")
    print(f"   Token preview: {token[:20]}...")
    print()
    
    # Test Orders API
    print("📦 TESTING ORDERS API")
    print("-" * 40)
    
    # Try different date ranges
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # Try 3 months
    
    created_after = start_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
    created_before = end_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
    
    print(f"📅 Date range: {created_after} to {created_before}")
    
    orders_result = get_orders(
        access_token=token,
        created_after=created_after,
        created_before=created_before,
        limit=10
    )
    
    print(f"✅ Orders API Success: {orders_result['success']}")
    if not orders_result['success']:
        print(f"   Error: {orders_result['error']}")
        if 'body' in orders_result:
            print(f"   Response body: {orders_result['body']}")
    else:
        print(f"   Raw response structure: {type(orders_result['data'])}")
        print(f"   Response keys: {list(orders_result['data'].keys()) if isinstance(orders_result['data'], dict) else 'Not a dict'}")
        
        # Pretty print first few lines of response
        response_str = json.dumps(orders_result['data'], indent=2)
        lines = response_str.split('\n')[:20]  # First 20 lines
        print("   Response preview:")
        for line in lines:
            print(f"     {line}")
        if len(response_str.split('\n')) > 20:
            print("     ... (truncated)")
    
    print()
    
    # Test Products API
    print("🛍️ TESTING PRODUCTS API")
    print("-" * 40)
    
    products_result = get_products(
        access_token=token,
        limit=10
    )
    
    print(f"✅ Products API Success: {products_result['success']}")
    if not products_result['success']:
        print(f"   Error: {products_result['error']}")
        if 'body' in products_result:
            print(f"   Response body: {products_result['body']}")
    else:
        print(f"   Raw response structure: {type(products_result['data'])}")
        print(f"   Response keys: {list(products_result['data'].keys()) if isinstance(products_result['data'], dict) else 'Not a dict'}")
        
        # Pretty print first few lines
        response_str = json.dumps(products_result['data'], indent=2)
        lines = response_str.split('\n')[:20]
        print("   Response preview:")
        for line in lines:
            print(f"     {line}")
        if len(response_str.split('\n')) > 20:
            print("     ... (truncated)")
    
    print()
    
    # Test Vouchers API
    print("🎫 TESTING VOUCHERS API")
    print("-" * 40)
    
    vouchers_result = get_seller_vouchers(access_token=token)
    
    print(f"✅ Vouchers API Success: {vouchers_result['success']}")
    if not vouchers_result['success']:
        print(f"   Error: {vouchers_result['error']}")
        if 'body' in vouchers_result:
            print(f"   Response body: {vouchers_result['body']}")
    else:
        print(f"   Raw response structure: {type(vouchers_result['data'])}")
        if isinstance(vouchers_result['data'], list):
            print(f"   Number of vouchers: {len(vouchers_result['data'])}")
        
        # Pretty print response
        response_str = json.dumps(vouchers_result['data'], indent=2)
        lines = response_str.split('\n')[:20]
        print("   Response preview:")
        for line in lines:
            print(f"     {line}")
        if len(response_str.split('\n')) > 20:
            print("     ... (truncated)")
    
    print()
    print("="*60)
    print("🎯 DIAGNOSTIC COMPLETE")
    print()
    print("📝 RECOMMENDATIONS:")
    print("   • If APIs return success but no data, your account may not have:")
    print("     - Recent orders (try extending date range)")
    print("     - Listed products")
    print("     - Active vouchers")
    print("   • If APIs fail, check:")
    print("     - Token validity")
    print("     - API permissions")
    print("     - Rate limiting")
    print("="*60)

if __name__ == "__main__":
    diagnose_api_responses()