"""
Lazada API Data Functions
Functions for fetching data from various Lazada API endpoints
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from lazop_sdk import LazopClient, LazopRequest
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Lazada API credentials
APP_KEY = os.getenv('LAZADA_APP_KEY')
APP_SECRET = os.getenv('LAZADA_APP_SECRET')
LAZADA_API_BASE = "https://api.lazada.com.ph/rest"

def get_lazop_client():
    """Get configured LazopClient instance"""
    return LazopClient(LAZADA_API_BASE, APP_KEY, APP_SECRET)

def get_orders(access_token: str, created_after: str = None, created_before: str = None, 
              offset: int = 0, limit: int = 100) -> Dict:
    """
    Fetch orders from Lazada API
    
    Args:
        access_token: Valid access token
        created_after: Start date (ISO format)
        created_before: End date (ISO format)
        offset: Pagination offset
        limit: Number of orders to fetch (max 100)
    
    Returns:
        Dict: API response with orders data
    """
    try:
        client = get_lazop_client()
        request = LazopRequest('/orders/get', 'GET')
        
        # Set parameters
        if created_after:
            request.add_api_param('created_after', created_after)
        if created_before:
            request.add_api_param('created_before', created_before)
        request.add_api_param('offset', str(offset))
        request.add_api_param('limit', str(limit))
        
        # Execute request
        response = client.execute(request, access_token)
        
        # Parse response body  
        if isinstance(response.body, dict):
            response_data = response.body
        else:
            response_data = json.loads(response.body) if response.body else {}
        
        # Check if success (code '0' means success in Lazada API)
        if response_data.get('code') == '0':
            return {
                'success': True,
                'data': response_data.get('data', {})
            }
        else:
            return {
                'success': False,
                'error': f"API Error: {response_data.get('code')} - {response_data.get('message', 'Unknown error')}",
                'body': response.body
            }
            
    except Exception as e:
        logger.error(f"Error fetching orders: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def get_order_items(access_token: str, order_id: int) -> Dict:
    """
    Fetch order items for a specific order
    
    Args:
        access_token: Valid access token
        order_id: Order ID to fetch items for
    
    Returns:
        Dict: API response with order items data
    """
    try:
        client = get_lazop_client()
        request = LazopRequest('/order/items/get', 'GET')
        request.add_api_param('order_id', str(order_id))
        
        response = client.execute(request, access_token)
        
        # Parse response body  
        if isinstance(response.body, dict):
            response_data = response.body
        else:
            response_data = json.loads(response.body) if response.body else {}
        
        # Check if success (code '0' means success in Lazada API)
        if response_data.get('code') == '0':
            return {
                'success': True,
                'data': response_data.get('data', [])
            }
        else:
            return {
                'success': False,
                'error': f"API Error: {response_data.get('code')} - {response_data.get('message', 'Unknown error')}",
                'body': response.body
            }
            
    except Exception as e:
        logger.error(f"Error fetching order items for order {order_id}: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def get_products(access_token: str, offset: int = 0, limit: int = 50, 
                search: str = None, filter: str = 'all') -> Dict:
    """
    Fetch products from Lazada API
    
    Args:
        access_token: Valid access token
        offset: Pagination offset
        limit: Number of products to fetch (max 50 for Lazada)
        search: Search term
        filter: Product filter ('all', 'live', 'inactive', etc.)
    
    Returns:
        Dict: API response with products data
    """
    try:
        client = get_lazop_client()
        request = LazopRequest('/products/get', 'GET')
        
        request.add_api_param('offset', str(offset))
        request.add_api_param('limit', str(min(limit, 50)))  # Lazada max is 50
        request.add_api_param('filter', filter)
        
        if search:
            request.add_api_param('search', search)
        
        response = client.execute(request, access_token)
        
        # Parse response body  
        if isinstance(response.body, dict):
            response_data = response.body
        else:
            response_data = json.loads(response.body) if response.body else {}
        
        # Check if success (code '0' means success in Lazada API)
        if response_data.get('code') == '0':
            return {
                'success': True,
                'data': response_data.get('data', {})
            }
        else:
            return {
                'success': False,
                'error': f"API Error: {response_data.get('code')} - {response_data.get('message', 'Unknown error')}",
                'body': response.body
            }
            
    except Exception as e:
        logger.error(f"Error fetching products: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def get_seller_vouchers(access_token: str, voucher_type: str = 'seller') -> Dict:
    """
    Fetch seller vouchers from Lazada API
    
    Args:
        access_token: Valid access token
        voucher_type: Type of voucher ('seller', 'platform', etc.)
    
    Returns:
        Dict: API response with vouchers data
    """
    try:
        client = get_lazop_client()
        request = LazopRequest('/promotion/vouchers/get', 'GET')
        request.add_api_param('voucher_type', voucher_type)
        
        response = client.execute(request, access_token)
        
        # Parse response body  
        if isinstance(response.body, dict):
            response_data = response.body
        else:
            response_data = json.loads(response.body) if response.body else {}
        
        # Check if success (code '0' means success in Lazada API)
        if response_data.get('code') == '0':
            return {
                'success': True,
                'data': response_data.get('data', [])
            }
        else:
            return {
                'success': False,
                'error': f"API Error: {response_data.get('code')} - {response_data.get('message', 'Unknown error')}",
                'body': response.body
            }
            
    except Exception as e:
        logger.error(f"Error fetching vouchers: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def get_voucher_products(access_token: str, voucher_id: str) -> Dict:
    """
    Fetch products associated with a voucher
    
    Args:
        access_token: Valid access token
        voucher_id: Voucher ID
    
    Returns:
        Dict: API response with voucher products data
    """
    try:
        client = get_lazop_client()
        request = LazopRequest('/promotion/voucher/products/get', 'GET')
        request.add_api_param('voucher_id', voucher_id)
        
        response = client.execute(request, access_token)
        
        if response.type == 'nil':
            return {
                'success': True,
                'data': json.loads(response.body) if response.body else []
            }
        else:
            return {
                'success': False,
                'error': f"API Error: {response.code} - {response.message}",
                'body': response.body
            }
            
    except Exception as e:
        logger.error(f"Error fetching voucher products for voucher {voucher_id}: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def test_all_endpoints(access_token: str):
    """Test all API endpoints"""
    print("🧪 Testing Lazada API Endpoints...")
    
    # Test orders
    print("\n📦 Testing Orders API...")
    orders_result = get_orders(access_token, limit=5)
    print(f"   Orders: {'✅' if orders_result['success'] else '❌'}")
    if orders_result['success']:
        orders = orders_result.get('data', {}).get('orders', [])
        print(f"   Found {len(orders)} orders")
        
        # Test order items for first order
        if orders:
            first_order_id = orders[0].get('order_id')
            print(f"\n📋 Testing Order Items API for order {first_order_id}...")
            items_result = get_order_items(access_token, first_order_id)
            print(f"   Order Items: {'✅' if items_result['success'] else '❌'}")
    
    # Test products
    print(f"\n🛍️ Testing Products API...")
    products_result = get_products(access_token, limit=5)
    print(f"   Products: {'✅' if products_result['success'] else '❌'}")
    if products_result['success']:
        products = products_result.get('data', {}).get('products', [])
        print(f"   Found {len(products)} products")
    
    # Test vouchers
    print(f"\n🎫 Testing Vouchers API...")
    vouchers_result = get_seller_vouchers(access_token)
    print(f"   Vouchers: {'✅' if vouchers_result['success'] else '❌'}")
    if vouchers_result['success']:
        vouchers = vouchers_result.get('data', [])
        print(f"   Found {len(vouchers)} vouchers")


if __name__ == "__main__":
    # Test with a valid token
    from app.lazada.get_lazada_tokens import get_valid_token
    
    token = get_valid_token()
    if token:
        test_all_endpoints(token)
    else:
        print("❌ No valid token available for testing")