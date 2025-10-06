#!/usr/bin/env python3
"""
Simple Lazada ETL Test with Direct Token Usage

This script directly uses the provided access token to test the enhanced sales schema
"""

import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Tuple
from lazop_sdk import LazopClient, LazopRequest
import os
from dotenv import load_dotenv

load_dotenv()

class SimpleLazadaExtractor:
    def __init__(self):
        self.app_key = os.getenv('LAZADA_APP_KEY')
        self.app_secret = os.getenv('LAZADA_APP_SECRET')
        self.client = LazopClient('https://api.lazada.com.ph/rest', self.app_key, self.app_secret)
        
        # Use the provided access token directly
        self.access_token = "50000601410cNTkuepxEvx1d75ecf3k7ijT3LWviiIvpWFtzIGbHqsEtxTBUxSb8"
    
    async def get_orders(self) -> Dict[str, Any]:
        """Get orders data using direct API call"""
        print("🔍 Fetching orders...")
        
        try:
            request = LazopRequest('/orders/get', 'GET')
            request.add_api_param('created_after', '2024-01-01T00:00:00+08:00')
            request.add_api_param('limit', '50')
            
            response = self.client.execute(request, self.access_token)
            
            if response.code == '0':  # Response code is a string
                print(f"✅ Orders retrieved successfully: {response.body.get('data', {}).get('count', 0)} orders")
                return response.body
            else:
                print(f"❌ Failed to get orders: {response.message or 'Unknown error'}")
                return {}
                
        except Exception as e:
            print(f"❌ Error getting orders: {e}")
            return {}
    
    async def get_order_items(self, order_ids: List[str]) -> Dict[str, Any]:
        """Get order items for specific order IDs"""
        print(f"🔍 Fetching items for {len(order_ids)} orders...")
        
        try:
            request = LazopRequest('/orders/items/get', 'GET')
            request.add_api_param('order_ids', json.dumps(order_ids))
            
            response = self.client.execute(request, self.access_token)
            
            if response.code == '0':  # Response code is a string
                print(f"✅ Order items retrieved successfully")
                return response.body
            else:
                print(f"❌ Failed to get order items: {response.message}")
                return {}
                
        except Exception as e:
            print(f"❌ Error getting order items: {e}")
            return {}
    
    def extract_sales_data(self, orders_data: Dict[str, Any], items_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data for enhanced sales schema"""
        
        # Extract orders
        orders = []
        if orders_data.get('data', {}).get('orders'):
            for order in orders_data['data']['orders']:
                shipping_addr = order.get('address_shipping', {})
                
                orders.append({
                    'order_id': order.get('order_id'),
                    'order_number': order.get('order_number'),
                    'order_date': order.get('created_at'),
                    'order_status': order.get('statuses', ['unknown'])[0] if order.get('statuses') else 'unknown',
                    'customer_first_name': order.get('customer_first_name', ''),
                    'customer_last_name': order.get('customer_last_name', ''),
                    'customer_city': shipping_addr.get('city', ''),
                    'customer_country': shipping_addr.get('country', ''),
                    'payment_method': order.get('payment_method', ''),
                    'order_total_amount': float(order.get('price', 0)),
                    'shipping_fee': float(order.get('shipping_fee', 0)),
                    'voucher_total': float(order.get('voucher', 0)),
                    'items_count': order.get('items_count', 0),
                    'platform': 'Lazada'
                })
        
        # Extract order items and products
        order_items = []
        products = []
        seen_products = set()
        
        if items_data.get('data'):
            for order_data in items_data['data']:
                order_id = order_data.get('order_id')
                items = order_data.get('order_items', [])
                
                for item in items:
                    # Order item data
                    order_items.append({
                        'order_item_id': item.get('order_item_id'),
                        'order_id': order_id,
                        'product_name': item.get('name', ''),
                        'product_sku': item.get('sku', ''),
                        'sku_id': item.get('sku_id', ''),
                        'variation': item.get('variation', ''),
                        'item_price': float(item.get('item_price', 0)),
                        'paid_price': float(item.get('paid_price', 0)),
                        'item_status': item.get('status', ''),
                        'product_image_url': item.get('product_main_image', ''),
                        'shipping_amount': float(item.get('shipping_amount', 0))
                    })
                    
                    # Product data (unique products only)
                    sku_id = item.get('sku_id', '')
                    if sku_id and sku_id not in seen_products:
                        seen_products.add(sku_id)
                        products.append({
                            'sku_id': sku_id,
                            'product_name': item.get('name', ''),
                            'product_sku': item.get('sku', ''),
                            'last_price': float(item.get('item_price', 0)),
                            'product_image_url': item.get('product_main_image', ''),
                            'status': 'Active'
                        })
        
        return {
            'orders': orders,
            'order_items': order_items,
            'products': products
        }

async def main():
    """Test the enhanced sales data extraction"""
    print("🏪 SIMPLE LAZADA SALES DATA TEST")
    print("=" * 40)
    
    extractor = SimpleLazadaExtractor()
    
    # Get orders data
    orders_data = await extractor.get_orders()
    
    if not orders_data:
        print("❌ No orders data to process")
        return
    
    # Extract order IDs for items API
    order_ids = []
    if orders_data.get('data', {}).get('orders'):
        order_ids = [str(order['order_id']) for order in orders_data['data']['orders'][:10]]  # First 10 orders
    
    if not order_ids:
        print("❌ No order IDs found")
        return
    
    # Get order items
    items_data = await extractor.get_order_items(order_ids)
    
    # Transform data
    print("\n🔧 Transforming data for sales schema...")
    sales_data = extractor.extract_sales_data(orders_data, items_data)
    
    # Print results
    print(f"\n📊 EXTRACTED DATA SUMMARY:")
    print(f"  📦 Orders: {len(sales_data['orders'])}")
    print(f"  📋 Order Items: {len(sales_data['order_items'])}")
    print(f"  🛍️  Unique Products: {len(sales_data['products'])}")
    
    # Save results
    with open('simple_sales_data.json', 'w') as f:
        json.dump(sales_data, f, indent=2, default=str)
    
    print(f"\n✅ Data saved to: simple_sales_data.json")
    
    # Show samples
    if sales_data['orders']:
        print(f"\n📦 Sample Order:")
        sample_order = sales_data['orders'][0]
        print(f"  Order ID: {sample_order['order_id']}")
        print(f"  Customer: {sample_order['customer_first_name']} {sample_order['customer_last_name']}")
        print(f"  Total: ₱{sample_order['order_total_amount']}")
        print(f"  Status: {sample_order['order_status']}")
        print(f"  Items: {sample_order['items_count']}")
    
    if sales_data['order_items']:
        print(f"\n📋 Sample Order Item:")
        sample_item = sales_data['order_items'][0]
        print(f"  Product: {sample_item['product_name']}")
        print(f"  SKU: {sample_item['product_sku']}")
        print(f"  Price: ₱{sample_item['item_price']}")
        print(f"  Status: {sample_item['item_status']}")

if __name__ == "__main__":
    asyncio.run(main())