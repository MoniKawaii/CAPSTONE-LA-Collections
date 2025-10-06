#!/usr/bin/env python3
"""
Enhanced Lazada Sales ETL Extractor

This script extracts orders and order items data from Lazada API
and transforms it into a more intuitive sales-focused schema
"""

import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Tuple
from app.lazada_service import LazadaOAuthService
from lazop_sdk import LazopClient, LazopRequest
import os
from dotenv import load_dotenv

load_dotenv()

class EnhancedLazadaSalesExtractor:
    def __init__(self):
        self.lazada_service = LazadaOAuthService()
        # Setup direct client for order items API
        self.app_key = os.getenv('LAZADA_APP_KEY')
        self.app_secret = os.getenv('LAZADA_APP_SECRET')
        self.client = LazopClient('https://api.lazada.com.ph/rest', self.app_key, self.app_secret)
        
    def load_tokens(self):
        """Load tokens from JSON file"""
        with open('lazada_tokens.json', 'r') as f:
            return json.load(f)
    
    async def get_orders_and_items(self, days_back: int = 30) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get both orders and order items data"""
        print("🔍 Fetching orders data...")
        
        # Get orders data
        orders_result = await self.lazada_service.get_orders()
        if not orders_result or 'data' not in orders_result:
            print("❌ No orders data found")
            return {}, {}
        
        orders = orders_result['data'].get('orders', [])
        print(f"📦 Found {len(orders)} orders")
        
        # Get order items for each order (in batches)
        print("🔍 Fetching order items data...")
        order_items_result = await self.get_order_items_batch(orders)
        
        return orders_result, order_items_result
    
    async def get_order_items_batch(self, orders: List[Dict[str, Any]], batch_size: int = 10) -> Dict[str, Any]:
        """Get order items in batches to avoid API limits"""
        all_items = []
        tokens = self.load_tokens()
        access_token = tokens['access_token']
        
        # Process orders in batches
        for i in range(0, len(orders), batch_size):
            batch = orders[i:i + batch_size]
            order_ids = [str(order['order_id']) for order in batch]
            
            try:
                print(f"  📋 Processing batch {i//batch_size + 1} ({len(order_ids)} orders)...")
                
                request = LazopRequest('/orders/items/get', 'GET')
                request.add_api_param('order_ids', json.dumps(order_ids))
                
                response = self.client.execute(request, access_token)
                
                if response.code == 0 and response.body.get('data'):
                    batch_items = response.body['data']
                    all_items.extend(batch_items)
                    print(f"    ✅ Retrieved items for {len(batch_items)} orders")
                else:
                    print(f"    ❌ Failed to get items for batch: {response.message}")
                    
            except Exception as e:
                print(f"    ❌ Error getting items for batch: {e}")
                continue
        
        return {'data': all_items, 'count': len(all_items)}
    
    def extract_orders_data(self, orders_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract and transform orders data for the Orders table"""
        if not orders_data or 'data' not in orders_data:
            return []
        
        orders = orders_data['data'].get('orders', [])
        extracted_orders = []
        
        for order in orders:
            # Extract shipping address
            shipping_addr = order.get('address_shipping', {})
            billing_addr = order.get('address_billing', {})
            
            extracted_order = {
                'order_id': order.get('order_id'),
                'order_number': order.get('order_number'),
                'order_date': self._parse_date(order.get('created_at')),
                'order_status': order.get('statuses', ['unknown'])[0] if order.get('statuses') else 'unknown',
                'customer_first_name': order.get('customer_first_name', ''),
                'customer_last_name': order.get('customer_last_name', ''),
                'customer_city': shipping_addr.get('city', ''),
                'customer_country': shipping_addr.get('country', ''),
                'customer_phone': shipping_addr.get('phone', ''),
                'payment_method': order.get('payment_method', ''),
                'shipping_address': self._format_address(shipping_addr),
                'billing_address': self._format_address(billing_addr),
                'order_total_amount': float(order.get('price', 0)),
                'shipping_fee': float(order.get('shipping_fee', 0)),
                'voucher_total': float(order.get('voucher', 0)) + float(order.get('voucher_seller', 0)) + float(order.get('voucher_platform', 0)),
                'voucher_seller': float(order.get('voucher_seller', 0)),
                'voucher_platform': float(order.get('voucher_platform', 0)),
                'items_count': order.get('items_count', 0),
                'warehouse_code': order.get('warehouse_code', ''),
                'created_at': self._parse_date(order.get('created_at')),
                'updated_at': self._parse_date(order.get('updated_at')),
                'platform': 'Lazada',
                'platform_region': 'Philippines'
            }
            extracted_orders.append(extracted_order)
        
        return extracted_orders
    
    def extract_order_items_data(self, order_items_data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract and transform order items data for Order_Items and Products tables"""
        if not order_items_data or 'data' not in order_items_data:
            return [], []
        
        order_items_list = []
        products_list = []
        seen_products = set()
        
        for order_data in order_items_data['data']:
            order_id = order_data.get('order_id')
            order_number = order_data.get('order_number')
            items = order_data.get('order_items', [])
            
            for item in items:
                # Extract order item data
                order_item = {
                    'order_item_id': item.get('order_item_id'),
                    'order_id': order_id,
                    'order_number': order_number,
                    'product_name': item.get('name', ''),
                    'product_sku': item.get('sku', ''),
                    'shop_sku': item.get('shop_sku', ''),
                    'sku_id': item.get('sku_id', ''),
                    'variation': item.get('variation', ''),
                    'item_price': float(item.get('item_price', 0)),
                    'paid_price': float(item.get('paid_price', 0)),
                    'quantity': 1,  # Lazada API doesn't provide quantity directly
                    'item_status': item.get('status', ''),
                    'cancellation_reason': item.get('reason', ''),
                    'product_image_url': item.get('product_main_image', ''),
                    'product_detail_url': item.get('product_detail_url', ''),
                    'shipping_type': item.get('shipping_type', ''),
                    'shipping_amount': float(item.get('shipping_amount', 0)),
                    'voucher_amount': float(item.get('voucher_amount', 0)),
                    'tax_amount': float(item.get('tax_amount', 0)),
                    'tracking_code': item.get('tracking_code', ''),
                    'warehouse_code': item.get('warehouse_code', ''),
                    'created_at': self._parse_date(item.get('created_at')),
                    'updated_at': self._parse_date(item.get('updated_at'))
                }
                order_items_list.append(order_item)
                
                # Extract product data (unique products only)
                sku_id = item.get('sku_id', '')
                if sku_id and sku_id not in seen_products:
                    seen_products.add(sku_id)
                    product = {
                        'sku_id': sku_id,
                        'product_name': item.get('name', ''),
                        'product_sku': item.get('sku', ''),
                        'shop_sku': item.get('shop_sku', ''),
                        'category': 'General',  # Could be enhanced with category API
                        'last_price': float(item.get('item_price', 0)),
                        'product_image_url': item.get('product_main_image', ''),
                        'product_detail_url': item.get('product_detail_url', ''),
                        'total_sales_quantity': 0,  # Will be calculated
                        'total_sales_amount': 0,   # Will be calculated
                        'average_selling_price': float(item.get('item_price', 0)),
                        'first_sold_date': self._parse_date(item.get('created_at')),
                        'last_sold_date': self._parse_date(item.get('created_at')),
                        'status': 'Active'
                    }
                    products_list.append(product)
        
        return order_items_list, products_list
    
    def extract_customers_data(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract and transform customer data"""
        customers_dict = {}
        
        for order in orders:
            customer_id = f"{order['customer_first_name']}_{order['customer_city']}"
            
            if customer_id not in customers_dict:
                customers_dict[customer_id] = {
                    'customer_id': customer_id,
                    'first_name': order['customer_first_name'],
                    'last_name': order['customer_last_name'],
                    'city': order['customer_city'],
                    'country': order['customer_country'],
                    'phone': order['customer_phone'],
                    'total_orders': 0,
                    'total_spent': 0,
                    'first_order_date': order['order_date'],
                    'last_order_date': order['order_date'],
                    'customer_segment': 'Regular',
                    'platform': 'Lazada'
                }
            
            # Update customer stats
            customer = customers_dict[customer_id]
            customer['total_orders'] += 1
            customer['total_spent'] += order['order_total_amount']
            
            if order['order_date'] < customer['first_order_date']:
                customer['first_order_date'] = order['order_date']
            if order['order_date'] > customer['last_order_date']:
                customer['last_order_date'] = order['order_date']
        
        # Calculate average order value and set customer segment
        for customer in customers_dict.values():
            customer['average_order_value'] = customer['total_spent'] / customer['total_orders']
            
            # Set customer segment based on order count
            if customer['total_orders'] >= 5:
                customer['customer_segment'] = 'VIP'
            elif customer['total_orders'] >= 3:
                customer['customer_segment'] = 'Regular'
            else:
                customer['customer_segment'] = 'New'
        
        return list(customers_dict.values())
    
    def generate_sales_summary(self, orders: List[Dict[str, Any]], order_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate daily sales summary data"""
        daily_stats = {}
        
        for order in orders:
            date_key = order['order_date'].date() if isinstance(order['order_date'], datetime) else order['order_date']
            
            if date_key not in daily_stats:
                daily_stats[date_key] = {
                    'date': date_key,
                    'platform': 'Lazada',
                    'total_orders': 0,
                    'total_items_sold': 0,
                    'total_revenue': 0,
                    'total_shipping': 0,
                    'total_vouchers': 0,
                    'unique_customers': set(),
                    'canceled_orders': 0,
                    'canceled_revenue': 0,
                    'ready_to_ship_orders': 0,
                    'shipped_orders': 0,
                    'delivered_orders': 0
                }
            
            stats = daily_stats[date_key]
            stats['total_orders'] += 1
            stats['total_items_sold'] += order['items_count']
            stats['total_revenue'] += order['order_total_amount']
            stats['total_shipping'] += order['shipping_fee']
            stats['total_vouchers'] += order['voucher_total']
            stats['unique_customers'].add(f"{order['customer_first_name']}_{order['customer_city']}")
            
            # Count by status
            if order['order_status'] == 'canceled':
                stats['canceled_orders'] += 1
                stats['canceled_revenue'] += order['order_total_amount']
            elif order['order_status'] == 'ready_to_ship':
                stats['ready_to_ship_orders'] += 1
            elif order['order_status'] == 'shipped':
                stats['shipped_orders'] += 1
            elif order['order_status'] == 'delivered':
                stats['delivered_orders'] += 1
        
        # Convert sets to counts and calculate averages
        summary_list = []
        for stats in daily_stats.values():
            stats['unique_customers'] = len(stats['unique_customers'])
            stats['average_order_value'] = stats['total_revenue'] / stats['total_orders'] if stats['total_orders'] > 0 else 0
            summary_list.append(stats)
        
        return summary_list
    
    def _parse_date(self, date_string: str) -> datetime:
        """Parse Lazada date format to datetime object"""
        if not date_string:
            return None
        try:
            # Lazada format: "2025-09-10 10:26:29 +0800"
            date_part = date_string.split(' +')[0]  # Remove timezone
            return datetime.strptime(date_part, '%Y-%m-%d %H:%M:%S')
        except Exception:
            return None
    
    def _format_address(self, address: Dict[str, Any]) -> str:
        """Format address dictionary into a readable string"""
        if not address:
            return ""
        
        parts = []
        for key in ['address1', 'address2', 'address3', 'city', 'country']:
            if address.get(key):
                parts.append(str(address[key]))
        
        return ", ".join(parts)
    
    async def extract_complete_sales_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Main method to extract complete sales data"""
        print("🏪 ENHANCED LAZADA SALES DATA EXTRACTION")
        print("=" * 50)
        
        # Get raw data
        orders_data, order_items_data = await self.get_orders_and_items()
        
        if not orders_data:
            print("❌ No data to process")
            return {}
        
        print("\n🔧 Transforming data for sales schema...")
        
        # Extract data for each table
        orders = self.extract_orders_data(orders_data)
        order_items, products = self.extract_order_items_data(order_items_data)
        customers = self.extract_customers_data(orders)
        sales_summary = self.generate_sales_summary(orders, order_items)
        
        # Print summary
        print(f"\n📊 EXTRACTION SUMMARY:")
        print(f"  📦 Orders: {len(orders)}")
        print(f"  📋 Order Items: {len(order_items)}")
        print(f"  🛍️  Products: {len(products)}")
        print(f"  👥 Customers: {len(customers)}")
        print(f"  📈 Daily Summaries: {len(sales_summary)}")
        
        return {
            'orders': orders,
            'order_items': order_items,
            'products': products,
            'customers': customers,
            'sales_summary': sales_summary
        }

async def main():
    """Main function to run the enhanced sales ETL extraction"""
    extractor = EnhancedLazadaSalesExtractor()
    
    try:
        # Extract complete sales data
        sales_data = await extractor.extract_complete_sales_data()
        
        if sales_data:
            # Save to JSON file for review
            output_file = 'enhanced_lazada_sales_data.json'
            with open(output_file, 'w') as f:
                json.dump(sales_data, f, indent=2, default=str)
            
            print(f"\n✅ Enhanced sales data saved to: {output_file}")
            
            # Show sample data from each table
            print(f"\n📋 SAMPLE DATA:")
            
            if sales_data.get('orders'):
                print(f"\n📦 Sample Order:")
                sample_order = sales_data['orders'][0]
                for key, value in sample_order.items():
                    print(f"  {key}: {value}")
            
            if sales_data.get('order_items'):
                print(f"\n📋 Sample Order Item:")
                sample_item = sales_data['order_items'][0]
                for key, value in list(sample_item.items())[:10]:  # First 10 fields
                    print(f"  {key}: {value}")
                    
        else:
            print("❌ No sales data extracted")
            
    except Exception as e:
        print(f"❌ ETL Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())