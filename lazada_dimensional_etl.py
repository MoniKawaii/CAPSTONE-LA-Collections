#!/usr/bin/env python3
"""
Enhanced Lazada Dimensional ETL Extractor

This script extracts data from multiple Lazada APIs including:
- Orders and Order Items
- Vouchers and Voucher Products
- Transforms data into dimensional model with fact and dimension tables
- Implements proper price calculations per Lazada formula
"""

import json
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from lazop_sdk import LazopClient, LazopRequest
import os
from dotenv import load_dotenv

load_dotenv()

class LazadaDimensionalETL:
    def __init__(self):
        self.app_key = os.getenv('LAZADA_APP_KEY')
        self.app_secret = os.getenv('LAZADA_APP_SECRET')
        self.client = LazopClient('https://api.lazada.com.ph/rest', self.app_key, self.app_secret)
        
        # Use the provided access token directly
        self.access_token = "50000601410cNTkuepxEvx1d75ecf3k7ijT3LWviiIvpWFtzIGbHqsEtxTBUxSb8"
        
    async def get_orders(self, days_back: int = 90) -> Dict[str, Any]:
        """Get orders data"""
        print("🔍 Fetching orders...")
        
        try:
            request = LazopRequest('/orders/get', 'GET')
            request.add_api_param('created_after', '2024-01-01T00:00:00+08:00')
            request.add_api_param('limit', '100')
            
            response = self.client.execute(request, self.access_token)
            
            if response.code == '0':
                print(f"✅ Orders retrieved: {response.body.get('data', {}).get('count', 0)} orders")
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
            
            if response.code == '0':
                print(f"✅ Order items retrieved successfully")
                return response.body
            else:
                print(f"❌ Failed to get order items: {response.message}")
                return {}
                
        except Exception as e:
            print(f"❌ Error getting order items: {e}")
            return {}
    
    async def get_vouchers(self) -> Dict[str, Any]:
        """Get voucher data from /promotion/vouchers/get"""
        print("🎫 Fetching vouchers...")
        
        try:
            request = LazopRequest('/promotion/vouchers/get', 'GET')
            request.add_api_param('voucher_type', 'all')  # Required parameter
            request.add_api_param('limit', '100')
            
            response = self.client.execute(request, self.access_token)
            
            if response.code == '0':
                voucher_count = len(response.body.get('data', {}).get('vouchers', []))
                print(f"✅ Vouchers retrieved: {voucher_count} vouchers")
                return response.body
            else:
                print(f"❌ Failed to get vouchers: {response.message or 'Unknown error'}")
                return {}
                
        except Exception as e:
            print(f"❌ Error getting vouchers: {e}")
            return {}
    
    async def get_voucher_products(self, voucher_id: str) -> Dict[str, Any]:
        """Get voucher products from /promotion/voucher/products/get"""
        try:
            request = LazopRequest('/promotion/voucher/products/get', 'GET')
            request.add_api_param('voucher_id', voucher_id)
            
            response = self.client.execute(request, self.access_token)
            
            if response.code == '0':
                return response.body
            else:
                print(f"❌ Failed to get voucher products for {voucher_id}: {response.message}")
                return {}
                
        except Exception as e:
            print(f"❌ Error getting voucher products for {voucher_id}: {e}")
            return {}
    
    def generate_time_dimension(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Generate time dimension data"""
        print("📅 Generating time dimension...")
        
        time_data = []
        current_date = start_date
        
        while current_date <= end_date:
            time_key = int(current_date.strftime('%Y%m%d'))
            
            # Determine season
            month = current_date.month
            if month in [12, 1, 2]:
                season = 'Winter'
            elif month in [3, 4, 5]:
                season = 'Spring'
            elif month in [6, 7, 8]:
                season = 'Summer'
            else:
                season = 'Fall'
            
            time_record = {
                'time_key': time_key,
                'date': current_date.date(),
                'year': current_date.year,
                'quarter': (current_date.month - 1) // 3 + 1,
                'month': current_date.month,
                'month_name': current_date.strftime('%B'),
                'week': current_date.isocalendar()[1],
                'day_of_month': current_date.day,
                'day_of_week': current_date.weekday() + 1,  # 1 = Monday
                'day_name': current_date.strftime('%A'),
                'is_weekend': current_date.weekday() >= 5,
                'is_holiday': False,  # Can be enhanced with holiday data
                'fiscal_year': current_date.year,
                'fiscal_quarter': (current_date.month - 1) // 3 + 1,
                'season': season
            }
            
            time_data.append(time_record)
            current_date += timedelta(days=1)
        
        print(f"✅ Generated {len(time_data)} time dimension records")
        return time_data
    
    def extract_customer_dimension(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract customer dimension data"""
        print("👥 Extracting customer dimension...")
        
        customers_dict = {}
        
        for order in orders:
            # Create customer ID from available data
            customer_id = f"{order.get('customer_first_name', 'Unknown')}_{order.get('address_shipping', {}).get('city', 'Unknown')}"
            
            if customer_id not in customers_dict:
                shipping_addr = order.get('address_shipping', {})
                customers_dict[customer_id] = {
                    'customer_id': customer_id,
                    'first_name': order.get('customer_first_name', ''),
                    'last_name': order.get('customer_last_name', ''),
                    'full_name': f"{order.get('customer_first_name', '')} {order.get('customer_last_name', '')}".strip(),
                    'city': shipping_addr.get('city', ''),
                    'region': shipping_addr.get('address4', ''),
                    'country': shipping_addr.get('country', ''),
                    'phone': shipping_addr.get('phone', ''),
                    'total_orders': 0,
                    'total_spent': 0,
                    'first_order_date': order.get('created_at'),
                    'last_order_date': order.get('created_at'),
                    'customer_segment': 'New',
                    'platform': 'Lazada',
                    'is_active': True
                }
            
            # Update customer metrics
            customer = customers_dict[customer_id]
            customer['total_orders'] += 1
            
            # Calculate buyer paid price: price - voucher + shipping_fee
            order_price = float(order.get('price', 0))
            voucher_total = (float(order.get('voucher', 0)) + 
                           float(order.get('voucher_seller', 0)) + 
                           float(order.get('voucher_platform', 0)))
            shipping_fee = float(order.get('shipping_fee', 0))
            buyer_paid = order_price - voucher_total + shipping_fee
            
            customer['total_spent'] += buyer_paid
            
            # Update date ranges
            order_date = order.get('created_at')
            if order_date < customer['first_order_date']:
                customer['first_order_date'] = order_date
            if order_date > customer['last_order_date']:
                customer['last_order_date'] = order_date
        
        # Calculate derived metrics
        for customer in customers_dict.values():
            customer['average_order_value'] = customer['total_spent'] / customer['total_orders']
            customer['customer_lifetime_value'] = customer['total_spent']
            
            # Segment customers
            if customer['total_orders'] >= 10:
                customer['customer_segment'] = 'VIP'
            elif customer['total_orders'] >= 3:
                customer['customer_segment'] = 'Regular'
            else:
                customer['customer_segment'] = 'New'
        
        customers_list = list(customers_dict.values())
        print(f"✅ Extracted {len(customers_list)} customers")
        return customers_list
    
    def extract_product_dimension(self, order_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract product dimension data"""
        print("🛍️ Extracting product dimension...")
        
        products_dict = {}
        
        for order_data in order_items:
            items = order_data.get('order_items', [])
            
            for item in items:
                sku_id = item.get('sku_id', '')
                if not sku_id or sku_id in products_dict:
                    continue
                
                # Parse variation for color/size
                variation = item.get('variation', '')
                color_family = ''
                size = ''
                if variation:
                    parts = variation.split(', ')
                    for part in parts:
                        if 'Color' in part:
                            color_family = part.split(':')[1] if ':' in part else ''
                        elif 'Size' in part:
                            size = part.split(':')[1] if ':' in part else ''
                
                product = {
                    'sku_id': sku_id,
                    'product_name': item.get('name', ''),
                    'product_sku': item.get('sku', ''),
                    'shop_sku': item.get('shop_sku', ''),
                    'category': 'General',  # Would need product API for actual category
                    'subcategory': '',
                    'brand': '',
                    'last_price': float(item.get('item_price', 0)),
                    'cost_price': None,
                    'margin_percentage': None,
                    'product_image_url': item.get('product_main_image', ''),
                    'product_detail_url': item.get('product_detail_url', ''),
                    'color_family': color_family,
                    'size': size,
                    'total_sales_quantity': 0,
                    'total_sales_amount': 0,
                    'average_selling_price': float(item.get('item_price', 0)),
                    'first_sold_date': item.get('created_at'),
                    'last_sold_date': item.get('created_at'),
                    'inventory_status': 'Active'
                }
                
                products_dict[sku_id] = product
        
        products_list = list(products_dict.values())
        print(f"✅ Extracted {len(products_list)} products")
        return products_list
    
    def extract_voucher_dimension(self, vouchers_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract voucher dimension data"""
        print("🎫 Extracting voucher dimension...")
        
        vouchers = vouchers_data.get('data', {}).get('vouchers', [])
        voucher_list = []
        
        for voucher in vouchers:
            voucher_record = {
                'voucher_id': voucher.get('voucher_id', ''),
                'voucher_code': voucher.get('voucher_code', ''),
                'voucher_name': voucher.get('voucher_name', ''),
                'voucher_type': voucher.get('type', ''),
                'discount_type': voucher.get('discount_type', ''),
                'discount_value': float(voucher.get('discount_value', 0)),
                'discount_percentage': float(voucher.get('discount_percentage', 0)),
                'minimum_spend': float(voucher.get('minimum_spend', 0)),
                'maximum_discount': float(voucher.get('maximum_discount', 0)),
                'usage_limit': voucher.get('usage_limit', 0),
                'usage_limit_per_customer': voucher.get('usage_limit_per_customer', 0),
                'start_date': voucher.get('start_time', ''),
                'end_date': voucher.get('end_time', ''),
                'target_audience': voucher.get('target_audience', ''),
                'status': voucher.get('status', 'Active'),
                'created_by': voucher.get('created_by', '')
            }
            voucher_list.append(voucher_record)
        
        print(f"✅ Extracted {len(voucher_list)} vouchers")
        return voucher_list
    
    def calculate_fact_sales(self, orders: List[Dict[str, Any]], order_items: List[Dict[str, Any]], 
                           customers: List[Dict[str, Any]], products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate fact sales data with proper price calculations"""
        print("📊 Calculating fact sales...")
        
        # Create lookup dictionaries
        customer_lookup = {c['customer_id']: i for i, c in enumerate(customers)}
        product_lookup = {p['sku_id']: i for i, p in enumerate(products)}
        
        fact_sales = []
        
        for order_data in order_items:
            order_id = order_data.get('order_id')
            items = order_data.get('order_items', [])
            
            # Find corresponding order
            order_info = None
            for order in orders:
                if order.get('order_id') == order_id:
                    order_info = order
                    break
            
            if not order_info:
                continue
            
            for item in items:
                # Get keys for dimensions
                customer_id = f"{order_info.get('customer_first_name', 'Unknown')}_{order_info.get('address_shipping', {}).get('city', 'Unknown')}"
                customer_key = customer_lookup.get(customer_id, 0) + 1  # 1-based indexing
                product_key = product_lookup.get(item.get('sku_id', ''), 0) + 1  # 1-based indexing
                
                # Time key
                order_date = datetime.strptime(order_info.get('created_at', '').split(' +')[0], '%Y-%m-%d %H:%M:%S')
                time_key = int(order_date.strftime('%Y%m%d'))
                
                # Price calculations
                unit_price = float(item.get('item_price', 0))
                quantity = 1  # Lazada doesn't provide quantity directly
                gross_sales = unit_price * quantity
                discount_amount = float(item.get('voucher_amount', 0))
                net_sales = float(item.get('paid_price', gross_sales - discount_amount))
                shipping_revenue = float(item.get('shipping_amount', 0))
                
                # Status flags
                is_cancelled = 'cancel' in item.get('status', '').lower()
                
                fact_record = {
                    'time_key': time_key,
                    'customer_key': customer_key,
                    'product_key': product_key,
                    'order_key': order_id,  # Using order_id as order_key for simplicity
                    'voucher_key': None,  # Would need voucher mapping
                    'quantity_sold': quantity,
                    'gross_sales_amount': gross_sales,
                    'discount_amount': discount_amount,
                    'net_sales_amount': net_sales,
                    'shipping_revenue': shipping_revenue,
                    'tax_amount': float(item.get('tax_amount', 0)),
                    'unit_price': unit_price,
                    'is_cancelled': is_cancelled,
                    'is_returned': False,
                    'is_voucher_used': discount_amount > 0
                }
                
                fact_sales.append(fact_record)
        
        print(f"✅ Generated {len(fact_sales)} fact sales records")
        return fact_sales
    
    async def extract_complete_dimensional_data(self) -> Dict[str, Any]:
        """Main method to extract complete dimensional data"""
        print("🏪 LAZADA DIMENSIONAL ETL EXTRACTION")
        print("=" * 50)
        
        # Get raw data
        orders_data = await self.get_orders()
        vouchers_data = await self.get_vouchers()
        
        if not orders_data:
            print("❌ No orders data to process")
            return {}
        
        # Get order items
        orders = orders_data.get('data', {}).get('orders', [])
        order_ids = [str(order['order_id']) for order in orders[:20]]  # Limit for testing
        order_items_data = await self.get_order_items(order_ids)
        
        if not order_items_data:
            print("❌ No order items data to process")
            return {}
        
        order_items = order_items_data.get('data', [])
        
        print("\n🔧 Transforming data for dimensional model...")
        
        # Generate dimensions
        time_dimension = self.generate_time_dimension(
            datetime(2024, 1, 1), 
            datetime(2025, 12, 31)
        )
        
        customers = self.extract_customer_dimension(orders)
        products = self.extract_product_dimension(order_items)
        vouchers = self.extract_voucher_dimension(vouchers_data)
        
        # Generate fact table
        fact_sales = self.calculate_fact_sales(orders, order_items, customers, products)
        
        # Generate summary data
        print("\n📊 DIMENSIONAL MODEL SUMMARY:")
        print(f"  📅 Time Dimension: {len(time_dimension)} records")
        print(f"  👥 Customer Dimension: {len(customers)} records")
        print(f"  🛍️  Product Dimension: {len(products)} records")
        print(f"  🎫 Voucher Dimension: {len(vouchers)} records")
        print(f"  📈 Fact Sales: {len(fact_sales)} records")
        print(f"  📦 Source Orders: {len(orders)} records")
        print(f"  📋 Source Order Items: {len(order_items)} records")
        
        return {
            'dimensions': {
                'time': time_dimension,
                'customers': customers,
                'products': products,
                'vouchers': vouchers
            },
            'facts': {
                'sales': fact_sales
            },
            'source_data': {
                'orders': orders,
                'order_items': order_items
            }
        }

async def main():
    """Main function to run the dimensional ETL extraction"""
    extractor = LazadaDimensionalETL()
    
    try:
        # Extract complete dimensional data
        dimensional_data = await extractor.extract_complete_dimensional_data()
        
        if dimensional_data:
            # Save to JSON file for review
            output_file = 'lazada_dimensional_data.json'
            with open(output_file, 'w') as f:
                json.dump(dimensional_data, f, indent=2, default=str)
            
            print(f"\n✅ Dimensional data saved to: {output_file}")
            
            # Show sample records
            if dimensional_data.get('dimensions', {}).get('customers'):
                print(f"\n👥 Sample Customer:")
                sample_customer = dimensional_data['dimensions']['customers'][0]
                print(f"  Customer ID: {sample_customer['customer_id']}")
                print(f"  Name: {sample_customer['full_name']}")
                print(f"  Segment: {sample_customer['customer_segment']}")
                print(f"  Total Orders: {sample_customer['total_orders']}")
                print(f"  Total Spent: ₱{sample_customer['total_spent']:.2f}")
                print(f"  AOV: ₱{sample_customer['average_order_value']:.2f}")
            
            if dimensional_data.get('facts', {}).get('sales'):
                print(f"\n📊 Sample Fact Sales Record:")
                sample_fact = dimensional_data['facts']['sales'][0]
                print(f"  Time Key: {sample_fact['time_key']}")
                print(f"  Gross Sales: ₱{sample_fact['gross_sales_amount']:.2f}")
                print(f"  Discount: ₱{sample_fact['discount_amount']:.2f}")
                print(f"  Net Sales: ₱{sample_fact['net_sales_amount']:.2f}")
                print(f"  Cancelled: {sample_fact['is_cancelled']}")
            
            # Calculate total metrics
            total_gross = sum(f['gross_sales_amount'] for f in dimensional_data['facts']['sales'])
            total_net = sum(f['net_sales_amount'] for f in dimensional_data['facts']['sales'])
            total_discount = sum(f['discount_amount'] for f in dimensional_data['facts']['sales'])
            
            print(f"\n💰 FINANCIAL SUMMARY:")
            print(f"  Gross Revenue: ₱{total_gross:.2f}")
            print(f"  Total Discounts: ₱{total_discount:.2f}")
            print(f"  Net Revenue: ₱{total_net:.2f}")
            print(f"  Discount Rate: {(total_discount/total_gross*100) if total_gross > 0 else 0:.1f}%")
            
        else:
            print("❌ No dimensional data extracted")
            
    except Exception as e:
        print(f"❌ ETL Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())