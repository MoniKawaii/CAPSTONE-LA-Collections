#!/usr/bin/env python3
"""
Simple Dimensional Data Pull

This script pulls data from Lazada using the existing service and generates
simple dimensional data for analysis.
"""

import json
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from lazada_service import LazadaOAuthService

class SimpleDimensionalPull:
    def __init__(self):
        self.lazada_service = LazadaOAuthService()
        
    async def pull_all_data(self):
        """Pull all available data and create dimensional structure"""
        print("🚀 Starting Dimensional Data Pull...")
        print("=" * 50)
        
        # Initialize data containers
        dimensional_data = {
            'dim_time': self.generate_time_dimension(),
            'dim_customers': [],
            'dim_products': [],
            'dim_vouchers': [],
            'fact_sales': [],
            'orders': [],
            'order_items': []
        }
        
        try:
            # 1. Get Orders
            print("📦 Fetching Orders...")
            orders_response = await self.lazada_service.get_orders(limit=50)
            
            if orders_response and 'data' in orders_response:
                orders = orders_response['data'].get('orders', [])
                print(f"✅ Found {len(orders)} orders")
                
                # Process orders
                for order in orders:
                    dimensional_data['orders'].append(order)
                    
                    # Extract customer info
                    customer = {
                        'customer_id': order.get('customer_id', f"customer_{len(dimensional_data['dim_customers']) + 1}"),
                        'customer_name': order.get('address_billing', {}).get('first_name', 'Unknown') + ' ' + 
                                       order.get('address_billing', {}).get('last_name', ''),
                        'customer_email': order.get('address_billing', {}).get('email', ''),
                        'customer_phone': order.get('address_billing', {}).get('phone', ''),
                        'city': order.get('address_billing', {}).get('city', ''),
                        'country': order.get('address_billing', {}).get('country', 'Philippines'),
                        'created_at': datetime.now().isoformat()
                    }
                    
                    # Add unique customers only
                    if not any(c['customer_id'] == customer['customer_id'] for c in dimensional_data['dim_customers']):
                        dimensional_data['dim_customers'].append(customer)
                    
                    # Create sales fact
                    sales_fact = {
                        'sales_id': f"sale_{order.get('order_id', len(dimensional_data['fact_sales']) + 1)}",
                        'order_id': order.get('order_id'),
                        'customer_id': customer['customer_id'],
                        'order_date': order.get('created_at', datetime.now().isoformat()),
                        'order_total_price': float(order.get('price', 0)),
                        'shipping_fee': float(order.get('shipping_fee', 0)),
                        'voucher_total': float(order.get('voucher', 0)),
                        'buyer_paid_price': float(order.get('price', 0)) - float(order.get('voucher', 0)) + float(order.get('shipping_fee', 0)),
                        'order_status': order.get('statuses', ['unknown'])[0] if order.get('statuses') else 'unknown',
                        'payment_method': order.get('payment_method', 'unknown'),
                        'created_at': datetime.now().isoformat()
                    }
                    
                    dimensional_data['fact_sales'].append(sales_fact)
            
            # 2. Get Products
            print("🏷️ Fetching Products...")
            products_response = await self.lazada_service.get_products(limit=50)
            
            if products_response and 'data' in products_response:
                products = products_response['data'].get('products', [])
                print(f"✅ Found {len(products)} products")
                
                for product in products:
                    product_dim = {
                        'product_id': product.get('item_id'),
                        'seller_sku': product.get('seller_sku'),
                        'shop_sku': product.get('shop_sku'),
                        'product_name': product.get('attributes', {}).get('name', 'Unknown Product'),
                        'brand': product.get('attributes', {}).get('brand', ''),
                        'category': product.get('primary_category', ''),
                        'price': float(product.get('price', 0)),
                        'special_price': float(product.get('special_price', 0)),
                        'quantity': int(product.get('quantity', 0)),
                        'available': product.get('available', 0),
                        'status': product.get('status', 'unknown'),
                        'created_at': product.get('created_time', datetime.now().isoformat()),
                        'updated_at': product.get('updated_time', datetime.now().isoformat())
                    }
                    
                    dimensional_data['dim_products'].append(product_dim)
            
            # 3. Generate some sample vouchers (since API might not be available)
            print("🎫 Generating Sample Vouchers...")
            sample_vouchers = [
                {
                    'voucher_id': 'voucher_001',
                    'voucher_name': 'New Customer Discount',
                    'voucher_type': 'seller',
                    'discount_type': 'percentage',
                    'discount_value': 10.0,
                    'min_order_amount': 500.0,
                    'start_date': '2024-01-01T00:00:00+08:00',
                    'end_date': '2024-12-31T23:59:59+08:00',
                    'usage_limit': 1000,
                    'used_count': 0,
                    'status': 'active',
                    'created_at': datetime.now().isoformat()
                },
                {
                    'voucher_id': 'voucher_002',
                    'voucher_name': 'Free Shipping',
                    'voucher_type': 'platform',
                    'discount_type': 'fixed_amount',
                    'discount_value': 50.0,
                    'min_order_amount': 300.0,
                    'start_date': '2024-01-01T00:00:00+08:00',
                    'end_date': '2024-12-31T23:59:59+08:00',
                    'usage_limit': 500,
                    'used_count': 0,
                    'status': 'active',
                    'created_at': datetime.now().isoformat()
                }
            ]
            
            dimensional_data['dim_vouchers'] = sample_vouchers
            print(f"✅ Generated {len(sample_vouchers)} sample vouchers")
            
            # Save the dimensional data
            output_file = 'dimensional_data_pull.json'
            with open(output_file, 'w') as f:
                json.dump(dimensional_data, f, indent=2)
            
            print(f"\n💾 Data saved to {output_file}")
            
            # Print summary
            self.print_summary(dimensional_data)
            
            return dimensional_data
            
        except Exception as e:
            print(f"❌ Error during data pull: {e}")
            return None
    
    def generate_time_dimension(self) -> List[Dict[str, Any]]:
        """Generate time dimension for 2024-2025"""
        time_records = []
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2025, 12, 31)
        
        current_date = start_date
        while current_date <= end_date:
            time_record = {
                'date_key': current_date.strftime('%Y%m%d'),
                'full_date': current_date.strftime('%Y-%m-%d'),
                'year': current_date.year,
                'quarter': f"Q{((current_date.month - 1) // 3) + 1}",
                'month': current_date.month,
                'month_name': current_date.strftime('%B'),
                'week_of_year': current_date.isocalendar()[1],
                'day_of_month': current_date.day,
                'day_of_week': current_date.weekday() + 1,
                'day_name': current_date.strftime('%A'),
                'is_weekend': current_date.weekday() >= 5,
                'season': self.get_season(current_date.month),
                'created_at': datetime.now().isoformat()
            }
            
            time_records.append(time_record)
            current_date += timedelta(days=1)
        
        return time_records
    
    def get_season(self, month: int) -> str:
        """Get season based on month"""
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'
    
    def print_summary(self, data: Dict[str, Any]):
        """Print summary of extracted data"""
        print("\n📊 DIMENSIONAL DATA SUMMARY")
        print("=" * 50)
        print(f"🕒 Time Dimension: {len(data['dim_time'])} records")
        print(f"👥 Customers: {len(data['dim_customers'])} unique customers")
        print(f"🏷️ Products: {len(data['dim_products'])} products")
        print(f"🎫 Vouchers: {len(data['dim_vouchers'])} vouchers")
        print(f"💰 Sales Facts: {len(data['fact_sales'])} sales transactions")
        print(f"📦 Orders: {len(data['orders'])} orders")
        
        # Calculate totals
        if data['fact_sales']:
            total_revenue = sum(float(sale['buyer_paid_price']) for sale in data['fact_sales'])
            avg_order_value = total_revenue / len(data['fact_sales']) if data['fact_sales'] else 0
            
            print(f"\n💵 FINANCIAL SUMMARY")
            print(f"Total Revenue: ₱{total_revenue:,.2f}")
            print(f"Average Order Value: ₱{avg_order_value:,.2f}")
        
        print("\n✅ Dimensional data pull completed successfully!")

async def main():
    """Main execution function"""
    puller = SimpleDimensionalPull()
    await puller.pull_all_data()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())