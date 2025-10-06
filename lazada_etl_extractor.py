#!/usr/bin/env python3
"""
Lazada Orders ETL Extractor

This script extracts only the necessary fields from Lazada orders API
for the star schema ETL pipeline
"""

import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any
from app.lazada_service import LazadaOAuthService

class LazadaOrdersExtractor:
    def __init__(self):
        self.lazada_service = LazadaOAuthService()
    
    async def get_orders_data(self, days_back: int = 30) -> Dict[str, Any]:
        """Get raw orders data from Lazada API"""
        try:
            result = await self.lazada_service.get_orders()
            return result
        except Exception as e:
            print(f"❌ Error fetching orders: {e}")
            return None
    
    def extract_order_fields(self, orders_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract only the fields needed for ETL from orders data
        
        Returns list of dictionaries with extracted fields for each order
        """
        if not orders_data or 'data' not in orders_data:
            return []
        
        orders = orders_data['data'].get('orders', [])
        extracted_orders = []
        
        for order in orders:
            # Extract core order information
            extracted_order = {
                # Order identifiers
                'order_id': order.get('order_id'),
                'order_number': order.get('order_number'),
                
                # Dates (for Dim_Time)
                'created_at': order.get('created_at'),
                'updated_at': order.get('updated_at'),
                
                # Financial data (for Fact_Orders)
                'price': float(order.get('price', 0)),
                'shipping_fee': float(order.get('shipping_fee', 0)),
                'voucher': float(order.get('voucher', 0)),
                'voucher_seller': float(order.get('voucher_seller', 0)),
                'voucher_platform': float(order.get('voucher_platform', 0)),
                
                # Order details
                'items_count': order.get('items_count', 0),
                'status': order.get('statuses', []),
                'payment_method': order.get('payment_method'),
                'warehouse_code': order.get('warehouse_code'),
                
                # Customer information (for Dim_Customer)
                'customer_first_name': order.get('customer_first_name'),
                'customer_last_name': order.get('customer_last_name'),
                
                # Address information
                'shipping_address': self._extract_address(order.get('address_shipping', {})),
                'billing_address': self._extract_address(order.get('address_billing', {})),
                
                # Platform info
                'platform': 'Lazada',
                'platform_region': 'Philippines'
            }
            
            extracted_orders.append(extracted_order)
        
        return extracted_orders
    
    def _extract_address(self, address: Dict[str, Any]) -> Dict[str, str]:
        """Extract relevant address fields"""
        return {
            'city': address.get('city', ''),
            'country': address.get('country', ''),
            'phone': address.get('phone', ''),
            'first_name': address.get('first_name', ''),
            'last_name': address.get('last_name', '')
        }
    
    def transform_for_star_schema(self, extracted_orders: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform extracted orders into star schema format
        """
        # Prepare data for each dimension and fact table
        dim_time_data = []
        dim_customer_data = []
        dim_platform_data = []
        fact_orders_data = []
        
        # Track unique values to avoid duplicates
        unique_dates = set()
        unique_customers = set()
        unique_platforms = set()
        
        for order in extracted_orders:
            # Process Dim_Time
            created_date = self._parse_date(order['created_at'])
            if created_date and created_date not in unique_dates:
                unique_dates.add(created_date)
                dim_time_data.append({
                    'date': created_date,
                    'day_of_week': created_date.weekday() + 1,
                    'month': created_date.month,
                    'year': created_date.year,
                    'is_mega_sale_day': False  # You can implement logic for this
                })
            
            # Process Dim_Customer
            customer_id = f"{order['customer_first_name']}_{order['shipping_address']['city']}"
            if customer_id not in unique_customers:
                unique_customers.add(customer_id)
                dim_customer_data.append({
                    'platform_buyer_id': customer_id,
                    'city': order['shipping_address']['city'],
                    'region': order['platform_region'],
                    'buyer_segment': 'Regular',  # You can implement segmentation logic
                    'LTV_tier': 'Standard',     # You can implement LTV calculation
                    'last_order_date': created_date
                })
            
            # Process Dim_Platform
            platform_key = f"{order['platform']}_{order['platform_region']}"
            if platform_key not in unique_platforms:
                unique_platforms.add(platform_key)
                dim_platform_data.append({
                    'platform_name': order['platform'],
                    'platform_region': order['platform_region']
                })
            
            # Process Fact_Orders
            fact_orders_data.append({
                'order_id': order['order_id'],
                'order_date': created_date,
                'customer_id': customer_id,
                'platform': platform_key,
                'paid_price': order['price'],
                'item_quantity': order['items_count'],
                'shipping_fee': order['shipping_fee'],
                'voucher_amount': order['voucher'] + order['voucher_seller'] + order['voucher_platform'],
                'order_status': order['status'][0] if order['status'] else 'unknown',
                'payment_method': order['payment_method'],
                'cancellation_reason': 'canceled' if 'canceled' in order.get('status', []) else None,
                'seller_commission_fee': 0.0,  # Not provided in current API
                'platform_subsidy_amount': order['voucher_platform']
            })
        
        return {
            'dim_time': dim_time_data,
            'dim_customer': dim_customer_data,
            'dim_platform': dim_platform_data,
            'fact_orders': fact_orders_data
        }
    
    def _parse_date(self, date_string: str) -> datetime:
        """Parse Lazada date format to datetime object"""
        try:
            # Lazada format: "2025-09-10 10:26:29 +0800"
            date_part = date_string.split(' +')[0]  # Remove timezone
            return datetime.strptime(date_part, '%Y-%m-%d %H:%M:%S')
        except Exception:
            return None
    
    async def extract_and_transform(self) -> Dict[str, List[Dict[str, Any]]]:
        """Main method to extract and transform orders data"""
        print("🔍 Fetching Lazada orders data...")
        
        # Get raw orders data
        orders_data = await self.get_orders_data()
        if not orders_data:
            return {}
        
        print(f"📦 Found {orders_data['data']['count']} orders")
        
        # Extract relevant fields
        print("🔧 Extracting relevant fields...")
        extracted_orders = self.extract_order_fields(orders_data)
        
        # Transform for star schema
        print("⭐ Transforming for star schema...")
        star_schema_data = self.transform_for_star_schema(extracted_orders)
        
        # Print summary
        print("\n📊 ETL Summary:")
        print(f"  📅 Unique Dates: {len(star_schema_data['dim_time'])}")
        print(f"  👥 Unique Customers: {len(star_schema_data['dim_customer'])}")
        print(f"  🌐 Platforms: {len(star_schema_data['dim_platform'])}")
        print(f"  📦 Total Orders: {len(star_schema_data['fact_orders'])}")
        
        return star_schema_data

async def main():
    """Main function to run the ETL extraction"""
    extractor = LazadaOrdersExtractor()
    
    try:
        # Extract and transform data
        etl_data = await extractor.extract_and_transform()
        
        if etl_data:
            # Save to JSON file for review
            output_file = 'lazada_etl_data.json'
            with open(output_file, 'w') as f:
                json.dump(etl_data, f, indent=2, default=str)
            
            print(f"\n✅ ETL data saved to: {output_file}")
            
            # Show sample data
            print("\n📋 Sample Fact Orders Data:")
            for i, order in enumerate(etl_data['fact_orders'][:3]):
                print(f"  Order {i+1}: {order}")
                
        else:
            print("❌ No data extracted")
            
    except Exception as e:
        print(f"❌ ETL Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())