"""
Lazada API Client for Data Extraction

This module provides a comprehensive client for fetching data from Lazada API
and transforming it into the star schema format.
"""

import os
import time
import hmac
import hashlib
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys
sys.path.append('./app')

from supabase_client import supabase
from lazada_token_manager import create_token_manager_from_env

# Load environment variables
load_dotenv()

class LazadaAPIClient:
    def __init__(self):
        # Initialize token manager for automated refresh
        try:
            self.token_manager = create_token_manager_from_env()
            print("✅ Lazada Token Manager initialized with automated refresh")
        except Exception as e:
            print(f"⚠️ Could not initialize token manager: {e}")
            # Fallback to direct environment variables
            self.token_manager = None
            self.app_key = os.getenv('LAZADA_APP_KEY')
            self.app_secret = os.getenv('LAZADA_APP_SECRET')
            self.access_token = os.getenv('LAZADA_ACCESS_TOKEN')
            self.refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
        
        # Set up API configuration
        if self.token_manager:
            self.app_key = self.token_manager.app_key
            self.app_secret = self.token_manager.app_secret
        else:
            self.app_key = os.getenv('LAZADA_APP_KEY')
            self.app_secret = os.getenv('LAZADA_APP_SECRET')
            self.access_token = os.getenv('LAZADA_ACCESS_TOKEN')
            self.refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
        
        # Validate required credentials
        if not all([self.app_key, self.app_secret]):
            raise ValueError("Missing required Lazada credentials in environment variables")
        
        # Use Philippines sandbox environment for development
        self.base_url = 'https://api.lazada.com.ph/rest'  # Philippines sandbox
        print("🧪 Using Lazada SANDBOX environment (Philippines)")
        
    def get_current_access_token(self):
        """Get current valid access token"""
        if self.token_manager:
            token = self.token_manager.get_valid_access_token()
            if token:
                return token
            else:
                raise Exception("Could not obtain valid access token")
        else:
            return self.access_token
        
    def generate_signature(self, api_path, parameters):
        """Generate signature for Lazada API using token manager"""
        if self.token_manager:
            return self.token_manager.generate_signature(api_path, parameters)
        else:
            # Fallback to original method
            sorted_params = sorted(parameters.items())
            query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
            string_to_sign = api_path + query_string
            
            signature = hmac.new(
                self.app_secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.sha256
            ).hexdigest().upper()
            
            return signature
        string_to_sign = api_path + query_string
        
        # Generate signature
        signature = hmac.new(
            self.app_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()
        
        return signature
    
    def refresh_access_token(self):
        """Refresh the access token using refresh token"""
        print("Refreshing access token...")
        
        api_path = '/auth/token/refresh'
        base_url = 'https://auth.lazada.com.ph/rest'  # Philippines auth sandbox
        
        timestamp = str(int(time.time() * 1000))
        parameters = {
            'app_key': self.app_key,
            'timestamp': timestamp,
            'sign_method': 'sha256',
            'refresh_token': self.refresh_token
        }
        
        signature = self.generate_signature(api_path, parameters)
        parameters['sign'] = signature
        
        response = requests.post(f"{base_url}{api_path}", data=parameters, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if 'access_token' in data:
                self.access_token = data['access_token']
                print("Access token refreshed successfully")
                
                # Update environment variable
                os.environ['LAZADA_ACCESS_TOKEN'] = self.access_token
                return True
            else:
                print(f"Token refresh failed: {data}")
                return False
        else:
            print(f"Token refresh HTTP error: {response.status_code} - {response.text}")
            return False
    
    def test_api_connection(self):
        """Test API connection with a simple call"""
        print("Testing API connection...")
        
        # Try to get seller info (simpler endpoint)
        api_path = '/seller/get'
        timestamp = str(int(time.time() * 1000))
        
        parameters = {
            'app_key': self.app_key,
            'timestamp': timestamp,
            'sign_method': 'sha256',
            'access_token': self.access_token
        }
        
        signature = self.generate_signature(api_path, parameters)
        parameters['sign'] = signature
        
        url = f"{self.base_url}{api_path}"
        response = requests.get(url, params=parameters, timeout=30)
        
        print(f"Test API Response: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Response data: {data}")
            return data.get('code') == '0'
        else:
            print(f"Response text: {response.text}")
            return False
    def make_api_call(self, api_path, params=None, retry_on_auth_error=True):
        """Make authenticated API call to Lazada with automated token management"""
        if params is None:
            params = {}
        
        # Get current valid access token (automatically refreshes if needed)
        try:
            current_token = self.get_current_access_token()
        except Exception as e:
            return {'success': False, 'error': f'Token error: {str(e)}'}
        
        # Add required parameters
        timestamp = str(int(time.time() * 1000))
        parameters = {
            'app_key': self.app_key,
            'timestamp': timestamp,
            'sign_method': 'sha256',
            'access_token': current_token,
            **params
        }
        
        # Generate signature
        signature = self.generate_signature(api_path, parameters)
        parameters['sign'] = signature
        
        # Make request
        url = f"{self.base_url}{api_path}"
        response = requests.get(url, params=parameters, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '0':
                return {'success': True, 'data': data.get('data', {})}
            elif 'signature' in data.get('message', '').lower() and retry_on_auth_error:
                # Try force refreshing token and retry once
                print("🔄 Signature error detected, force refreshing token...")
                if self.token_manager:
                    refresh_result = self.token_manager.refresh_access_token()
                    if refresh_result['success']:
                        return self.make_api_call(api_path, params, retry_on_auth_error=False)
                    else:
                        return {'success': False, 'error': f'Token refresh failed: {refresh_result["error"]}'}
                else:
                    # Fallback to old refresh method
                    if self.refresh_access_token():
                        return self.make_api_call(api_path, params, retry_on_auth_error=False)
                    else:
                        return {'success': False, 'error': 'Token refresh failed'}
            else:
                return {'success': False, 'error': data.get('message', 'Unknown API error')}
        else:
            return {'success': False, 'error': f'HTTP {response.status_code}: {response.text}'}
    
    def get_orders(self, status='delivered', limit=100, days_back=30):
        """Fetch orders from Lazada API"""
        print(f"Fetching {status} orders (last {days_back} days, limit {limit})...")
        
        # Try different order endpoints
        endpoints_to_try = [
            '/orders/get',
            '/order/get',  # Alternative endpoint
        ]
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        for api_path in endpoints_to_try:
            print(f"Trying endpoint: {api_path}")
            
            params = {
                'status': status,
                'limit': str(limit),
                'created_after': start_date.strftime('%Y-%m-%dT%H:%M:%S+08:00'),
                'created_before': end_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
            }
            
            result = self.make_api_call(api_path, params)
            
            if result['success']:
                orders = result['data'].get('orders', []) or result['data'].get('data', [])
                print(f"Successfully fetched {len(orders)} orders")
                return orders
            else:
                print(f"Error with {api_path}: {result['error']}")
                
        print("All order endpoints failed")
        return []
    
    def get_seller_info(self):
        """Get seller account information"""
        print("Fetching seller information...")
        
        result = self.make_api_call('/seller/get')
        
        if result['success']:
            seller_info = result['data']
            print(f"Successfully fetched seller info")
            return seller_info
        else:
            print(f"Error fetching seller info: {result['error']}")
            return {}
    
    def get_product_list(self, limit=50):
        """Fetch product list from seller account"""
        print(f"Fetching product list (limit {limit})...")
        
        params = {
            'filter': 'all',
            'limit': str(limit),
            'offset': '0'
        }
        
        result = self.make_api_call('/products/get', params)
        
        if result['success']:
            products = result['data'].get('products', [])
            print(f"Successfully fetched {len(products)} products")
            return products
        else:
            print(f"Error fetching products: {result['error']}")
            return []
    
    def get_order_items(self, order_id):
        """Fetch detailed order items for a specific order"""
        params = {'order_id': str(order_id)}
        
        result = self.make_api_call('/order/items/get', params)
        
        if result['success']:
            return result['data'].get('order_items', [])
        else:
            print(f"Error fetching order items for {order_id}: {result['error']}")
            return []
    
    def get_products(self, limit=100):
        """Fetch product catalog from Lazada"""
        print(f"Fetching products (limit {limit})...")
        
        params = {
            'limit': str(limit),
            'offset': '0'
        }
        
        result = self.make_api_call('/products/get', params)
        
        if result['success']:
            products = result['data'].get('products', [])
            print(f"Successfully fetched {len(products)} products")
            return products
        else:
            print(f"Error fetching products: {result['error']}")
            return []
    
    def get_seller_metrics(self, date_range_days=30):
        """Fetch seller performance metrics"""
        print(f"Fetching seller metrics (last {date_range_days} days)...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range_days)
        
        params = {
            'date_from': start_date.strftime('%Y-%m-%d'),
            'date_to': end_date.strftime('%Y-%m-%d')
        }
        
        result = self.make_api_call('/seller/performance/get', params)
        
        if result['success']:
            metrics = result['data']
            print(f"Successfully fetched seller metrics")
            return metrics
        else:
            print(f"Error fetching seller metrics: {result['error']}")
            return {}

class LazadaDataTransformer:
    """Transform Lazada API data to star schema format"""
    
    def __init__(self):
        self.platform_key = 1  # Lazada platform key
    
    def get_or_create_time_dimension(self, date_str):
        """Get or create time dimension entry and return time_key"""
        try:
            # Parse date string
            if isinstance(date_str, str):
                date_obj = pd.to_datetime(date_str).date()
            else:
                date_obj = date_str.date() if hasattr(date_str, 'date') else date_str
                
            time_key = int(date_obj.strftime('%Y%m%d'))  # YYYYMMDD format
            
            # Check if time dimension exists
            result = supabase.table('Dim_Time').select('time_key').eq('time_key', time_key).execute()
            
            if not result.data:
                # Create new time dimension record
                time_data = {
                    'time_key': time_key,
                    'date': date_obj.isoformat(),
                    'day_of_week': date_obj.weekday() + 1,  # 1=Monday, 7=Sunday
                    'month': date_obj.month,
                    'year': date_obj.year,
                    'is_mega_sale_day': self._is_mega_sale_day(date_obj)
                }
                
                supabase.table('Dim_Time').insert(time_data).execute()
                print(f"Created time dimension for {date_obj}")
                
            return time_key
        except Exception as e:
            print(f"Error creating time dimension: {e}")
            return None
    
    def _is_mega_sale_day(self, date_obj):
        """Check if date is a mega sale day (11.11, 12.12, etc.)"""
        mega_sale_dates = [
            (11, 11),  # Singles Day
            (12, 12),  # 12.12 Sale
            (3, 3),    # 3.3 Sale
            (4, 4),    # 4.4 Sale
            (5, 5),    # 5.5 Sale
            (6, 6),    # 6.6 Sale
            (7, 7),    # 7.7 Sale
            (8, 8),    # 8.8 Sale
            (9, 9),    # 9.9 Sale
            (10, 10),  # 10.10 Sale
        ]
        return (date_obj.month, date_obj.day) in mega_sale_dates
    
    def get_or_create_customer_dimension(self, buyer_info):
        """Get or create customer dimension entry and return customer_key"""
        try:
            # Extract buyer information
            buyer_id = buyer_info.get('buyer_id', 'unknown')
            
            # Create platform buyer ID (anonymized for privacy)
            platform_buyer_id = f"LZ_{abs(hash(str(buyer_id))) % 1000000}"
            
            # Check if customer exists
            result = supabase.table('Dim_Customer').select('customer_key').eq('platform_buyer_id', platform_buyer_id).execute()
            
            if result.data:
                return result.data[0]['customer_key']
            else:
                # Extract location info from shipping address
                shipping_address = buyer_info.get('shipping_address', {})
                city = shipping_address.get('city', 'Unknown')
                region = shipping_address.get('region', 'Philippines')
                
                # Create new customer dimension record
                customer_data = {
                    'platform_buyer_id': platform_buyer_id,
                    'city': city,
                    'region': region,
                    'buyer_segment': 'Active',  # Will be calculated based on order history
                    'LTV_tier': 'Bronze',  # Will be calculated based on purchase history
                    'last_order_date': datetime.now().date().isoformat()
                }
                
                result = supabase.table('Dim_Customer').insert(customer_data).execute()
                return result.data[0]['customer_key']
                
        except Exception as e:
            print(f"Error creating customer dimension: {e}")
            return None
    
    def get_or_create_product_dimension(self, product_info):
        """Get or create product dimension entry and return product_key"""
        try:
            # Extract product information
            sku_id = product_info.get('sku_id') or product_info.get('item_id')
            product_name = product_info.get('name', 'Unknown Product')
            
            # Check if product exists by Lazada item ID
            result = supabase.table('Dim_Product').select('product_key').eq('lazada_item_id', str(sku_id)).execute()
            
            if result.data:
                return result.data[0]['product_key']
            else:
                # Create new product dimension record
                product_data = {
                    'product_name': product_name,
                    'lazada_item_id': str(sku_id),
                    'shopee_item_id': None,
                    'category_l2': product_info.get('category', 'General'),
                    'product_rating': float(product_info.get('rating', 0)) if product_info.get('rating') else None,
                    'review_count': int(product_info.get('review_count', 0)),
                    'stock_on_hand': int(product_info.get('stock', 0)),
                    'promo_type': product_info.get('promotion_type')
                }
                
                result = supabase.table('Dim_Product').insert(product_data).execute()
                return result.data[0]['product_key']
                
        except Exception as e:
            print(f"Error creating product dimension: {e}")
            return None
    
    def transform_orders_to_facts(self, orders):
        """Transform Lazada orders to fact table records"""
        fact_orders = []
        
        print(f"Transforming {len(orders)} orders to fact records...")
        
        for order in orders:
            try:
                # Get time dimension
                created_at = order.get('created_at')
                time_key = self.get_or_create_time_dimension(created_at)
                
                if not time_key:
                    continue
                
                # Get customer dimension
                buyer_info = {
                    'buyer_id': order.get('customer_first_name', '') + order.get('customer_last_name', ''),
                    'shipping_address': order.get('address_shipping', {})
                }
                customer_key = self.get_or_create_customer_dimension(buyer_info)
                
                if not customer_key:
                    continue
                
                # Process order items
                order_items = order.get('order_items', [])
                
                for item in order_items:
                    # Get product dimension
                    product_key = self.get_or_create_product_dimension(item)
                    
                    if not product_key:
                        continue
                    
                    # Calculate pricing
                    item_price = float(item.get('item_price', 0))
                    quantity = int(item.get('quantity', 1))
                    total_price = item_price * quantity
                    
                    # Create fact order record
                    fact_record = {
                        'time_key': time_key,
                        'product_key': product_key,
                        'customer_key': customer_key,
                        'platform_key': self.platform_key,
                        'paid_price': total_price,
                        'item_quantity': quantity,
                        'cancellation_reason': order.get('cancel_reason'),
                        'return_reason': order.get('return_reason'),
                        'seller_commission_fee': total_price * 0.05,  # 5% commission estimate
                        'platform_subsidy_amount': float(item.get('voucher_amount', 0))
                    }
                    
                    fact_orders.append(fact_record)
                    
            except Exception as e:
                print(f"Error processing order {order.get('order_id')}: {e}")
                continue
        
        print(f"Created {len(fact_orders)} fact order records")
        return fact_orders
    
    def load_facts_to_database(self, fact_orders):
        """Load fact records into the database"""
        try:
            if fact_orders:
                # Insert in batches to avoid timeout
                batch_size = 100
                total_inserted = 0
                
                for i in range(0, len(fact_orders), batch_size):
                    batch = fact_orders[i:i+batch_size]
                    result = supabase.table('Fact_Orders').insert(batch).execute()
                    total_inserted += len(batch)
                    print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
                
                print(f"Successfully loaded {total_inserted} fact orders into database")
                return True
            else:
                print("No fact orders to load")
                return False
                
        except Exception as e:
            print(f"Error loading fact orders: {e}")
            return False

def fetch_and_load_lazada_data():
    """Main function to fetch Lazada data and load into star schema"""
    try:
        print("=== Lazada API Data Extraction & ETL ===")
        
        # Initialize clients
        api_client = LazadaAPIClient()
        transformer = LazadaDataTransformer()
        
        # Fetch orders from API
        orders = api_client.get_orders(status='delivered', limit=50, days_back=30)
        
        if not orders:
            print("No orders found or API error")
            return {'success': False, 'message': 'No orders fetched from API'}
        
        # Fetch detailed order items for each order
        print("Fetching detailed order items...")
        detailed_orders = []
        
        for order in orders[:10]:  # Limit to first 10 orders for testing
            order_id = order.get('order_id')
            order_items = api_client.get_order_items(order_id)
            
            if order_items:
                order['order_items'] = order_items
                detailed_orders.append(order)
        
        print(f"Got detailed data for {len(detailed_orders)} orders")
        
        # Transform to star schema
        fact_orders = transformer.transform_orders_to_facts(detailed_orders)
        
        # Load into database
        success = transformer.load_facts_to_database(fact_orders)
        
        if success:
            return {
                'success': True,
                'orders_processed': len(detailed_orders),
                'fact_records_created': len(fact_orders),
                'message': f'Successfully processed {len(detailed_orders)} orders and created {len(fact_orders)} fact records'
            }
        else:
            return {
                'success': False,
                'message': 'Failed to load data into database'
            }
            
    except Exception as e:
        return {
            'success': False,
            'message': f'Error in ETL process: {str(e)}'
        }

if __name__ == "__main__":
    # Test the Lazada API integration
    result = fetch_and_load_lazada_data()
    print(f"\nResult: {result}")