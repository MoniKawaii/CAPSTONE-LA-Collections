try:
    import hmac
    import hashlib
except ImportError:
    print("⚠️ Warning: hmac or hashlib not found. Please ensure Python standard library is accessible.")
    
import json
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys
import os
import time
import math
import requests

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import SHOPEE_TOKENS, SHOPEE_BASE_URL, DIM_PRODUCT_COLUMNS, DIM_ORDER_COLUMNS, FACT_ORDERS_COLUMNS, FACT_TRAFFIC_COLUMNS, DIM_CUSTOMER_COLUMNS
except ImportError:
    # Fallback for VS Code or different environments
    try:
        from app.config import SHOPEE_TOKENS, SHOPEE_BASE_URL, DIM_PRODUCT_COLUMNS, DIM_ORDER_COLUMNS, FACT_ORDERS_COLUMNS, FACT_TRAFFIC_COLUMNS, DIM_CUSTOMER_COLUMNS
    except ImportError:
        print("Warning: Could not import config. Make sure config.py is accessible.")
        # Define minimal fallbacks if needed
        SHOPEE_TOKENS = {}
        SHOPEE_BASE_URL = "https://partner.shopeemobile.com"

class ShopeeDataExtractor:
    """
    Shopee API data extraction class with batch processing and JSON storage
    Optimized for API calls with Shopee Open Platform API v2.0
    Mirrors Lazada extraction structure for unified harmonization
    """
    
    def __init__(self):
        self.base_url = SHOPEE_BASE_URL
        self.partner_id = SHOPEE_TOKENS.get("partner_id", 0)
        self.partner_key = SHOPEE_TOKENS.get("partner_key", "")
        self.shop_id = SHOPEE_TOKENS.get("shop_id", 0)
        self.access_token = SHOPEE_TOKENS.get("access_token", "")
        self.refresh_token = SHOPEE_TOKENS.get("refresh_token", "")
        
        # API call tracking
        self.api_calls_made = 0
        self.max_daily_calls = 10000
        self.batch_size = 50  # Safe batch size for products
        self.orders_batch_size = 100  # Maximum for orders API
        
        # Storage paths
        self.staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
        os.makedirs(self.staging_dir, exist_ok=True)
        
        print(f"✅ Shopee Extractor initialized")
        print(f"📁 Staging directory: {self.staging_dir}")
        print(f"📊 Daily API limit: {self.max_daily_calls}")
    
    def _generate_signature(self, path, timestamp, access_token=None, shop_id=None, body=None):
        """Generate HMAC-SHA256 signature for Shopee API calls"""
        base_string = f"{self.partner_id}{path}{timestamp}"
        
        # Authenticated calls require access_token and shop_id
        if access_token and shop_id:
            base_string += f"{access_token}{shop_id}"
        
        # For POST requests, the body is part of the signature
        if body:
            base_string += json.dumps(body, separators=(',', ':'), sort_keys=True)
        
        signature = hmac.new(
            self.partner_key.encode('utf-8'),
            base_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def _make_api_call(self, path, method="GET", body=None, call_type="general"):
        """Make API call with rate limiting and tracking"""
        if self.api_calls_made >= self.max_daily_calls:
            print(f"⚠️ Daily API limit ({self.max_daily_calls}) reached!")
            return None
        
        try:
            timestamp = int(time.time())
            sign = self._generate_signature(path, timestamp, self.access_token, self.shop_id, body)
            
            # Build URL with common parameters
            url = (
                f"{self.base_url}{path}?"
                f"partner_id={self.partner_id}&"
                f"timestamp={timestamp}&"
                f"access_token={self.access_token}&"
                f"shop_id={self.shop_id}&"
                f"sign={sign}"
            )
            
            self.api_calls_made += 1
            
            if method == "GET":
                response = requests.get(url, timeout=30)
            else:
                headers = {'Content-Type': 'application/json'}
                response = requests.post(url, json=body, headers=headers, timeout=30)
            
            print(f"🔄 API Call #{self.api_calls_made} ({call_type}) - Status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"❌ API Error - Status: {response.status_code}, Response: {response.text}")
                return None
            
            data = response.json()
            
            # Check for Shopee API errors
            if 'error' in data and data['error']:
                error_msg = data.get('message', 'Unknown error')
                print(f"❌ Shopee API Error - Message: {error_msg}")
                
                # Handle rate limit
                if 'rate limit' in error_msg.lower():
                    print(f"   ⏳ Rate limit hit! Waiting 60 seconds before retry...")
                    time.sleep(60)
                    print(f"   🔄 Retrying API call...")
                    return self._make_api_call(path, method, body, call_type)
                
                return None
            
            # Rate limiting to prevent API frequency issues
            time.sleep(1.5)
            
            return data
            
        except Exception as e:
            print(f"❌ API call failed: {e}")
            return None
    
    def _save_to_json(self, data, filename):
        """Save data to JSON file in staging directory"""
        filepath = os.path.join(self.staging_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        print(f"💾 Saved {len(data) if isinstance(data, list) else 1} records to {filename}")
    
    def _load_from_json(self, filename):
        """Load data from JSON file if it exists"""
        filepath = os.path.join(self.staging_dir, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        return json.loads(content)
                    else:
                        print(f"📄 {filename} is empty, starting fresh extraction")
                        return []
            except (json.JSONDecodeError, Exception) as e:
                print(f"⚠️ Error reading {filename}: {e}. Starting fresh extraction")
                return []
        return []
    
    def extract_all_products(self, start_fresh=False):
        """
        Extract ALL products from Shopee with pagination
        Saves to shopee_products_raw.json
        Uses /api/v2/product/get_item_list and /api/v2/product/get_item_base_info
        """
        filename = 'shopee_products_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📦 Found {len(existing_data)} existing products. Use start_fresh=True to re-extract.")
                return existing_data
        
        print("🔍 Starting complete product extraction...")
        
        # Step 1: Get item list (item_ids only)
        path = "/api/v2/product/get_item_list"
        offset = 0
        page_size = 100  # Max 100 per page
        item_status = "NORMAL"  # Get active products
        
        all_item_ids = []
        has_more = True
        
        while has_more and self.api_calls_made < self.max_daily_calls:
            # Build query parameters
            query_path = f"{path}&item_status={item_status}&offset={offset}&page_size={page_size}"
            
            data = self._make_api_call(query_path, method="GET", call_type=f"product-list-offset-{offset}")
            
            if not data or 'response' not in data:
                break
            
            response = data['response']
            items = response.get('item', [])
            
            if not items:
                has_more = False
                print("✅ No more products found")
            else:
                item_ids = [item['item_id'] for item in items]
                all_item_ids.extend(item_ids)
                offset += page_size
                print(f"📦 Extracted {len(items)} product IDs (Total: {len(all_item_ids)})")
                
                # Check if there's more
                has_more = response.get('has_next_page', False)
        
        print(f"📊 Total product IDs extracted: {len(all_item_ids)}")
        
        # Step 2: Get detailed product information in batches
        print("🔍 Fetching detailed product information...")
        all_products = []
        batch_size = 50  # Max 50 items per batch
        total_batches = math.ceil(len(all_item_ids) / batch_size)
        
        for i in range(0, len(all_item_ids), batch_size):
            if self.api_calls_made >= self.max_daily_calls:
                print(f"⚠️ Daily API limit reached at batch {i//batch_size + 1}")
                break
            
            batch = all_item_ids[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            # Get base info for batch
            path = "/api/v2/product/get_item_base_info"
            item_id_list = ','.join(str(id) for id in batch)
            query_path = f"{path}&item_id_list={item_id_list}"
            
            data = self._make_api_call(query_path, method="GET", call_type=f"product-details-batch-{batch_num}")
            
            if data and 'response' in data:
                items = data['response'].get('item_list', [])
                all_products.extend(items)
                print(f"  └── Batch {batch_num}/{total_batches}: +{len(items)} products (total: {len(all_products)})")
            else:
                print(f"  └── Batch {batch_num}: No data returned or error.")
            
            # Save progress periodically
            if len(all_products) % 500 == 0 and all_products:
                self._save_to_json(all_products, filename)
        
        # Final save
        self._save_to_json(all_products, filename)
        print(f"🎉 Product extraction complete! Total: {len(all_products)} products")
        return all_products
    
    def extract_all_orders(self, start_date=None, end_date=None, start_fresh=False):
        """
        Extract ALL orders from Shopee with time-based pagination
        Shopee API allows 15-day chunks maximum
        Saves to shopee_orders_raw.json
        """
        filename = 'shopee_orders_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📋 Found {len(existing_data)} existing orders. Use start_fresh=True to re-extract.")
                return existing_data
        
        # Default to extract from 2020-04-01 to 2025-04-30 (main business period)
        if not start_date:
            start_date = datetime(2020, 4, 1)
        elif isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        
        if not end_date:
            end_date = datetime(2025, 4, 30)
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        print(f"🔍 Starting complete order extraction from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
        
        # Calculate total days and number of 15-day chunks needed
        total_days = (end_date - start_date).days
        chunk_days = 15  # Shopee API maximum
        total_chunks = (total_days // chunk_days) + (1 if total_days % chunk_days > 0 else 0)
        
        print(f"📊 Total period: {total_days} days")
        print(f"📦 Breaking into {total_chunks} chunks of {chunk_days} days each (API limit)")
        
        all_orders = []
        current_start = start_date
        chunk_num = 0
        
        while current_start < end_date:
            chunk_num += 1
            chunk_end = min(current_start + timedelta(days=chunk_days), end_date)
            
            # Convert to Unix timestamp
            time_from = int(current_start.timestamp())
            time_to = int(chunk_end.timestamp())
            
            print(f"\n📅 Chunk {chunk_num}/{total_chunks}: {current_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            # Extract orders for this chunk with pagination
            chunk_orders = self._extract_orders_chunk(time_from, time_to, chunk_num)
            all_orders.extend(chunk_orders)
            
            print(f"✅ Chunk {chunk_num}: Got {len(chunk_orders)} orders (Total: {len(all_orders)})")
            
            # Check API limit
            if self.api_calls_made >= self.max_daily_calls:
                print(f"⚠️ Daily API limit reached! Stopping at chunk {chunk_num}")
                break
            
            # Move to next chunk
            current_start = chunk_end + timedelta(days=1)
        
        # Final save
        self._save_to_json(all_orders, filename)
        print(f"🎉 Order extraction complete! Total: {len(all_orders)} orders across {chunk_num} chunks")
        return all_orders
    
    def _extract_orders_chunk(self, time_from, time_to, chunk_num):
        """Extract orders for a single time chunk with pagination"""
        chunk_orders = []
        page_size = 100  # Max 100 orders per page
        cursor = ""
        has_more = True
        batch_count = 0
        
        path = "/api/v2/order/get_order_list"
        
        while has_more and self.api_calls_made < self.max_daily_calls:
            batch_count += 1
            
            # Build query parameters
            query_path = f"{path}&time_range_field=create_time&time_from={time_from}&time_to={time_to}&page_size={page_size}&order_status=ALL"
            
            if cursor:
                query_path += f"&cursor={cursor}"
            
            data = self._make_api_call(query_path, method="GET", call_type=f"chunk-{chunk_num}-batch-{batch_count}")
            
            if not data or 'response' not in data:
                break
            
            response = data['response']
            orders = response.get('order_list', [])
            
            if not orders:
                has_more = False
                print(f"  └── No more orders in chunk {chunk_num}")
            else:
                # Get detailed order info for each order
                order_sns = [order['order_sn'] for order in orders]
                detailed_orders = self._get_order_details(order_sns, chunk_num, batch_count)
                chunk_orders.extend(detailed_orders)
                
                print(f"  └── Batch {batch_count}: +{len(detailed_orders)} orders (chunk total: {len(chunk_orders)})")
                
                # Check if there's more
                has_more = response.get('more', False)
                cursor = response.get('next_cursor', "")
                
                # Save progress every 500 orders
                if len(chunk_orders) % 500 == 0:
                    print(f"  💾 Saving progress... {len(chunk_orders)} orders in current chunk")
        
        return chunk_orders
    
    def _get_order_details(self, order_sns, chunk_num, batch_num):
        """Get detailed order information for a list of order_sn"""
        if not order_sns:
            return []
        
        path = "/api/v2/order/get_order_detail"
        
        # Split into batches of 50 (API limit)
        batch_size = 50
        all_details = []
        
        for i in range(0, len(order_sns), batch_size):
            batch = order_sns[i:i+batch_size]
            order_sn_list = ','.join(batch)
            
            query_path = f"{path}&order_sn_list={order_sn_list}&response_optional_fields=buyer_user_id,buyer_username,estimated_shipping_fee,recipient_address,actual_shipping_fee,goods_to_declare,note,note_update_time,item_list,pay_time,dropshipper,credit_card_number,dropshipper_phone,split_up,buyer_cancel_reason,cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,fulfillment_flag,pickup_done_time,package_list,shipping_carrier,payment_method,total_amount,buyer_username,invoice_data"
            
            data = self._make_api_call(query_path, method="GET", call_type=f"order-details-c{chunk_num}-b{batch_num}-sub{i//batch_size}")
            
            if data and 'response' in data:
                orders = data['response'].get('order_list', [])
                all_details.extend(orders)
        
        return all_details
    
    def extract_all_order_items(self, orders_data=None, start_fresh=False):
        """
        Extract order items from order data
        Shopee includes items in order details, so this processes existing order data
        Saves to shopee_multiple_order_items_raw.json
        """
        filename = 'shopee_multiple_order_items_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📦 Found {len(existing_data)} existing order items. Use start_fresh=True to re-extract.")
                return existing_data
        
        # Load orders if not provided
        if not orders_data:
            orders_data = self._load_from_json('shopee_orders_raw.json')
        
        if not orders_data:
            print("❌ No orders data found. Please extract orders first.")
            return []
        
        print(f"🔍 Starting order items extraction from {len(orders_data)} orders...")
        
        all_order_items = []
        
        for order in orders_data:
            order_sn = order.get('order_sn')
            item_list = order.get('item_list', [])
            
            for item in item_list:
                # Add order context to each item
                item_with_order = {
                    'order_sn': order_sn,
                    'order_status': order.get('order_status'),
                    'create_time': order.get('create_time'),
                    **item  # Include all item fields
                }
                all_order_items.append(item_with_order)
        
        # Save order items
        self._save_to_json(all_order_items, filename)
        print(f"🎉 Order items extraction complete! Total: {len(all_order_items)} items from {len(orders_data)} orders")
        print(f"📊 API calls used: {self.api_calls_made}")
        return all_order_items
    
    def extract_traffic_metrics(self, start_date=None, end_date=None, start_fresh=False, monthly_aggregate=True):
        """
        Extract traffic/advertising metrics - Monthly Aggregates
        Saves to shopee_reportoverview_raw.json
        
        Args:
            start_date: Start date (YYYY-MM-DD or datetime object)
            end_date: End date (YYYY-MM-DD or datetime object)
            start_fresh: Whether to re-extract all data
            monthly_aggregate: Whether to extract monthly data (True) or single period (False)
        """
        filename = 'shopee_reportoverview_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📊 Found existing traffic data. Use start_fresh=True to re-extract.")
                return existing_data
        
        # Default date range: 2022-10-01 to 2025-04-30
        if not start_date:
            start_date = '2022-10-01'
        if not end_date:
            end_date = '2025-04-30'
        
        # Convert string dates to datetime objects if needed
        if isinstance(start_date, str):
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            start_dt = start_date
            
        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_dt = end_date
        
        if monthly_aggregate:
            print(f"📊 Extracting monthly traffic metrics from {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}...")
            return self._extract_monthly_traffic(start_dt, end_dt, filename)
        else:
            print(f"📊 Extracting single period traffic metrics from {start_date} to {end_date}...")
            return self._extract_single_period_traffic(start_date, end_date, filename)
    
    def _extract_monthly_traffic(self, start_date, end_date, filename):
        """Extract traffic data month by month for detailed analysis"""
        monthly_traffic = []
        current_date = start_date
        month_count = 0
        total_months = self._count_months(start_date, end_date)
        
        print(f"📊 Processing {total_months} months of traffic data...")
        print("⚠️ Note: Using shop performance metrics as proxy for advertising data")
        
        while current_date < end_date and self.api_calls_made < self.max_daily_calls:
            # Calculate month boundaries
            month_start = current_date
            month_end = min(
                month_start + relativedelta(months=1) - timedelta(days=1),
                end_date
            )
            
            month_count += 1
            print(f"📅 Month {month_count}/{total_months}: {month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')}")
            
            try:
                # Use shop performance metrics as traffic proxy
                path = "/api/v2/public/get_shop_info"
                
                data = self._make_api_call(path, method="GET", call_type=f"traffic-month-{month_start.strftime('%Y-%m')}")
                
                if data and 'response' in data:
                    shop_info = data['response']
                    
                    # Use middle of month as representative date
                    mid_month = month_start + timedelta(days=15)
                    time_key = int(mid_month.strftime('%Y%m%d'))
                    
                    traffic_record = {
                        'time_key': time_key,
                        'date': mid_month.strftime('%Y-%m-%d'),
                        'year_month': month_start.strftime('%Y-%m'),
                        'platform_key': 2,  # Shopee
                        'platform_name': 'Shopee',
                        
                        # Core Fact_Traffic measures (placeholder - actual metrics may differ)
                        'impressions': 0,
                        'clicks': 0,
                        
                        # Additional metrics
                        'ctr': 0.0,
                        'spend': 0.0,
                        'units_sold': 0,
                        'revenue': 0.0,
                        'cpc': 0.0,
                        'roi': 0.0,
                        
                        # Shop metrics
                        'shop_rating': shop_info.get('rating', 0.0),
                        'response_rate': shop_info.get('response_rate', 0),
                        'response_time': shop_info.get('response_time', 0),
                        
                        # Metadata
                        'period_start': month_start.strftime('%Y-%m-%d'),
                        'period_end': month_end.strftime('%Y-%m-%d'),
                        'granularity': 'monthly',
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                    
                    monthly_traffic.append(traffic_record)
                    print(f"   ✅ Shop Rating: {shop_info.get('rating', 0):.2f}")
                else:
                    print(f"   ⚠️ No data returned for this month")
                
                time.sleep(0.2)
                
            except Exception as e:
                print(f"   ❌ Error extracting month {month_start.strftime('%Y-%m')}: {str(e)}")
            
            # Move to next month
            current_date = month_start + relativedelta(months=1)
        
        # Save monthly data
        self._save_to_json(monthly_traffic, filename)
        
        print(f"\n🎉 Monthly traffic extraction complete!")
        print(f"📊 Total months extracted: {len(monthly_traffic)}")
        print(f"🔄 Total API calls used: {self.api_calls_made}")
        
        return monthly_traffic
    
    def _extract_single_period_traffic(self, start_date, end_date, filename):
        """Extract traffic data for a single period (legacy method)"""
        print("⚠️ Single period traffic extraction - using shop info as proxy")
        
        path = "/api/v2/public/get_shop_info"
        data = self._make_api_call(path, method="GET", call_type="traffic-metrics-single")
        
        if data:
            traffic_data = [data]
            self._save_to_json(traffic_data, filename)
            print(f"✅ Traffic metrics extraction complete!")
            return traffic_data
        else:
            print("❌ Failed to extract traffic metrics")
            return []
    
    def _count_months(self, start_date, end_date):
        """Helper to calculate number of months between dates"""
        months = 0
        current = start_date
        while current < end_date:
            months += 1
            current += relativedelta(months=1)
        return months
    
    def extract_product_details(self, products_data=None, start_fresh=False):
        """
        Extract detailed product info for all item_ids
        Uses /api/v2/product/get_item_base_info in batches of 50
        Saves to shopee_productitem_raw.json
        """
        filename = 'shopee_productitem_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📦 Found {len(existing_data)} existing product details. Use start_fresh=True to re-extract.")
                return existing_data
        
        # Load products if not provided
        if products_data is None:
            products_data = self._load_from_json('shopee_products_raw.json')
        if not products_data:
            print("❌ No products found to extract details for.")
            return []
        
        all_details = []
        item_ids = [str(prod.get('item_id')) for prod in products_data if prod.get('item_id')]
        batch_size = 50
        total_batches = math.ceil(len(item_ids) / batch_size)
        print(f"🔍 Extracting product details for {len(item_ids)} items in {total_batches} batches of {batch_size}...")
        
        for i in range(0, len(item_ids), batch_size):
            batch = item_ids[i:i+batch_size]
            batch_str = ','.join(batch)
            
            path = "/api/v2/product/get_item_base_info"
            query_path = f"{path}&item_id_list={batch_str}"
            
            data = self._make_api_call(query_path, method="GET", call_type=f'product-item-batch-{i//batch_size+1}')
            
            if data and 'response' in data:
                items = data['response'].get('item_list', [])
                all_details.extend(items)
                print(f"  └── Batch {i//batch_size+1}: +{len(items)} items (total: {len(all_details)})")
            else:
                print(f"  └── Batch {i//batch_size+1}: No data returned or error.")
            time.sleep(0.2)
        
        self._save_to_json(all_details, filename)
        print(f"🎉 Product details extraction complete! Total: {len(all_details)} items saved to {filename}")
        return all_details
    
    def extract_review_history_list(self, start_fresh=False, limit_products=None):
        """
        Step 1: Extract review IDs using product item_ids from shopee_products_raw.json
        Uses /api/v2/product/get_comment endpoint
        Saves review data to shopee_reviewhistorylist_raw.json
        
        Args:
            start_fresh (bool): Whether to start fresh or append to existing data
            limit_products (int): Limit number of products to process (for testing)
        
        Returns:
            list: List of review entries
        """
        filename = 'shopee_reviewhistorylist_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📋 Found {len(existing_data)} existing review entries. Use start_fresh=True to overwrite.")
                return existing_data
        
        print(f"🔍 Starting product-based review extraction...")
        
        # Load product data to get item_ids
        products = self._load_from_json('shopee_products_raw.json')
        if not products:
            print("❌ No products found. Please extract products first.")
            return []
        
        # Extract item_ids from products
        item_ids = [product.get('item_id') for product in products if product.get('item_id')]
        
        if not item_ids:
            print("❌ No item_ids found in products data.")
            return []
        
        # Limit products for testing if specified
        if limit_products:
            item_ids = item_ids[:limit_products]
            print(f"🔬 Testing mode: Processing first {len(item_ids)} products")
        
        print(f"📦 Processing reviews for {len(item_ids)} products...")
        
        all_review_entries = []
        processed_count = 0
        
        path = "/api/v2/product/get_comment"
        
        for i, item_id in enumerate(item_ids):
            if self.api_calls_made >= self.max_daily_calls:
                print("⚠️ Daily API limit reached")
                break
            
            processed_count += 1
            print(f"⭐ Product {processed_count}/{len(item_ids)}: Getting reviews for item_id {item_id}")
            
            # Add rate limiting between calls
            if processed_count > 1:
                time.sleep(2)
            
            try:
                # API call to get reviews for specific product
                query_path = f"{path}&item_id={item_id}&page_size=50"
                
                data = self._make_api_call(query_path, method="GET", call_type=f"product-reviews-{item_id}")
                
                if data and 'response' in data:
                    response = data['response']
                    
                    if 'item_comment_list' in response and response['item_comment_list']:
                        reviews = response['item_comment_list']
                        print(f"   ✅ Found {len(reviews)} reviews for product {item_id}")
                        
                        for review in reviews:
                            review_entry = {
                                'item_id': item_id,
                                'comment_id': review.get('comment_id', f"comment_{item_id}_{len(all_review_entries)}"),
                                'rating': review.get('rating_star', 0),
                                'created_at': review.get('ctime', 0),
                                'type': 'product_review'
                            }
                            all_review_entries.append(review_entry)
                    else:
                        print(f"   ℹ️ No reviews found for product {item_id}")
                else:
                    print(f"   ⚠️ No data returned for product {item_id}")
                    
            except Exception as e:
                print(f"   ❌ Error processing product {item_id}: {e}")
                continue
        
        # Save review entries
        self._save_to_json(all_review_entries, filename)
        
        print(f"\n🎉 Review history extraction complete!")
        print(f"   Products processed: {processed_count}")
        print(f"   Total review entries: {len(all_review_entries)}")
        print(f"   Saved to: {filename}")
        
        return all_review_entries
    
    def extract_review_details(self, review_ids=None, start_fresh=False):
        """
        Step 2: Extract detailed review information
        For Shopee, reviews are already collected in step 1, so this processes and enriches them
        Saves detailed reviews to shopee_productreview_raw.json
        
        Args:
            review_ids (list): List of review entries to process
            start_fresh (bool): Whether to start fresh or append to existing data
        
        Returns:
            list: List of detailed review data
        """
        filename = 'shopee_productreview_raw.json'
        
        if not start_fresh:
            existing_reviews = self._load_from_json(filename)
            if existing_reviews:
                print(f"📋 Found {len(existing_reviews)} existing reviews. Use start_fresh=True to overwrite.")
                return existing_reviews
        
        # Load review entries if not provided
        if review_ids is None:
            review_ids = self._load_from_json('shopee_reviewhistorylist_raw.json')
        
        if not review_ids:
            print("❌ No review data found. Please run extract_review_history_list() first.")
            return []
        
        print(f"🔍 Processing {len(review_ids)} review entries...")
        
        # For Shopee, reviews are already detailed from the initial extraction
        # So we just need to format and save them
        all_reviews = []
        
        for entry in review_ids:
            if isinstance(entry, dict):
                review_detail = {
                    'item_id': entry.get('item_id'),
                    'comment_id': entry.get('comment_id'),
                    'rating': entry.get('rating'),
                    'created_at': entry.get('created_at'),
                    'review_type': 'product_based'
                }
                all_reviews.append(review_detail)
        
        # Save reviews
        self._save_to_json(all_reviews, filename)
        
        print(f"\n🎉 Review details processing complete!")
        print(f"   Total reviews processed: {len(all_reviews)}")
        print(f"   Saved to: {filename}")
        
        return all_reviews
    
    def extract_product_reviews(self, start_fresh=False, limit_products=None):
        """
        Complete product review extraction:
        1. Extract reviews for each product using item_ids from shopee_products_raw.json
        2. Process and save detailed review information
        
        Args:
            start_fresh (bool): Whether to start fresh or append to existing data
            limit_products (int): Limit number of products to process (for testing)
        
        Returns:
            list: List of detailed review data
        """
        print(f"🔍 Starting complete product review extraction...")
        
        # Step 1: Extract review entries
        print(f"\n📋 Step 1: Extracting review entries...")
        review_entries = self.extract_review_history_list(start_fresh=start_fresh, limit_products=limit_products)
        
        if not review_entries:
            print("❌ No review data found. Cannot proceed with processing.")
            return []
        
        # Step 2: Process detailed reviews
        print(f"\n📋 Step 2: Processing detailed review information...")
        reviews = self.extract_review_details(review_ids=review_entries, start_fresh=start_fresh)
        
        print(f"\n🎉 Complete review extraction finished!")
        print(f"   Review entries collected: {len(review_entries)}")
        print(f"   Final reviews processed: {len(reviews)}")
        
        return reviews
    
    def run_complete_extraction(self, start_fresh=False):
        """
        Run complete data extraction in optimal order
        """
        print("🚀 Starting COMPLETE Shopee data extraction...")
        print("=" * 60)
        
        extraction_plan = [
            ("Products", self.extract_all_products),
            ("Orders", self.extract_all_orders),
            ("Order Items", self.extract_all_order_items),
            ("Traffic Metrics", self.extract_traffic_metrics),
            ("Product Details", self.extract_product_details),
            ("Product Reviews", self.extract_product_reviews)
        ]
        
        results = {}
        
        for step_name, extraction_func in extraction_plan:
            print(f"\n🔹 Step: {step_name}")
            print("-" * 40)
            
            if self.api_calls_made >= self.max_daily_calls:
                print(f"⚠️ Daily API limit reached. Stopping at {step_name}")
                break
            
            try:
                results[step_name] = extraction_func(start_fresh=start_fresh)
                    
                print(f"✅ {step_name} completed")
                print(f"📊 API calls used: {self.api_calls_made}/{self.max_daily_calls}")
                
            except Exception as e:
                print(f"❌ Error in {step_name}: {e}")
                continue
        
        print("\n" + "=" * 60)
        print("📊 EXTRACTION SUMMARY")
        print("=" * 60)
        
        for step_name, data in results.items():
            count = len(data) if isinstance(data, list) else 0
            print(f"✅ {step_name}: {count} records")
        
        print(f"🔄 Total API calls used: {self.api_calls_made}/{self.max_daily_calls}")
        print(f"📁 All data saved to: {self.staging_dir}")
        
        return results
