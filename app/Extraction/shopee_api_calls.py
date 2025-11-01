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
        self.partner_id = int(SHOPEE_TOKENS.get("partner_id", 0))
        self.partner_key = SHOPEE_TOKENS.get("partner_key", "")
        self.shop_id = int(SHOPEE_TOKENS.get("shop_id", 0))
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
        print(f"🔍 Debug - partner_id type: {type(self.partner_id)}, value: {self.partner_id}")
        print(f"🔍 Debug - shop_id type: {type(self.shop_id)}, value: {self.shop_id}")
    
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
    
    def _count_months(self, start_date, end_date):
        """
        Count the number of months between two dates
        """
        from dateutil.relativedelta import relativedelta
        
        count = 0
        current = start_date
        while current < end_date:
            count += 1
            current += relativedelta(months=1)
        return count
    
    def _make_api_call(self, path, method="GET", body=None, call_type="general"):
        """Make API call with rate limiting and tracking"""
        if self.api_calls_made >= self.max_daily_calls:
            print(f"⚠️ Daily API limit ({self.max_daily_calls}) reached!")
            return None
        
        try:
            timestamp = int(time.time())
            
            # Extract base path (without query params) for signature
            # Shopee signature uses: partner_id + base_path + timestamp + access_token + shop_id
            base_path = path.split('?')[0]
            sign = self._generate_signature(base_path, timestamp, self.access_token, self.shop_id, body)
            
            # Determine separator - use & if path already has ?, otherwise use ?
            separator = '&' if '?' in path else '?'
            
            # Build URL with common parameters
            url = (
                f"{self.base_url}{path}{separator}"
                f"partner_id={self.partner_id}&"
                f"timestamp={timestamp}&"
                f"access_token={self.access_token}&"
                f"shop_id={self.shop_id}&"
                f"sign={sign}"
            )
            
            # Debug: Print URL parameters
            if self.api_calls_made <= 2:  # Only print first 2 calls
                print(f"🔍 Debug URL - partner_id: {self.partner_id} (type: {type(self.partner_id)})")
                print(f"🔍 Debug - Base path for signature: {base_path}")
                print(f"🔍 Debug URL - Full URL: {url[:200]}...")
            
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
                
                # For rate limit errors, return the data so retry logic can handle it
                if 'too many requests' in error_msg.lower() or 'rate limit' in error_msg.lower():
                    return data  # Return error data for retry handling
                
                return None  # For other errors, return None
            
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
    
    def _get_last_date_from_data(self, data, date_field='create_time'):
        """
        Find the last date in a dataset
        
        Args:
            data (list): List of records
            date_field (str): Field name containing date information
            
        Returns:
            datetime: Last date found, or None if no dates found
        """
        if not data:
            return None
        
        last_date = None
        for record in data:
            date_value = record.get(date_field)
            if date_value:
                try:
                    # Handle Unix timestamp (Shopee often uses timestamps)
                    if isinstance(date_value, (int, float)):
                        current_date = datetime.fromtimestamp(date_value)
                    elif isinstance(date_value, str):
                        # Try parsing various date formats
                        current_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                    else:
                        continue
                    
                    if last_date is None or current_date > last_date:
                        last_date = current_date
                except (ValueError, TypeError):
                    continue
        
        return last_date
    
    def _find_last_extraction_date(self):
        """
        Find the last extraction date across all data files
        
        Returns:
            dict: Dictionary with last dates for each data type
        """
        last_dates = {}
        
        # Check orders data
        orders_data = self._load_from_json('shopee_orders_raw.json')
        if orders_data:
            last_dates['orders'] = self._get_last_date_from_data(orders_data, 'create_time')
        
        # Check traffic data (empty for Shopee but keep for consistency)
        traffic_data = self._load_from_json('shopee_reportoverview_raw.json')
        if traffic_data:
            # For consistency, though Shopee traffic is empty
            last_dates['traffic'] = None
        
        # Check product reviews
        reviews_data = self._load_from_json('shopee_productreview_raw.json')
        if reviews_data:
            last_dates['reviews'] = self._get_last_date_from_data(reviews_data, 'ctime')
        
        return last_dates
    
    def _remove_duplicates_by_id(self, existing_data, new_data, id_field):
        """
        Remove duplicates from new data based on ID field, keeping the latest version
        
        Args:
            existing_data (list): Existing records
            new_data (list): New records to merge
            id_field (str): Field name to use as unique identifier
            
        Returns:
            list: Merged data without duplicates
        """
        # Create a dict of existing records by ID
        existing_by_id = {}
        for record in existing_data:
            record_id = record.get(id_field)
            if record_id:
                existing_by_id[record_id] = record
        
        # Add/update with new records
        for record in new_data:
            record_id = record.get(id_field)
            if record_id:
                existing_by_id[record_id] = record
        
        # Return as list
        return list(existing_by_id.values())
    
    def _get_month_start_date(self, date_obj):
        """Get the first day of the month for a given date"""
        return date_obj.replace(day=1)
    
    def _should_start_fresh_extraction(self, last_dates):
        """
        Determine if we should start fresh extraction based on last dates
        
        Args:
            last_dates (dict): Dictionary of last dates by data type
            
        Returns:
            tuple: (should_start_fresh, start_date)
        """
        if not last_dates or not any(last_dates.values()):
            # No existing data, start from default date
            return False, datetime(2020, 4, 1)
        
        # Find the most recent date across all data types
        most_recent = None
        for date_val in last_dates.values():
            if date_val and (most_recent is None or date_val > most_recent):
                most_recent = date_val
        
        if most_recent:
            # Start from the beginning of the month containing the most recent date
            start_date = self._get_month_start_date(most_recent)
            print(f"🔍 Last extraction date found: {most_recent.strftime('%Y-%m-%d')}")
            print(f"🔄 Will restart extraction from: {start_date.strftime('%Y-%m-%d')} (beginning of month)")
            return False, start_date
        else:
            return False, datetime(2020, 4, 1)
    
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
            query_path = f"{path}?item_status={item_status}&offset={offset}&page_size={page_size}"
            
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
            query_path = f"{path}?item_id_list={item_id_list}"
            
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
    
    def extract_all_orders(self, start_date=None, end_date=None, start_fresh=False, incremental=True):
        """
        Extract ALL orders from Shopee with time-based pagination
        Shopee API allows 15-day chunks maximum
        Saves to shopee_orders_raw.json
        
        Args:
            start_date: Override start date (will be auto-detected if None)
            end_date: End date (defaults to Oct 31, 2025)
            start_fresh: Force complete re-extraction
            incremental: Use incremental update logic
        """
        filename = 'shopee_orders_raw.json'
        
        # Load existing data first
        existing_data = self._load_from_json(filename)
        
        if not start_fresh and not incremental and existing_data:
            print(f"� Found {len(existing_data)} existing orders. Use start_fresh=True to re-extract all.")
            return existing_data
        
        # Determine extraction dates
        if incremental and not start_fresh:
            last_dates = self._find_last_extraction_date()
            should_start_fresh, auto_start_date = self._should_start_fresh_extraction(last_dates)
            
            if should_start_fresh:
                print("🔄 Starting fresh extraction...")
                start_date = auto_start_date
            else:
                start_date = auto_start_date if start_date is None else start_date
                print(f"📈 Incremental extraction from {start_date.strftime('%Y-%m-%d')}")
        elif start_fresh:
            print("🔄 Force starting fresh extraction...")
            if not start_date:
                start_date = datetime(2020, 4, 1)
        
        # Set default dates if not provided
        if not start_date:
            start_date = datetime(2020, 4, 1)
        elif isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        
        if not end_date:
            end_date = datetime(2025, 10, 31)  # Updated to Oct 31, 2025
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        print(f"🔍 Extracting orders from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
        
        # Calculate total days and number of 15-day chunks needed
        total_days = (end_date - start_date).days
        chunk_days = 15  # Shopee API maximum
        total_chunks = (total_days // chunk_days) + (1 if total_days % chunk_days > 0 else 0)
        
        print(f"📊 Total period: {total_days} days")
        print(f"📦 Breaking into {total_chunks} chunks of {chunk_days} days each (API limit)")
        
        new_orders = []
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
            if chunk_orders:
                new_orders.extend(chunk_orders)
                print(f"  ✓ Chunk {chunk_num}: {len(chunk_orders)} orders extracted")
            else:
                print(f"  ✓ Chunk {chunk_num}: No orders found")
            
            # Check API limit
            if self.api_calls_made >= self.max_daily_calls:
                print(f"⚠️ Daily API limit reached! Stopping at chunk {chunk_num}")
                break
            
            # Move to next chunk
            current_start = chunk_end + timedelta(days=1)
        
        # Merge with existing data, removing duplicates by order_sn
        if existing_data and not start_fresh:
            print(f"🔄 Merging {len(new_orders)} new orders with {len(existing_data)} existing orders...")
            all_orders = self._remove_duplicates_by_id(existing_data, new_orders, 'order_sn')
            print(f"📊 Merged result: {len(all_orders)} total orders (duplicates removed by order_sn)")
        else:
            all_orders = new_orders
        
        # Final save
        self._save_to_json(all_orders, filename)
        print(f"🎉 Order extraction complete! Total: {len(all_orders)} orders across {chunk_num} chunks")
        print(f"📈 New orders extracted: {len(new_orders)}")
        print(f"🔄 Duplicates handled by order_sn deduplication")
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
            
            # Build query parameters (use ? for first param)
            # Note: Omit order_status to get all orders regardless of status
            query_path = f"{path}?time_range_field=create_time&time_from={time_from}&time_to={time_to}&page_size={page_size}"
            
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
        
        # Split into batches of 50 (API limit)
        batch_size = 50
        all_details = []
        
        for i in range(0, len(order_sns), batch_size):
            batch = order_sns[i:i+batch_size]
            order_sn_list = ','.join(batch)
            
            # Build path with query parameters (will be merged with auth params in _make_api_call)
            path = f"/api/v2/order/get_order_detail"
            # Add additional parameters as body or separate params
            extra_params = f"order_sn_list={order_sn_list}&response_optional_fields=buyer_user_id,buyer_username,estimated_shipping_fee,recipient_address,actual_shipping_fee,goods_to_declare,note,note_update_time,item_list,pay_time,dropshipper,credit_card_number,dropshipper_phone,split_up,buyer_cancel_reason,cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,fulfillment_flag,pickup_done_time,package_list,shipping_carrier,payment_method,total_amount,buyer_username,invoice_data"
            
            full_path = f"{path}?{extra_params}"
            
            data = self._make_api_call(full_path, method="GET", call_type=f"order-details-c{chunk_num}-b{batch_num}-sub{i//batch_size}")
            
            if data and 'response' in data:
                orders = data['response'].get('order_list', [])
                all_details.extend(orders)
        
        return all_details
    
    def extract_all_order_items(self, orders_data=None, start_fresh=False, incremental=True):
        """
        Extract order items from order data with incremental updates
        Shopee includes items in order details, so this processes existing order data
        Handles duplicates using unique combination of order_sn + item_id
        Saves to shopee_multiple_order_items_raw.json
        
        Args:
            orders_data: Order data to process (auto-loaded if None)
            start_fresh: Force complete re-extraction
            incremental: Use incremental update logic
        """
        filename = 'shopee_multiple_order_items_raw.json'
        
        # Load existing data
        existing_data = self._load_from_json(filename)
        
        if not start_fresh and not incremental and existing_data:
            print(f"📦 Found {len(existing_data)} existing order items. Use start_fresh=True to re-extract.")
            return existing_data
        
        # Load orders if not provided
        if not orders_data:
            orders_data = self._load_from_json('shopee_orders_raw.json')
        
        if not orders_data:
            print("❌ No orders data found. Please extract orders first.")
            return []
        
        print(f"🔍 Starting order items extraction from {len(orders_data)} orders...")
        
        new_order_items = []
        
        for order in orders_data:
            order_sn = order.get('order_sn')
            item_list = order.get('item_list', [])
            
            for item in item_list:
                # Add order context to each item
                item_with_order = {
                    'order_sn': order_sn,
                    'order_status': order.get('order_status'),
                    'create_time': order.get('create_time'),
                    'item_id': item.get('item_id'),
                    'unique_key': f"{order_sn}_{item.get('item_id')}",  # Unique identifier
                    **item  # Include all item fields
                }
                new_order_items.append(item_with_order)
        
        # Merge with existing data, removing duplicates by unique_key
        if existing_data and not start_fresh and incremental:
            print(f"🔄 Merging {len(new_order_items)} new order items with {len(existing_data)} existing items...")
            all_order_items = self._remove_duplicates_by_id(existing_data, new_order_items, 'unique_key')
            print(f"📊 Merged result: {len(all_order_items)} total order items (duplicates removed)")
        else:
            all_order_items = new_order_items
        
        # Save order items
        self._save_to_json(all_order_items, filename)
        print(f"🎉 Order items extraction complete! Total: {len(all_order_items)} items from {len(orders_data)} orders")
        print(f"📈 New order items processed: {len(new_order_items)}")
        print(f"📊 API calls used: {self.api_calls_made}")
        return all_order_items
    
    def extract_traffic_metrics(self, start_date=None, end_date=None, start_fresh=False, monthly_aggregate=True, incremental=True):
        """
        Extract traffic/advertising metrics using Shopee Ads API (v2.ads.get_ad_data)
        Saves to shopee_reportoverview_raw.json
        
        Args:
            start_date: Start date (YYYY-MM-DD or datetime object)
            end_date: End date (YYYY-MM-DD or datetime object)
            start_fresh: Whether to re-extract all data
            monthly_aggregate: Whether to extract monthly data (True) or single period (False)
            incremental: Use incremental update logic
        """
        filename = 'shopee_reportoverview_raw.json'
        
        # Load existing data first
        existing_data = self._load_from_json(filename)
        
        if not start_fresh and not incremental and existing_data:
            print(f"📊 Found {len(existing_data)} existing traffic records. Use start_fresh=True to re-extract.")
            return existing_data
        
        # Determine extraction dates for incremental updates
        if incremental and not start_fresh and existing_data:
            last_dates = self._find_last_extraction_date()
            if last_dates.get('traffic'):
                # Get last traffic date and continue from next month
                last_traffic_date = last_dates['traffic']
                start_date = self._get_month_start_date(last_traffic_date + relativedelta(months=1))
                print(f"🔄 Incremental traffic extraction from: {start_date.strftime('%Y-%m-%d')}")
            
        # Set default dates if not provided
        if not start_date:
            start_date = datetime(2022, 10, 1)  # Start from when ads data is typically available
        elif isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        
        if not end_date:
            end_date = datetime(2025, 10, 31)  # Updated to Oct 31, 2025
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        print(f"\n🔍 Extracting Shopee Ads data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        if monthly_aggregate:
            new_traffic_data = self._extract_monthly_ads_data(start_date, end_date)
        else:
            new_traffic_data = self._extract_single_period_ads_data(start_date, end_date)
        
        # Merge with existing data if incremental
        if existing_data and not start_fresh and incremental:
            print(f"🔄 Merging {len(new_traffic_data)} new traffic records with {len(existing_data)} existing records...")
            # Use date + campaign_id as unique identifier for ads data
            all_traffic_data = self._remove_duplicates_by_id(existing_data, new_traffic_data, 'unique_key')
            print(f"📊 Merged result: {len(all_traffic_data)} total traffic records")
        else:
            all_traffic_data = new_traffic_data
        
        # Save traffic data
        self._save_to_json(all_traffic_data, filename)
        print(f"🎉 Traffic metrics extraction complete! Total: {len(all_traffic_data)} records")
        
        return all_traffic_data
    
    def _extract_monthly_ads_data(self, start_date, end_date):
        """
        Extract Shopee Ads data month by month using v2.ads.get_ad_data API
        Returns detailed advertising metrics for analysis
        """
        from dateutil.relativedelta import relativedelta
        
        monthly_ads = []
        current_date = start_date
        month_count = 0
        total_months = self._count_months(start_date, end_date)
        
        print(f"📊 Processing {total_months} months of Shopee Ads data...")
        
        while current_date < end_date and self.api_calls_made < self.max_daily_calls:
            # Calculate month boundaries
            month_end = min(current_date + relativedelta(months=1) - timedelta(days=1), end_date)
            month_count += 1
            
            print(f"\n📅 Month {month_count}/{total_months}: {current_date.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')}")
            
            # Convert to Unix timestamps for API
            time_from = int(current_date.timestamp())
            time_to = int(month_end.timestamp())
            
            # Extract ads data for this month
            month_ads_data = self._extract_ads_data_for_period(time_from, time_to, current_date)
            
            if month_ads_data:
                monthly_ads.extend(month_ads_data)
                print(f"   ✅ Month {month_count}: {len(month_ads_data)} ads records extracted")
            else:
                print(f"   ℹ️ Month {month_count}: No ads data found")
            
            # Enhanced rate limiting - wait longer to avoid "too many requests"
            if month_count < total_months:
                wait_time = 8 if month_count % 3 == 0 else 5  # Longer wait every 3rd month
                print(f"   ⏱️ Rate limiting: waiting {wait_time} seconds...")
                time.sleep(wait_time)
            
            # Move to next month
            current_date += relativedelta(months=1)
        
        print(f"\n📊 Monthly ads extraction complete!")
        print(f"   Total months processed: {month_count}")
        print(f"   Total ads records: {len(monthly_ads)}")
        print(f"   API calls used: {self.api_calls_made}")
        
        return monthly_ads
    
    def _extract_single_period_ads_data(self, start_date, end_date):
        """
        Extract Shopee Ads data for a single time period using v2.ads.get_ad_data API
        """
        print(f"📊 Extracting single period ads data...")
        
        # Convert to Unix timestamps
        time_from = int(start_date.timestamp())
        time_to = int(end_date.timestamp())
        
        # Extract ads data for the entire period
        ads_data = self._extract_ads_data_for_period(time_from, time_to, start_date)
        
        print(f"📊 Single period extraction complete: {len(ads_data)} records")
        return ads_data
    
    def _extract_ads_data_for_period(self, time_from, time_to, period_start_date):
        """
        Extract ads data using proper two-step process:
        1. Get campaign ID list using v2.ads.get_product_level_campaign_id_list
        2. Get performance data using v2.ads.get_product_campaign_daily_performance
        
        Args:
            time_from (int): Start timestamp
            time_to (int): End timestamp  
            period_start_date (datetime): Start date for labeling
            
        Returns:
            list: Monthly aggregated ads data records
        """
        print(f"   📋 Step 1: Getting campaign ID list...")
        
        # Step 1: Get campaign ID list
        campaign_ids = self._get_product_level_campaign_ids()
        
        if not campaign_ids:
            print("   ⚠️ No campaign IDs found")
            return []
        
        print(f"   📈 Step 2: Getting performance data for {len(campaign_ids)} campaigns...")
        
        # Step 2: Get daily performance data for these campaigns
        daily_ads_data = self._get_campaign_daily_performance_with_ids(
            campaign_ids, time_from, time_to, period_start_date
        )
        
        if not daily_ads_data:
            print("   ⚠️ No daily ads data found")
            return []
        
        # Step 3: Aggregate daily data to monthly
        monthly_aggregated = self._aggregate_daily_to_monthly(daily_ads_data, period_start_date)
        
        print(f"   📊 Aggregated {len(daily_ads_data)} daily records to {len(monthly_aggregated)} monthly records")
        return monthly_aggregated
    
    def _get_product_level_campaign_ids(self):
        """
        Get campaign ID list using v2.ads.get_product_level_campaign_id_list
        Saves the campaign list to shopee_campaign_id_list_raw.json
        """
        campaign_ids = []
        
        try:
            # Build API path for product level campaign ID list
            path = "/api/v2/ads/get_product_level_campaign_id_list"
            
            # Build query parameters
            query_params = {
                'page_size': 100,
                'page_no': 1
            }
            
            query_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
            query_path = f"{path}?{query_string}"
            
            print(f"   📋 Fetching product level campaign ID list...")
            
            # Make API call
            data = self._make_api_call(query_path, method="GET", call_type="campaign-id-list")
            
            if data and 'response' in data:
                response = data['response']
                campaign_list = response.get('campaign_id_list', [])
                
                print(f"   📊 Retrieved {len(campaign_list)} campaign IDs")
                
                # Process campaign list
                campaign_data = []
                for campaign_info in campaign_list:
                    campaign_record = {
                        'campaign_id': campaign_info.get('campaign_id'),
                        'campaign_name': campaign_info.get('campaign_name', f"Campaign_{campaign_info.get('campaign_id')}"),
                        'campaign_type': campaign_info.get('campaign_type', 'product_level'),
                        'status': campaign_info.get('status', 'unknown'),
                        'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'platform': 'Shopee',
                        'data_source': 'ads_api_v2_campaign_list'
                    }
                    campaign_data.append(campaign_record)
                    
                    # Extract just the ID for performance queries
                    if campaign_info.get('campaign_id'):
                        campaign_ids.append(campaign_info.get('campaign_id'))
                
                # Save campaign list to JSON file
                campaign_list_file = os.path.join(self.staging_dir, "shopee_campaign_id_list_raw.json")
                self._save_to_json(campaign_data, campaign_list_file)
                print(f"   💾 Campaign list saved to: shopee_campaign_id_list_raw.json")
                
                # Handle pagination if there are more campaigns
                total_count = response.get('total_count', len(campaign_list))
                if total_count > len(campaign_list):
                    print(f"   📄 More campaigns available: {total_count} total, fetching additional pages...")
                    
                    pages_needed = (total_count // 100) + (1 if total_count % 100 > 0 else 0)
                    
                    for page in range(2, min(pages_needed + 1, 11)):  # Limit to 10 pages max
                        query_params['page_no'] = page
                        query_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
                        query_path = f"{path}?{query_string}"
                        
                        data = self._make_api_call(query_path, method="GET", call_type=f"campaign-id-list-p{page}")
                        
                        if data and 'response' in data:
                            page_campaigns = data['response'].get('campaign_id_list', [])
                            for campaign_info in page_campaigns:
                                campaign_record = {
                                    'campaign_id': campaign_info.get('campaign_id'),
                                    'campaign_name': campaign_info.get('campaign_name', f"Campaign_{campaign_info.get('campaign_id')}"),
                                    'campaign_type': campaign_info.get('campaign_type', 'product_level'),
                                    'status': campaign_info.get('status', 'unknown'),
                                    'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'platform': 'Shopee',
                                    'data_source': 'ads_api_v2_campaign_list'
                                }
                                campaign_data.append(campaign_record)
                                
                                if campaign_info.get('campaign_id'):
                                    campaign_ids.append(campaign_info.get('campaign_id'))
                        
                        time.sleep(2)  # Rate limiting between pages
                    
                    # Update the saved file with all campaigns
                    self._save_to_json(campaign_data, campaign_list_file)
                    print(f"   💾 Complete campaign list saved: {len(campaign_data)} campaigns")
            
            else:
                print(f"   ⚠️ No campaign ID list returned")
                
        except Exception as e:
            print(f"   ❌ Error fetching campaign ID list: {e}")
        
        return campaign_ids
    
    def _get_campaign_daily_performance_with_ids(self, campaign_ids, time_from, time_to, period_start_date):
        """
        Get daily campaign performance data using v2.ads.get_product_campaign_daily_performance
        with the campaign IDs from the previous step
        """
        daily_data = []
        
        if not campaign_ids:
            print("   ⚠️ No campaign IDs provided")
            return []
        
        try:
            # Build API path for product campaign daily performance
            path = "/api/v2/ads/get_product_campaign_daily_performance"
            
            # Build query parameters with campaign ID list
            query_params = {
                'campaign_id_list': ','.join([str(cid) for cid in campaign_ids]),
                'time_from': time_from,
                'time_to': time_to,
                'page_size': 100,
                'page_no': 1
            }
            
            # Convert params to query string
            query_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
            query_path = f"{path}?{query_string}"
            
            print(f"   📅 Fetching daily performance for {len(campaign_ids)} campaigns: {period_start_date.strftime('%Y-%m-%d')}")
            
            # Make API call with retry logic for rate limiting
            max_retries = 3
            retry_count = 0
            data = None
            
            while retry_count < max_retries:
                data = self._make_api_call(query_path, method="GET", call_type=f"daily-performance-{period_start_date.strftime('%Y-%m')}")
                
                # Debug: check what we got
                print(f"   🔍 API Response debug: data={type(data)}, has_response={data and 'response' in data if data else False}")
                
                # Check if we got successful response
                if data and 'response' in data:
                    print(f"   ✅ Success! Breaking retry loop")
                    break
                    
                # Check if we got rate limit error
                elif data and isinstance(data, dict) and data.get('error'):
                    error_msg = data.get('message', '').lower()
                    print(f"   🔍 Error detected: {error_msg}")
                    if 'too many requests' in error_msg or 'rate limit' in error_msg:
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = retry_count * 15  # Exponential backoff: 15s, 30s, 45s
                            print(f"   ⏳ Rate limited, retrying in {wait_time} seconds... (attempt {retry_count}/{max_retries})")
                            time.sleep(wait_time)
                        else:
                            print(f"   ❌ Max retries reached, skipping this period")
                            break
                    else:
                        # Different error, break
                        print(f"   ❌ Non-rate-limit error, breaking")
                        break
                else:
                    # No data returned or unexpected format, break
                    print(f"   ❌ No data or unexpected format, breaking")
                    break
            
            if data and 'response' in data:
                response = data['response']
                performance_list = response.get('performance_list', [])
                
                print(f"   📊 Retrieved {len(performance_list)} daily performance records")
                
                # Process each daily record
                for record in performance_list:
                    daily_record = {
                        # Campaign info
                        'campaign_id': record.get('campaign_id'),
                        'campaign_name': record.get('campaign_name', f"Campaign_{record.get('campaign_id')}"),
                        'campaign_type': record.get('campaign_type', 'product_campaign'),
                        'product_id': record.get('product_id'),
                        'product_name': record.get('product_name', ''),
                        
                        # Date info
                        'date': record.get('date'),  # Daily date
                        'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        
                        # Performance metrics (daily values)
                        'impressions': record.get('impression', 0),
                        'clicks': record.get('click', 0),
                        'spend': float(record.get('spend', 0.0)),
                        'conversions': record.get('conversion', 0),
                        'gmv': float(record.get('gmv', 0.0)),
                        'orders': record.get('order', 0),
                        'sales': float(record.get('sales', 0.0)),
                        'units_sold': record.get('units_sold', 0),
                        
                        # Additional metrics if available
                        'ctr': float(record.get('ctr', 0.0)),
                        'cpc': float(record.get('cpc', 0.0)),
                        'conversion_rate': float(record.get('conversion_rate', 0.0)),
                        'roas': float(record.get('roas', 0.0)) if record.get('roas') else (float(record.get('gmv', 0)) / max(float(record.get('spend', 1)), 1)),
                        'cost_per_order': float(record.get('cost_per_order', 0.0)),
                        'avg_order_value': float(record.get('avg_order_value', 0.0)),
                        
                        # Platform identifier
                        'platform': 'Shopee',
                        'data_source': 'ads_api_v2_product_campaign_daily_performance'
                    }
                    daily_data.append(daily_record)
                
                # Handle pagination if there are more records
                total_count = response.get('total_count', len(performance_list))
                if total_count > len(performance_list):
                    print(f"   📄 More records available: {total_count} total, fetching additional pages...")
                    
                    # Calculate additional pages needed
                    pages_needed = (total_count // 100) + (1 if total_count % 100 > 0 else 0)
                    
                    for page in range(2, min(pages_needed + 1, 11)):  # Limit to 10 pages max
                        query_params['page_no'] = page
                        query_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
                        query_path = f"{path}?{query_string}"
                        
                        data = self._make_api_call(query_path, method="GET", call_type=f"daily-performance-{period_start_date.strftime('%Y-%m')}-p{page}")
                        
                        if data and 'response' in data:
                            page_records = data['response'].get('performance_list', [])
                            for record in page_records:
                                daily_record = {
                                    'campaign_id': record.get('campaign_id'),
                                    'campaign_name': record.get('campaign_name', f"Campaign_{record.get('campaign_id')}"),
                                    'campaign_type': record.get('campaign_type', 'product_campaign'),
                                    'product_id': record.get('product_id'),
                                    'product_name': record.get('product_name', ''),
                                    'date': record.get('date'),
                                    'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    'impressions': record.get('impression', 0),
                                    'clicks': record.get('click', 0),
                                    'spend': float(record.get('spend', 0.0)),
                                    'conversions': record.get('conversion', 0),
                                    'gmv': float(record.get('gmv', 0.0)),
                                    'orders': record.get('order', 0),
                                    'sales': float(record.get('sales', 0.0)),
                                    'units_sold': record.get('units_sold', 0),
                                    'ctr': float(record.get('ctr', 0.0)),
                                    'cpc': float(record.get('cpc', 0.0)),
                                    'conversion_rate': float(record.get('conversion_rate', 0.0)),
                                    'roas': float(record.get('roas', 0.0)) if record.get('roas') else (float(record.get('gmv', 0)) / max(float(record.get('spend', 1)), 1)),
                                    'cost_per_order': float(record.get('cost_per_order', 0.0)),
                                    'avg_order_value': float(record.get('avg_order_value', 0.0)),
                                    'platform': 'Shopee',
                                    'data_source': 'ads_api_v2_product_campaign_daily_performance'
                                }
                                daily_data.append(daily_record)
                        
                        # Rate limiting between pages
                        time.sleep(1)
            
            else:
                print(f"   ⚠️ No performance data returned for period")
                
        except Exception as e:
            print(f"   ❌ Error fetching daily performance data: {e}")
        
        return daily_data
    
    def _aggregate_daily_to_monthly(self, daily_data, period_start_date):
        """
        Aggregate daily performance data to monthly summaries
        """
        if not daily_data:
            return []
        
        from collections import defaultdict
        
        # Group by month and campaign
        monthly_groups = defaultdict(lambda: {
            'impressions': 0,
            'clicks': 0,
            'spend': 0.0,
            'conversions': 0,
            'gmv': 0.0,
            'orders': 0,
            'sales': 0.0,
            'units_sold': 0,
            'days_count': 0,
            'campaign_info': {},
            'products': set()
        })
        
        for record in daily_data:
            if not record.get('date'):
                continue
                
            try:
                # Extract year-month from date
                record_date = datetime.strptime(record['date'], '%Y-%m-%d')
                month_key = record_date.strftime('%Y-%m')
                campaign_id = record.get('campaign_id', 'unknown')
                
                # Create unique key for month-campaign combination
                group_key = f"{month_key}_{campaign_id}"
                
                # Aggregate metrics
                group = monthly_groups[group_key]
                group['impressions'] += record.get('impressions', 0)
                group['clicks'] += record.get('clicks', 0)
                group['spend'] += record.get('spend', 0.0)
                group['conversions'] += record.get('conversions', 0)
                group['gmv'] += record.get('gmv', 0.0)
                group['orders'] += record.get('orders', 0)
                group['sales'] += record.get('sales', 0.0)
                group['units_sold'] += record.get('units_sold', 0)
                group['days_count'] += 1
                
                # Store campaign info
                if not group['campaign_info']:
                    group['campaign_info'] = {
                        'campaign_id': campaign_id,
                        'campaign_name': record.get('campaign_name', f'Campaign_{campaign_id}'),
                        'month': month_key,
                        'period_start': record_date.replace(day=1).strftime('%Y-%m-%d')
                    }
                
                # Track unique products
                if record.get('product_id'):
                    group['products'].add(record.get('product_id'))
                    
            except ValueError as e:
                print(f"   ⚠️ Invalid date format in record: {record.get('date')} - {e}")
                continue
        
        # Convert aggregated groups to monthly records
        monthly_records = []
        for group_key, group_data in monthly_groups.items():
            campaign_info = group_data['campaign_info']
            
            # Calculate monthly averages and totals
            monthly_record = {
                # Unique identifier for deduplication
                'unique_key': f"{campaign_info['campaign_id']}_{campaign_info['month']}",
                
                # Campaign info
                'campaign_id': campaign_info['campaign_id'],
                'campaign_name': campaign_info['campaign_name'],
                'campaign_type': 'product_campaign',
                
                # Time info
                'date': campaign_info['period_start'],  # First day of month
                'month': campaign_info['month'],
                'period_start': campaign_info['period_start'],
                'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'days_with_data': group_data['days_count'],
                
                # Aggregated performance metrics (monthly totals)
                'impressions': group_data['impressions'],
                'clicks': group_data['clicks'],
                'spend': round(group_data['spend'], 2),
                'conversions': group_data['conversions'],
                'gmv': round(group_data['gmv'], 2),
                'orders': group_data['orders'],
                'sales': round(group_data['sales'], 2),
                'units_sold': group_data['units_sold'],
                'products_count': len(group_data['products']),
                
                # Calculated metrics (monthly averages)
                'ctr': round((group_data['clicks'] / max(group_data['impressions'], 1)) * 100, 2),
                'cpc': round(group_data['spend'] / max(group_data['clicks'], 1), 2),
                'conversion_rate': round((group_data['conversions'] / max(group_data['clicks'], 1)) * 100, 2),
                'roas': round(group_data['gmv'] / max(group_data['spend'], 1), 2),
                'avg_daily_spend': round(group_data['spend'] / max(group_data['days_count'], 1), 2),
                'avg_order_value': round(group_data['sales'] / max(group_data['orders'], 1), 2),
                'cost_per_order': round(group_data['spend'] / max(group_data['orders'], 1), 2),
                'units_per_order': round(group_data['units_sold'] / max(group_data['orders'], 1), 2),
                
                # Platform identifier
                'platform': 'Shopee',
                'data_source': 'ads_api_v2_gms_monthly_aggregate',
                'aggregation_method': 'daily_to_monthly'
            }
            
            monthly_records.append(monthly_record)
        
        # Sort by date
        monthly_records.sort(key=lambda x: x['date'])
        
        print(f"   📅 Aggregated to {len(monthly_records)} monthly campaign records")
        return monthly_records

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
            query_path = f"{path}?item_id_list={batch_str}"
            
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
    
    def extract_product_categories(self, start_fresh=False):
        """
        Extract product category information using /api/v2/product/get_category
        Decrypts category names for product_category mapping
        Saves to shopee_productcategory_raw.json
        
        Args:
            start_fresh (bool): Whether to start fresh or use existing data
            
        Returns:
            list: List of category data
        """
        filename = 'shopee_productcategory_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📋 Found {len(existing_data)} existing categories. Use start_fresh=True to re-extract.")
                return existing_data
        
        print("🔍 Starting product category extraction...")
        
        # Get all categories
        path = "/api/v2/product/get_category"
        language = "en"  # Get English category names
        
        query_path = f"{path}?language={language}"
        
        data = self._make_api_call(query_path, method="GET", call_type="product-categories")
        
        if data and 'response' in data:
            categories = data['response'].get('category_list', [])
            print(f"📦 Extracted {len(categories)} product categories")
            
            self._save_to_json(categories, filename)
            print(f"💾 Saved category data to {filename}")
            return categories
        else:
            print("❌ Failed to extract product categories")
            return []
    
    def extract_product_variants(self, start_fresh=False):
        """
        Extract product model list using /api/v2/product/get_model_list
        Gets variation options for products with models
        Saves to shopee_product_variant_raw.json
        
        Args:
            start_fresh (bool): Whether to start fresh or use existing data
            
        Returns:
            list: List of product model lists
        """
        filename = 'shopee_product_variant_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"📋 Found {len(existing_data)} existing model lists. Use start_fresh=True to re-extract.")
                return existing_data
        
        print("🔍 Starting product model list extraction...")
        
        # Load products to get item_ids that have variations
        products = self._load_from_json('shopee_products_raw.json')
        if not products:
            print("❌ No products found. Please extract products first.")
            return []
        
        # Filter products that have variations (has_model = true)
        variant_item_ids = []
        for product in products:
            if product.get('has_model', False):
                variant_item_ids.append(product.get('item_id'))
        
        if not variant_item_ids:
            print("ℹ️ No products with variations found.")
            return []
        
        print(f"📦 Found {len(variant_item_ids)} products with variations")
        
        all_model_lists = []
        batch_size = 50  # Process in batches to avoid API limits
        total_batches = math.ceil(len(variant_item_ids) / batch_size)
        
        path = "/api/v2/product/get_model_list"
        
        for i in range(0, len(variant_item_ids), batch_size):
            if self.api_calls_made >= self.max_daily_calls:
                print("⚠️ Daily API limit reached")
                break
            
            batch = variant_item_ids[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            # Create item_id_list parameter
            item_id_list = ','.join(str(id) for id in batch)
            query_path = f"{path}?item_id_list={item_id_list}"
            
            print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch)} products)")
            
            data = self._make_api_call(query_path, method="GET", call_type=f"product-models-batch-{batch_num}")
            
            if data and 'response' in data:
                model_lists = data['response'].get('model_list', [])
                all_model_lists.extend(model_lists)
                print(f"  └── Batch {batch_num}: +{len(model_lists)} model lists (total: {len(all_model_lists)})")
            else:
                print(f"  └── Batch {batch_num}: No data returned or error.")
            
            # Rate limiting between batches
            time.sleep(0.5)
        
        self._save_to_json(all_model_lists, filename)
        print(f"🎉 Product model list extraction complete! Total: {len(all_model_lists)} model lists saved to {filename}")
        return all_model_lists
    
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
                query_path = f"{path}?item_id={item_id}&page_size=50"
                
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
    
    def extract_review_details(self, review_ids=None, start_fresh=False, incremental=True):
        """
        Step 2: Extract detailed review information with incremental updates
        For Shopee, reviews are already collected in step 1, so this processes and enriches them
        Handles duplicates using comment_id as unique identifier
        Saves detailed reviews to shopee_productreview_raw.json
        
        Args:
            review_ids (list): List of review entries to process
            start_fresh (bool): Whether to start fresh or append to existing data
            incremental (bool): Use incremental update logic
        
        Returns:
            list: List of detailed review data
        """
        filename = 'shopee_productreview_raw.json'
        
        # Load existing data
        existing_reviews = self._load_from_json(filename)
        
        if not start_fresh and not incremental and existing_reviews:
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
        new_reviews = []
        
        for entry in review_ids:
            if isinstance(entry, dict):
                review_detail = {
                    'item_id': entry.get('item_id'),
                    'comment_id': entry.get('comment_id'),
                    'rating': entry.get('rating'),
                    'created_at': entry.get('created_at'),
                    'review_type': 'product_based'
                }
                new_reviews.append(review_detail)
        
        # Merge with existing data, removing duplicates by comment_id
        if existing_reviews and not start_fresh and incremental:
            print(f"🔄 Merging {len(new_reviews)} new reviews with {len(existing_reviews)} existing reviews...")
            all_reviews = self._remove_duplicates_by_id(existing_reviews, new_reviews, 'comment_id')
            print(f"📊 Merged result: {len(all_reviews)} total reviews (duplicates removed by comment_id)")
        else:
            all_reviews = new_reviews
        
        # Save reviews
        self._save_to_json(all_reviews, filename)
        
        print(f"\n🎉 Review details processing complete!")
        print(f"   Total reviews processed: {len(all_reviews)}")
        print(f"   New reviews added: {len(new_reviews)}")
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
    
    def run_incremental_extraction(self, end_date='2025-10-31'):
        """
        Run intelligent incremental extraction that automatically detects last extraction dates
        and continues from the beginning of the last month until October 31, 2025
        
        This method:
        1. Checks existing data files for last extraction dates
        2. Determines optimal restart point (beginning of last month)
        3. Extracts only new data from restart point to end_date
        4. Merges with existing data, handling duplicates intelligently
        5. Preserves existing data while adding new records
        
        Args:
            end_date (str): End date for extraction (YYYY-MM-DD format)
        
        Returns:
            dict: Results summary with extraction statistics
        """
        print("🔄 Shopee Incremental Data Extraction")
        print("=" * 50)
        
        # Convert end_date to datetime if string
        if isinstance(end_date, str):
            target_end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            target_end_date = end_date
        
        # Check existing data and determine extraction plan
        last_dates = self._find_last_extraction_date()
        should_start_fresh, start_date = self._should_start_fresh_extraction(last_dates)
        
        print(f"📊 Extraction Plan:")
        print(f"   Start Date: {start_date.strftime('%Y-%m-%d')}")
        print(f"   End Date: {target_end_date.strftime('%Y-%m-%d')}")
        print(f"   Strategy: {'Fresh extraction' if should_start_fresh else 'Incremental update'}")
        
        results = {}
        
        # Extract orders with incremental logic
        print(f"\n🔹 Step 1: Extracting Orders")
        print("-" * 30)
        orders = self.extract_all_orders(
            start_date=start_date,
            end_date=target_end_date,
            start_fresh=should_start_fresh,
            incremental=True
        )
        results['orders'] = len(orders) if orders else 0
        
        # Extract order items based on the orders
        print(f"\n🔹 Step 2: Extracting Order Items")
        print("-" * 30)
        order_items = self.extract_all_order_items(
            orders_data=orders,
            start_fresh=should_start_fresh,
            incremental=True
        )
        results['order_items'] = len(order_items) if order_items else 0
        
        # Extract traffic metrics (placeholder for Shopee)
        print(f"\n🔹 Step 3: Traffic Metrics (Placeholder)")
        print("-" * 30)
        traffic = self.extract_traffic_metrics(
            start_date=start_date,
            end_date=target_end_date,
            start_fresh=should_start_fresh
        )
        results['traffic'] = len(traffic) if traffic else 0
        
        # Extract product reviews if we have new data
        if results['orders'] > 0:
            print(f"\n🔹 Step 4: Product Reviews")
            print("-" * 30)
            reviews = self.extract_product_reviews(start_fresh=False)
            results['reviews'] = len(reviews) if reviews else 0
        else:
            print(f"\n🔹 Step 4: Skipping Product Reviews (no new orders)")
            results['reviews'] = 0
        
        # Summary
        print(f"\n" + "=" * 50)
        print(f"✅ INCREMENTAL EXTRACTION COMPLETE")
        print(f"=" * 50)
        print(f"📊 Results Summary:")
        print(f"   Orders: {results['orders']:,}")
        print(f"   Order Items: {results['order_items']:,}")
        print(f"   Traffic Records: {results['traffic']:,}")
        print(f"   Reviews: {results['reviews']:,}")
        print(f"🔄 API Calls Used: {self.api_calls_made}/{self.max_daily_calls}")
        print(f"📁 Data saved to: {self.staging_dir}")
        
        return results
    
    def check_extraction_status(self):
        """
        Check the status of existing extractions and provide recommendations
        """
        print("📊 Shopee Extraction Status Report")
        print("=" * 50)
        
        # Check each data type
        data_files = {
            'Orders': 'shopee_orders_raw.json',
            'Order Items': 'shopee_multiple_order_items_raw.json', 
            'Products': 'shopee_products_raw.json',
            'Product Details': 'shopee_productitem_raw.json',
            'Traffic': 'shopee_reportoverview_raw.json',
            'Reviews': 'shopee_productreview_raw.json'
        }
        
        total_records = 0
        status_report = {}
        
        for data_type, filename in data_files.items():
            data = self._load_from_json(filename)
            count = len(data) if data else 0
            total_records += count
            
            # Get last date for time-based data
            last_date = None
            if data_type == 'Orders' and data:
                last_date = self._get_last_date_from_data(data, 'create_time')
            elif data_type == 'Reviews' and data:
                last_date = self._get_last_date_from_data(data, 'ctime')
            
            status_report[data_type] = {
                'count': count,
                'last_date': last_date,
                'file_exists': count > 0
            }
            
            # Display status
            status_icon = "✅" if count > 0 else "❌"
            date_str = f" (latest: {last_date.strftime('%Y-%m-%d')})" if last_date else ""
            print(f"   {status_icon} {data_type}: {count:,} records{date_str}")
        
        print(f"\n📊 Total Records: {total_records:,}")
        
        # Recommendations
        print(f"\n💡 Recommendations:")
        
        # Check if we have recent data
        has_recent_data = False
        thirty_days_ago = datetime.now() - timedelta(days=30)
        
        for data_type, info in status_report.items():
            if info['last_date'] and info['last_date'] > thirty_days_ago:
                has_recent_data = True
                break
        
        if not has_recent_data:
            print(f"   🔄 Run incremental extraction to get latest data")
            print(f"   📅 No data found within last 30 days")
        else:
            print(f"   ✅ Recent data found - ready for incremental updates")
            print(f"   🔄 Use run_incremental_extraction() for latest updates")
        
        # Check for missing data types
        missing_data = [data_type for data_type, info in status_report.items() if info['count'] == 0]
        if missing_data:
            print(f"   ⚠️ Missing data types: {', '.join(missing_data)}")
            print(f"   💡 Consider running complete extraction first")
        
        return status_report
    
    def run_complete_extraction(self, start_fresh=False):
        """
        Run complete data extraction in optimal order
        """
        print("🚀 Starting COMPLETE Shopee data extraction...")
        print("=" * 60)
        
        extraction_plan = [
            ("Products", self.extract_all_products),
            ("Product Categories", self.extract_product_categories),
            ("Product Variants", self.extract_product_variants),
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


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def run_incremental_extraction(end_date='2025-10-31'):
    """
    Run incremental extraction that intelligently updates from last extraction date
    Handles duplicates and continues until Oct 31, 2025
    """
    extractor = ShopeeDataExtractor()
    return extractor.run_incremental_extraction(end_date=end_date)

def run_full_extraction(start_fresh=False, end_date='2025-10-31'):
    """Run complete extraction with all data until Oct 31, 2025"""
    extractor = ShopeeDataExtractor()
    return extractor.run_complete_extraction(start_fresh=start_fresh)

def extract_recent_data(days_back=30):
    """Extract only recent data (last N days) to save API calls"""
    extractor = ShopeeDataExtractor()
    
    # Extract recent orders
    recent_start = datetime.now() - timedelta(days=days_back)
    recent_end = datetime.now()
    
    orders = extractor.extract_all_orders(start_date=recent_start, end_date=recent_end, incremental=True)
    order_items = extractor.extract_all_order_items(orders_data=orders, incremental=True)
    traffic = extractor.extract_traffic_metrics(incremental=True)
    
    return {
        'orders': orders,
        'order_items': order_items,
        'traffic': traffic
    }

def check_extraction_status():
    """
    Check the status of existing extractions and provide recommendations
    """
    extractor = ShopeeDataExtractor()
    return extractor.check_extraction_status()

def extract_ads_data_only():
    """
    Extract only Shopee ads data using monthly aggregation
    Returns advertising metrics using v2.ads.get_ad_data API
    """
    extractor = ShopeeDataExtractor()
    return extractor.extract_traffic_metrics(
        start_date=None,  # Auto-detect from existing data
        end_date=None,    # Use October 31, 2025
        start_fresh=False,  # Incremental extraction
        monthly_aggregate=True,  # Monthly aggregation
        incremental=True   # Append to existing data
    )

def extract_product_reviews_only(start_fresh=False, limit_products=None):
    """Extract only product reviews (standalone function)"""
    extractor = ShopeeDataExtractor()
    return extractor.extract_product_reviews(start_fresh=start_fresh, limit_products=limit_products)

def extract_product_categories_only(start_fresh=False):
    """Extract only product categories using /api/v2/product/get_category"""
    extractor = ShopeeDataExtractor()
    return extractor.extract_product_categories(start_fresh=start_fresh)

def extract_product_variants_only(start_fresh=False):
    """Extract only product model lists using /api/v2/product/get_model_list"""
    extractor = ShopeeDataExtractor()
    return extractor.extract_product_variants(start_fresh=start_fresh)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("🚀 Shopee Data Extraction with Incremental Updates")
    print("=" * 60)
    print("Choose extraction mode:")
    print("1. 🔄 Incremental extraction (RECOMMENDED)")
    print("   - Automatically detects last extraction date")
    print("   - Starts from beginning of that month") 
    print("   - Continues to Oct 31, 2025")
    print("   - Handles duplicates and status updates")
    print("")
    print("2. 📊 Check extraction status")
    print("   - View current data status and recommendations")
    print("")
    print("3. 🆕 Complete fresh extraction")
    print("   - Re-extracts all data from 2020-04-01 to 2025-10-31")
    print("   - Uses more API calls")
    print("")
    print("4. 📈 Recent data only (last 30 days)")
    print("   - Quick extraction for recent updates")
    print("")
    print("5. ⭐ Product reviews only")
    print("   - Extract product reviews using 2-step process")
    print("")
    print("6. 📊 Ads data extraction (Monthly Aggregate)")
    print("   - Extract advertising metrics using v2.ads.get_ad_data")
    print("   - Monthly aggregated data for analysis")
    print("")
    print("7. 🏷️ Product categories only")
    print("   - Extract category data using /api/v2/product/get_category")
    print("   - Saves to shopee_productcategory_raw.json")
    print("")
    print("8. 🔧 Product variants only")
    print("   - Extract model lists using /api/v2/product/get_model_list")
    print("   - Saves to shopee_product_variant_raw.json")
    
    choice = input("\nEnter choice (1-8): ").strip()
    
    if choice == "1":
        print("\n🔄 Starting incremental extraction...")
        results = run_incremental_extraction()
        print(f"\n✅ Incremental extraction completed!")
        
    elif choice == "2":
        print("\n📊 Checking extraction status...")
        check_extraction_status()
        
    elif choice == "3":
        print("\n🆕 Starting complete fresh extraction...")
        confirm = input("This will overwrite existing data. Continue? (y/N): ").strip().lower()
        if confirm == 'y':
            results = run_full_extraction(start_fresh=True)
        else:
            print("❌ Extraction cancelled.")
            
    elif choice == "4":
        print("\n📈 Extracting recent data (last 30 days)...")
        try:
            results = extract_recent_data(days_back=30)
            print(f"✅ Recent data extraction completed!")
            print(f"   Orders: {len(results['orders'])}")
            print(f"   Order Items: {len(results['order_items'])}")
            print(f"   Traffic: {len(results['traffic'])}")
        except Exception as e:
            print(f"❌ Error: {e}")
            
    elif choice == "5":
        print("\n⭐ Extracting product reviews...")
        try:
            limit = input("Limit number of products (press Enter for all): ").strip()
            limit_products = int(limit) if limit.isdigit() else None
            
            reviews = extract_product_reviews_only(start_fresh=False, limit_products=limit_products)
            print(f"✅ Review extraction completed! Total: {len(reviews)} reviews")
        except Exception as e:
            print(f"❌ Error: {e}")
        
    elif choice == "6":
        print("\n📊 Extracting Shopee Ads Data (Monthly Aggregate)...")
        try:
            print("🎯 Starting ads data extraction with monthly aggregation...")
            ads_data = extract_ads_data_only()
            
            if ads_data:
                print(f"✅ Ads extraction completed! Total: {len(ads_data)} records")
                print(f"📁 Data saved to: app/Staging/shopee_reportoverview_raw.json")
                
                # Show sample
                if ads_data:
                    sample = ads_data[0]
                    print(f"\n📋 Sample record:")
                    print(f"   Campaign: {sample.get('campaign_name', 'N/A')}")
                    print(f"   Date: {sample.get('date', 'N/A')}")
                    print(f"   Impressions: {sample.get('impressions', 0):,}")
                    print(f"   Spend: ${sample.get('spend', 0):.2f}")
                    print(f"   ROAS: {sample.get('roas', 0):.2f}")
            else:
                print("⚠️ No ads data extracted - check campaigns and date range")
                
        except Exception as e:
            print(f"❌ Error during ads extraction: {e}")
        
    elif choice == "7":
        print("\n🏷️ Extracting product categories...")
        try:
            categories = extract_product_categories_only(start_fresh=True)
            print(f"✅ Category extraction completed! Total: {len(categories)} categories")
            print(f"📁 Data saved to: app/Staging/shopee_productcategory_raw.json")
            
            # Show sample categories
            if categories:
                print(f"\n📋 Sample categories:")
                for i, cat in enumerate(categories[:5]):
                    print(f"   - {cat.get('category_id', 'N/A')}: {cat.get('category_name', 'N/A')}")
                    if i >= 4:
                        break
        except Exception as e:
            print(f"❌ Error: {e}")
            
    elif choice == "8":
        print("\n🔧 Extracting product model lists...")
        try:
            variants = extract_product_variants_only(start_fresh=True)
            print(f"✅ Model list extraction completed! Total: {len(variants)} model lists")
            print(f"📁 Data saved to: app/Staging/shopee_product_variant_raw.json")
            
            # Show sample variant info
            if variants:
                print(f"\n📋 Sample model lists:")
                for i, variant in enumerate(variants[:3]):
                    item_id = variant.get('item_id', 'N/A')
                    model_count = len(variant.get('model_list', []))
                    print(f"   - Item {item_id}: {model_count} models")
                    if i >= 2:
                        break
        except Exception as e:
            print(f"❌ Error: {e}")
        
    else:
        print("❌ Invalid choice. Please run again and select 1-8.")
    
    print(f"\n🎉 Extraction completed!")
    print(f"📁 Check the app/Staging/ directory for JSON files")
    print(f"💡 Next time, just run option 1 (incremental) to get only new data!")
