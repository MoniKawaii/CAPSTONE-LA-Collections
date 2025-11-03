try:
    import lazop_sdk as lazop
    # Make sure the classes are available
    if hasattr(lazop, 'LazopClient') and hasattr(lazop, 'LazopRequest'):
        print("✅ Lazop SDK imported successfully")
    else:
        raise ImportError("Lazop classes not found")
except ImportError:
    try:
        import lazop
        print("✅ Lazop imported as fallback")
    except ImportError:
        print("⚠️ Warning: lazop-sdk not found. Please install with: pip install lazop-sdk")
        lazop = None
    
import json
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys
import os
import time
import math
from collections import defaultdict

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import LAZADA_TOKENS, LAZADA_API_URL, DIM_PRODUCT_COLUMNS, DIM_ORDER_COLUMNS, FACT_ORDERS_COLUMNS, FACT_TRAFFIC_COLUMNS, DIM_CUSTOMER_COLUMNS
except ImportError:
    # Fallback for VS Code or different environments
    try:
        from app.config import LAZADA_TOKENS, LAZADA_API_URL, DIM_PRODUCT_COLUMNS, DIM_ORDER_COLUMNS, FACT_ORDERS_COLUMNS, FACT_TRAFFIC_COLUMNS, DIM_CUSTOMER_COLUMNS
    except ImportError:
        print("Warning: Could not import config. Make sure config.py is accessible.")
        # Define minimal fallbacks if needed
        LAZADA_TOKENS = {}
        LAZADA_API_URL = "https://api.lazada.com.ph/rest"

class LazadaDataExtractor:
    def extract_product_details(self, products_data=None, start_fresh=False):
        """
        Extract detailed product info for all item_ids in lazada_products_raw.json
        Uses /product/item/get in batches of 50
        Saves to lazada_productitem_raw.json
        """
        import time
        import math
        filename = 'lazada_productitem_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"Found {len(existing_data)} existing product details. Use start_fresh=True to re-extract.")
                return existing_data
        
        # Load products if not provided
        if products_data is None:
            products_data = self._load_from_json('lazada_products_raw.json')
        if not products_data:
            print("No products found to extract details for.")
            return []
        
        all_details = []
        item_ids = [str(prod.get('item_id')) for prod in products_data if prod.get('item_id')]
        batch_size = 50
        total_batches = math.ceil(len(item_ids) / batch_size)
        print(f"🔍 Extracting product details for {len(item_ids)} items in {total_batches} batches of {batch_size}...")
        
        for i in range(0, len(item_ids), batch_size):
            batch = item_ids[i:i+batch_size]
            batch_str = ','.join(batch)
            request = lazop.LazopRequest('/product/item/get', 'GET')
            request.add_api_param('item_ids', f'[{batch_str}]')
            data = self._make_api_call(request, f'product-item-batch-{i//batch_size+1}')
            if data and 'data' in data:
                # Lazada returns a list of items in data['data']['items']
                items = data['data'].get('items', [])
                all_details.extend(items)
                print(f"  └── Batch {i//batch_size+1}: +{len(items)} items (total: {len(all_details)})")
            else:
                print(f"  Batch {i//batch_size+1}: No data returned or error.")
            time.sleep(0.2)  # Be nice to the API
        
        self._save_to_json(all_details, filename)
        print(f"🎉 Product details extraction complete! Total: {len(all_details)} items saved to {filename}")
        return all_details
    """
    Lazada API data extraction class with batch processing and JSON storage
    Optimized for 10,000 API calls per day limit
    """
    
    def __init__(self):
        self.url = LAZADA_API_URL
        self.app_key = LAZADA_TOKENS["app_key"]
        self.app_secret = LAZADA_TOKENS["app_secret"]
        self.access_token = LAZADA_TOKENS["access_token"]
        
        # Initialize client only if lazop is available
        if lazop:
            self.client = lazop.LazopClient(self.url, self.app_key, self.app_secret)
        else:
            self.client = None
            print("⚠️ Warning: lazop SDK not available. API calls will not work.")
        
        # API call tracking
        self.api_calls_made = 0
        self.max_daily_calls = 10000
        self.batch_size = 50  # Safe batch size for products
        self.orders_batch_size = 100  # Maximum for orders API
        
        # Storage paths
        self.staging_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Staging')
        os.makedirs(self.staging_dir, exist_ok=True)
        
        print(f"Lazada Extractor initialized")
        print(f"Staging directory: {self.staging_dir}")
        print(f"Daily API limit: {self.max_daily_calls}")
        if not lazop:
            print("📊 Running in status-check-only mode (no API capabilities)")
    
    def _make_api_call(self, request, call_type="general"):
        """Make API call with rate limiting and tracking"""
        if not self.client:
            print("❌ Cannot make API calls: lazop SDK not available")
            return None
            
        if self.api_calls_made >= self.max_daily_calls:
            print(f"Daily API limit ({self.max_daily_calls}) reached!")
            return None
        
        try:
            response = self.client.execute(request, self.access_token)
            self.api_calls_made += 1
            
            # Check for successful response (code "0" means success)
            response_code = getattr(response, 'code', None)
            print(f"API Call #{self.api_calls_made} ({call_type}) - Code: {response_code}")
            
            if response_code != "0":
                error_msg = getattr(response, 'message', 'Unknown error')
                print(f"API Error - Code: {response_code}, Message: {error_msg}")
                
                # Handle rate limit errors with automatic retry
                if response_code == "ApiCallLimit":
                    import time
                    print(f"   ⏳ Rate limit hit! Waiting 60 seconds before retry...")
                    time.sleep(60)
                    print(f"   🔄 Retrying API call...")
                    # Retry the same request once
                    retry_response = self.client.execute(request, self.access_token)
                    retry_code = getattr(retry_response, 'code', None)
                    print(f"   Retry - Code: {retry_code}")
                    if retry_code == "0" and hasattr(retry_response, 'body') and retry_response.body:
                        return retry_response.body
                
                return None
            
            # Longer delay to prevent API frequency limits
            import time
            time.sleep(1.5)  # 1.5 seconds between API calls to avoid rate limiting
            
            # Return the response body (should be a dict)
            if hasattr(response, 'body') and response.body:
                return response.body
            else:
                print(f"Response has no body")
                return None
            
        except Exception as e:
            print(f"API call failed: {e}")
            return None
    
    def _save_to_json(self, data, filename):
        """Save data to JSON file in staging directory"""
        filepath = os.path.join(self.staging_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        print(f"Saved {len(data) if isinstance(data, list) else 1} records to {filename}")
    
    def _get_last_date_from_data(self, data, date_field='created_at'):
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
            date_str = record.get(date_field)
            if date_str:
                try:
                    # Handle various date formats
                    if 'T' in str(date_str):
                        # ISO format with time
                        parsed_date = datetime.fromisoformat(str(date_str).replace('Z', '+00:00').replace('+08:00', ''))
                    else:
                        # Try parsing as date only
                        parsed_date = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
                    
                    if last_date is None or parsed_date > last_date:
                        last_date = parsed_date
                except (ValueError, TypeError) as e:
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
        orders_data = self._load_from_json('lazada_orders_raw.json')
        if orders_data:
            last_dates['orders'] = self._get_last_date_from_data(orders_data, 'created_at')
        
        # Check traffic data  
        traffic_data = self._load_from_json('lazada_reportoverview_raw.json')
        if traffic_data:
            # Traffic data might have different date field structure
            traffic_last = None
            for record in traffic_data:
                if 'time_key' in record:
                    try:
                        time_key_str = str(record['time_key'])
                        if len(time_key_str) >= 8:
                            parsed_date = datetime.strptime(time_key_str[:8], '%Y%m%d')
                            if traffic_last is None or parsed_date > traffic_last:
                                traffic_last = parsed_date
                    except ValueError:
                        continue
            last_dates['traffic'] = traffic_last
        
        # Check product reviews
        reviews_data = self._load_from_json('lazada_productreview_raw.json')
        if reviews_data:
            last_dates['reviews'] = self._get_last_date_from_data(reviews_data, 'created_at')
        
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
                existing_by_id[str(record_id)] = record
        
        # Add/update with new records
        for record in new_data:
            record_id = record.get(id_field)
            if record_id:
                existing_by_id[str(record_id)] = record  # This will overwrite if duplicate
        
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
            return True, datetime(2020, 4, 1)
        
        # Find the most recent date across all data types
        most_recent = None
        for date_val in last_dates.values():
            if date_val and (most_recent is None or date_val > most_recent):
                most_recent = date_val
        
        if most_recent:
            # Start from the beginning of the month containing the most recent date
            start_from = self._get_month_start_date(most_recent)
            print(f"📅 Last data found: {most_recent.strftime('%Y-%m-%d')}")
            print(f"📅 Will restart extraction from: {start_from.strftime('%Y-%m-%d')} (beginning of that month)")
            return False, start_from
        else:
            return True, datetime(2020, 4, 1)
    
    def _load_from_json(self, filename):
        """Load data from JSON file if it exists"""
        filepath = os.path.join(self.staging_dir, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:  # Check if file has content
                        return json.loads(content)
                    else:
                        print(f"📄 {filename} is empty, starting fresh extraction")
                        return []
            except (json.JSONDecodeError, Exception) as e:
                print(f"Error reading {filename}: {e}. Starting fresh extraction")
                return []
        return []
    
    def extract_all_products(self, start_fresh=False):
        """
        Extract ALL products from Lazada with pagination
        Saves to lazada_products_raw.json
        """
        filename = 'lazada_products_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"Found {len(existing_data)} existing products. Use start_fresh=True to re-extract.")
                return existing_data
        
        print("Starting complete product extraction...")
        all_products = []
        offset = 0
        has_more = True
        
        while has_more and self.api_calls_made < self.max_daily_calls:
            request = lazop.LazopRequest('/products/get', 'GET')
            request.add_api_param('filter', 'all')  # Get all products, not just live
            request.add_api_param('limit', str(self.batch_size))
            request.add_api_param('offset', str(offset))
            request.add_api_param('options', '1')
            
            data = self._make_api_call(request, f"products-offset-{offset}")
            
            if not data:
                break
            
            products = data.get('data', {}).get('products', [])
            
            if not products:
                has_more = False
                print("No more products found")
            else:
                all_products.extend(products)
                offset += self.batch_size
                print(f"Extracted {len(products)} products (Total: {len(all_products)})")
                
                # Save progress periodically
                if len(all_products) % 500 == 0:
                    self._save_to_json(all_products, filename)
        
        # Final save
        self._save_to_json(all_products, filename)
        print(f"🎉 Product extraction complete! Total: {len(all_products)} products")
        return all_products
    
    def extract_all_orders(self, start_date=None, end_date=None, start_fresh=False, incremental=True):
        """
        Extract ALL orders from Lazada with intelligent incremental updates
        Automatically detects last extraction date and continues from there
        Handles duplicates using order_id as unique identifier
        Saves to lazada_orders_raw.json
        
        Args:
            start_date: Override start date (will be auto-detected if None)
            end_date: End date (defaults to Oct 31, 2025)
            start_fresh: Force complete re-extraction
            incremental: Use incremental update logic
        """
        filename = 'lazada_orders_raw.json'
        
        # Load existing data first
        existing_data = self._load_from_json(filename)
        
        if not start_fresh and not incremental and existing_data:
            print(f"📋 Found {len(existing_data)} existing orders. Use start_fresh=True or incremental=True to update.")
            return existing_data
        
        # Determine extraction dates
        if incremental and not start_fresh:
            # Find last extraction dates
            last_dates = self._find_last_extraction_date()
            should_start_fresh, calculated_start = self._should_start_fresh_extraction(last_dates)
            
            if should_start_fresh:
                print("🆕 No existing data found, starting fresh extraction...")
                start_date = calculated_start
                existing_data = []
            else:
                start_date = calculated_start
                print(f"🔄 Incremental update from {start_date.strftime('%Y-%m-%d')}")
        elif start_fresh:
            print("🆕 Starting fresh extraction (start_fresh=True)...")
            existing_data = []
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
        
        # Calculate total days and number of 90-day chunks needed
        total_days = (end_date - start_date).days
        chunk_days = 90  # API maximum
        total_chunks = (total_days // chunk_days) + (1 if total_days % chunk_days > 0 else 0)
        
        print(f"📊 Total period: {total_days} days")
        print(f"📦 Breaking into {total_chunks} chunks of {chunk_days} days each (API limit)")
        
        new_orders = []
        current_start = start_date
        chunk_num = 0
        
        while current_start < end_date:
            chunk_num += 1
            chunk_end = min(current_start + timedelta(days=chunk_days), end_date)
            
            # Format dates for API (Lazada requires ISO format with timezone)
            start_str = current_start.strftime('%Y-%m-%dT00:00:00+08:00')
            end_str = chunk_end.strftime('%Y-%m-%dT23:59:59+08:00')
            
            print(f"\n📅 Chunk {chunk_num}/{total_chunks}: {current_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            # Extract orders for this chunk with pagination
            chunk_orders = self._extract_orders_chunk(start_str, end_str, chunk_num)
            new_orders.extend(chunk_orders)
            
            print(f"✅ Chunk {chunk_num}: Got {len(chunk_orders)} orders (New total: {len(new_orders)})")
            
            # Check API limit
            if self.api_calls_made >= self.max_daily_calls:
                print(f"Daily API limit reached! Stopping at chunk {chunk_num}")
                break
            
            # Move to next chunk
            current_start = chunk_end + timedelta(days=1)
        
        # Merge with existing data, removing duplicates by order_id
        if existing_data and not start_fresh:
            print(f"🔄 Merging {len(new_orders)} new orders with {len(existing_data)} existing orders...")
            all_orders = self._remove_duplicates_by_id(existing_data, new_orders, 'order_id')
            print(f"📊 After deduplication: {len(all_orders)} total orders")
        else:
            all_orders = new_orders
        
        # Final save
        self._save_to_json(all_orders, filename)
        print(f"🎉 Order extraction complete! Total: {len(all_orders)} orders across {chunk_num} chunks")
        print(f"📈 New orders extracted: {len(new_orders)}")
        print(f"🔄 Duplicates handled by order_id deduplication")
        return all_orders
    
    def _extract_orders_chunk(self, start_date_str, end_date_str, chunk_num):
        """Extract orders for a single 90-day chunk with pagination"""
        chunk_orders = []
        offset = 0
        has_more = True
        batch_count = 0
        
        while has_more and self.api_calls_made < self.max_daily_calls:
            batch_count += 1
            request = lazop.LazopRequest('/orders/get', 'GET')
            request.add_api_param('created_after', start_date_str)
            request.add_api_param('created_before', end_date_str)
            request.add_api_param('limit', str(self.orders_batch_size))  # Use 100 for orders
            request.add_api_param('offset', str(offset))
            request.add_api_param('sort_by', 'created_at')
            request.add_api_param('sort_direction', 'ASC')  # Oldest first for historical data
            
            data = self._make_api_call(request, f"chunk-{chunk_num}-batch-{batch_count}")
            
            if not data:
                break
            
            orders = data.get('data', {}).get('orders', [])
            
            if not orders:
                has_more = False
                print(f"  └── No more orders in chunk {chunk_num}")
            else:
                chunk_orders.extend(orders)
                offset += self.orders_batch_size
                print(f"  └── Batch {batch_count}: +{len(orders)} orders (chunk total: {len(chunk_orders)})")
                
                # Save progress every 500 orders
                if len(chunk_orders) % 500 == 0:
                    print(f"  Saving progress... {len(chunk_orders)} orders in current chunk")
        
        return chunk_orders
    
    def extract_all_order_items(self, orders_data=None, start_fresh=False, incremental=True):
        """
        Extract order items for all orders using /orders/items/get API
        Uses incremental update approach based on order data
        Processes up to 50 order IDs per API call (API limitation)
        Saves to lazada_multiple_order_items_raw.json
        """
        filename = 'lazada_multiple_order_items_raw.json'
        
        # Load existing data
        existing_data = self._load_from_json(filename)
        
        if not start_fresh and not incremental and existing_data:
            print(f"Found {len(existing_data)} existing order items. Use start_fresh=True or incremental=True to update.")
            return existing_data
        
        # Load orders if not provided
        if not orders_data:
            orders_data = self._load_from_json('lazada_orders_raw.json')
        
        if not orders_data:
            print("No orders data found. Please extract orders first.")
            return []
        
        # Extract order IDs from orders data
        order_ids = []
        for order in orders_data:
            order_id = order.get('order_id') or order.get('order_number')
            if order_id:
                order_ids.append(str(order_id))
        
        if not order_ids:
            print("No order IDs found in orders data.")
            return []
        
        print(f"Starting order items extraction for {len(order_ids)} orders...")
        print(f"Processing in chunks of 50 order IDs (API requirement)")
        
        # Calculate chunks
        chunk_size = 50
        total_chunks = (len(order_ids) // chunk_size) + (1 if len(order_ids) % chunk_size > 0 else 0)
        
        all_order_items = []
        processed_orders = 0
        
        for chunk_num in range(total_chunks):
            if self.api_calls_made >= self.max_daily_calls:
                print(f"Daily API limit reached! Stopping at chunk {chunk_num}")
                break
            
            # Get chunk of order IDs
            start_idx = chunk_num * chunk_size
            end_idx = min(start_idx + chunk_size, len(order_ids))
            chunk_order_ids = order_ids[start_idx:end_idx]
            
            print(f"\n Chunk {chunk_num + 1}/{total_chunks}: Processing {len(chunk_order_ids)} orders...")
            
            # Create comma-separated list in square brackets format
            order_ids_param = '[' + ','.join(chunk_order_ids) + ']'
            
            # Make API call for this chunk
            request = lazop.LazopRequest('/orders/items/get', 'GET')
            request.add_api_param('order_ids', order_ids_param)
            
            data = self._make_api_call(request, f"order-items-chunk-{chunk_num + 1}")
            
            if data:
                # Debug: Print response structure
                print(f"  Debug: Response type: {type(data)}")
                print(f"  Debug: Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                
                # Extract order items from response - try different possible structures
                items = []
                if isinstance(data, dict):
                    # Try different possible response structures
                    if 'data' in data:
                        data_section = data['data']
                        if isinstance(data_section, dict):
                            items = data_section.get('order_items', []) or data_section.get('items', [])
                        elif isinstance(data_section, list):
                            items = data_section
                    elif 'order_items' in data:
                        items = data['order_items']
                    elif 'items' in data:
                        items = data['items']
                    else:
                        # If data is a dict but doesn't have expected keys, take the values
                        print(f"  🔍 Debug: Unexpected response structure. Data: {data}")
                elif isinstance(data, list):
                    items = data
                
                if items:
                    all_order_items.extend(items)
                    print(f"  Got {len(items)} items from {len(chunk_order_ids)} orders")
                else:
                    print(f"  ⚠️ No items found for this chunk")
            else:
                print(f"  Failed to get items for chunk {chunk_num + 1}")
            
            processed_orders += len(chunk_order_ids)
            
            # Save progress every 10 chunks or when we have 1000+ items
            if (chunk_num + 1) % 10 == 0 or len(all_order_items) >= 1000:
                self._save_to_json(all_order_items, filename)
                print(f"  Progress saved: {len(all_order_items)} items from {processed_orders} orders")
        
        # Final save
        self._save_to_json(all_order_items, filename)
        print(f"Order items extraction complete!")
        print(f"Total: {len(all_order_items)} items from {processed_orders} orders")
        print(f"API calls used: {self.api_calls_made}")
        return all_order_items
    
    def extract_traffic_metrics(self, start_date=None, end_date=None, start_fresh=False, monthly_aggregate=True, incremental=True):
        """
        Extract traffic/advertising metrics with incremental updates
        Automatically detects last extraction date and continues from there
        Saves to lazada_reportoverview_raw.json
        
        Args:
            start_date: Override start date (will be auto-detected if None)
            end_date: End date (defaults to Oct 31, 2025)
            start_fresh: Whether to re-extract all data
            monthly_aggregate: Whether to extract monthly data (True) or single period (False)
            incremental: Use incremental update logic
        """
        filename = 'lazada_reportoverview_raw.json'
        
        # Load existing data
        existing_data = self._load_from_json(filename)
        
        if not start_fresh and not incremental and existing_data:
            print(f"Found existing traffic data. Use start_fresh=True or incremental=True to update.")
            return existing_data
        
        # Determine extraction dates
        if incremental and not start_fresh and existing_data:
            # Find last traffic date from existing data
            last_traffic_date = None
            for record in existing_data:
                if 'time_key' in record:
                    try:
                        time_key_str = str(record['time_key'])
                        if len(time_key_str) >= 8:
                            parsed_date = datetime.strptime(time_key_str[:8], '%Y%m%d')
                            if last_traffic_date is None or parsed_date > last_traffic_date:
                                last_traffic_date = parsed_date
                    except ValueError:
                        continue
            
            if last_traffic_date:
                # Start from the beginning of the month containing the last traffic date
                start_date = self._get_month_start_date(last_traffic_date)
                print(f"📅 Last traffic data: {last_traffic_date.strftime('%Y-%m-%d')}")
                print(f"📅 Incremental update from: {start_date.strftime('%Y-%m-%d')}")
            else:
                start_date = datetime(2022, 10, 1)  # Default start
                existing_data = []
        elif start_fresh:
            print("🆕 Starting fresh traffic extraction...")
            existing_data = []
        
        # Default date range: 2022-10-01 to 2025-10-31 for comprehensive historical data
        if not start_date:
            start_date = datetime(2022, 10, 1)
        elif isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        
        if not end_date:
            end_date = datetime(2025, 10, 31)  # Updated to Oct 31, 2025
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        if monthly_aggregate:
            print(f"Extracting monthly traffic metrics from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
            new_traffic_data = self._extract_monthly_traffic(start_date, end_date, filename)
        else:
            print(f"Extracting single period traffic metrics from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
            new_traffic_data = self._extract_single_period_traffic(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), filename)
        
        # Merge with existing data if incremental
        if existing_data and not start_fresh and incremental:
            print(f"🔄 Merging {len(new_traffic_data)} new traffic records with {len(existing_data)} existing records...")
            all_traffic_data = self._remove_duplicates_by_id(existing_data, new_traffic_data, 'time_key')
            print(f"📊 After deduplication: {len(all_traffic_data)} total traffic records")
            self._save_to_json(all_traffic_data, filename)
            return all_traffic_data
        else:
            return new_traffic_data
    
    def _extract_monthly_traffic(self, start_date, end_date, filename):
        """Extract traffic data month by month for detailed analysis"""
        from dateutil.relativedelta import relativedelta
        
        monthly_traffic = []
        current_date = start_date
        month_count = 0
        total_months = self._count_months(start_date, end_date)
        
        print(f"📊 Processing {total_months} months of traffic data...")
        
        while current_date < end_date and self.api_calls_made < self.max_daily_calls:
            # Calculate month boundaries
            month_start = current_date
            month_end = min(
                month_start + relativedelta(months=1) - timedelta(days=1),
                end_date
            )
            
            month_count += 1
            print(f"Month {month_count}/{total_months}: {month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')}")
            
            try:
                # Create API request for this month
                request = lazop.LazopRequest('/sponsor/solutions/report/getReportOverview')
                request.add_api_param('startDate', month_start.strftime('%Y-%m-%d'))
                request.add_api_param('endDate', month_end.strftime('%Y-%m-%d'))
                request.add_api_param('lastStartDate', month_start.strftime('%Y-%m-%d'))
                request.add_api_param('lastEndDate', month_end.strftime('%Y-%m-%d'))
                request.add_api_param('bizCode', 'sponsoredSearch')
                request.add_api_param('useRtTable', 'false')
                
                # Make API call
                data = self._make_api_call(request, f"traffic-month-{month_start.strftime('%Y-%m')}")
                
                if data and 'result' in data:
                    result = data['result']
                    
                    # Extract current period metrics
                    if 'reportOverviewDetailDTO' in result:
                        metrics = result['reportOverviewDetailDTO']
                        
                        # Use the middle of the month as representative date
                        mid_month = month_start + timedelta(days=15)
                        time_key = int(mid_month.strftime('%Y%m%d'))
                        
                        traffic_record = {
                            'time_key': time_key,
                            'date': mid_month.strftime('%Y-%m-%d'),
                            'year_month': month_start.strftime('%Y-%m'),
                            'platform_key': 1,  # Lazada
                            'platform_name': 'Lazada',
                            
                            # Core Fact_Traffic measures
                            'impressions': int(metrics.get('impressions', 0)),
                            'clicks': int(metrics.get('clicks', 0)),
                            
                            # Additional advertising metrics
                            'ctr': float(metrics.get('ctr', 0.0)),
                            'spend': float(metrics.get('spend', 0.0)),
                            'units_sold': int(metrics.get('unitsSold', 0)),
                            'revenue': float(metrics.get('revenue', 0.0)),
                            'cpc': float(metrics.get('cpc', 0.0)),
                            'roi': float(metrics.get('roi', 0.0)),
                            
                            # Metadata
                            'period_start': month_start.strftime('%Y-%m-%d'),
                            'period_end': month_end.strftime('%Y-%m-%d'),
                            'granularity': 'monthly',
                            'extraction_timestamp': datetime.now().isoformat()
                        }
                        
                        monthly_traffic.append(traffic_record)
                        
                        print(f"   Impressions: {metrics.get('impressions', 0):,} | "
                              f"Clicks: {metrics.get('clicks', 0):,} | "
                              f"CTR: {float(metrics.get('ctr', 0)):,.2f}%")
                    else:
                        print(f"     No reportOverviewDetailDTO data for this month")
                else:
                    print(f"    No valid data returned for this month")
                
                # Small delay to be respectful to API
                time.sleep(0.2)
                
            except Exception as e:
                print(f"   Error extracting month {month_start.strftime('%Y-%m')}: {str(e)}")
            
            # Move to next month
            current_date = month_start + relativedelta(months=1)
        
        # Save monthly data
        self._save_to_json(monthly_traffic, filename)
        
        print(f"\n Monthly traffic extraction complete!")
        print(f" Total months extracted: {len(monthly_traffic)}")
        print(f" Total API calls used: {self.api_calls_made}")
        
        # Display summary statistics
        if monthly_traffic:
            total_impressions = sum(r['impressions'] for r in monthly_traffic)
            total_clicks = sum(r['clicks'] for r in monthly_traffic)
            avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
            total_revenue = sum(r['revenue'] for r in monthly_traffic)
            total_spend = sum(r['spend'] for r in monthly_traffic)
            
            print(f"\n📈 Summary Statistics:")
            print(f"   Total Impressions: {total_impressions:,}")
            print(f"   Total Clicks: {total_clicks:,}")
            print(f"   Average CTR: {avg_ctr:.2f}%")
            print(f"   Total Revenue: ₱{total_revenue:,.2f}")
            print(f"   Total Spend: ₱{total_spend:,.2f}")
            print(f"   Overall ROI: {total_revenue/total_spend:.2f}" if total_spend > 0 else "   Overall ROI: N/A")
        
        return monthly_traffic
    
    def _extract_single_period_traffic(self, start_date, end_date, filename):
        """Extract traffic data for a single period (legacy method)"""
        request = lazop.LazopRequest('/sponsor/solutions/report/getReportOverview')
        request.add_api_param('startDate', start_date)
        request.add_api_param('endDate', end_date)
        request.add_api_param('lastStartDate', start_date)
        request.add_api_param('lastEndDate', end_date)
        request.add_api_param('bizCode', 'sponsoredSearch')
        request.add_api_param('useRtTable', 'false')
        
        data = self._make_api_call(request, "traffic-metrics-single")
        
        if data:
            traffic_data = [data]  # Wrap in list for consistency
            self._save_to_json(traffic_data, filename)
            print(f" Traffic metrics extraction complete!")
            return traffic_data
        else:
            print(" Failed to extract traffic metrics")
            return []
    
    def _count_months(self, start_date, end_date):
        """Helper to calculate number of months between dates"""
        from dateutil.relativedelta import relativedelta
        months = 0
        current = start_date
        while current < end_date:
            months += 1
            current += relativedelta(months=1)
        return months
    
    def extract_product_details(self, product_ids=None, start_fresh=False):
        """
        Extract detailed product information
        Saves to lazada_productitem_raw.json
        """
        filename = 'lazada_productitem_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f" Found {len(existing_data)} existing product details. Use start_fresh=True to re-extract.")
                return existing_data
        
        # Get product IDs from products data if not provided
        if not product_ids:
            products_data = self._load_from_json('lazada_products_raw.json')
            product_ids = [p.get('item_id') for p in products_data if p.get('item_id')][:100]  # Limit to first 100
        
        if not product_ids:
            print(" No product IDs found. Please extract products first.")
            return []
        
        print(f" Extracting detailed info for {len(product_ids)} products...")
        all_product_details = []
        processed_count = 0
        
        for product_id in product_ids:
            if self.api_calls_made >= self.max_daily_calls:
                print(f" Reached daily API limit. Processed {processed_count}/{len(product_ids)} products")
                break
            
            request = lazop.LazopRequest('/product/item/get', 'GET')
            request.add_api_param('item_id', str(product_id))
            
            data = self._make_api_call(request, f"product-detail-{product_id}")
            
            if data:
                product_detail = data.get('data', {})
                if product_detail:
                    all_product_details.append(product_detail)
            
            processed_count += 1
            
            if processed_count % 25 == 0:
                print(f" Processed {processed_count}/{len(product_ids)} products")
                self._save_to_json(all_product_details, filename)
        
        # Final save
        self._save_to_json(all_product_details, filename)
        print(f"🎉 Product details extraction complete! Total: {len(all_product_details)} items")
        return all_product_details
    
    def extract_product_review_complete(self, start_fresh=False, limit_products=None):
        """
        Complete two-step Lazada review extraction process:
        
        Step 1: Extract review IDs using /review/seller/history/list API for each item_id
        Step 2: Extract detailed review content using /review/seller/list/v2 API
        
        This follows the exact requirement:
        1. Create temporary item_id list from lazada_products_raw.json
        2. Use /review/seller/history/list for each item_id (store in lazada_reviewhistorylist_raw.json)
        3. Retrieve product reviews using id_list (max 10 at a time, store in lazada_productreview_raw.json)
        """
        print("🔄 Starting complete Lazada review extraction process...")
        
        # Step 1: Create temporary item_id list from products
        print("\n📝 Step 1: Creating temporary item_id list from lazada_products_raw.json")
        item_ids = self._create_temporary_item_list(limit_products)
        if not item_ids:
            print("❌ No item_ids found. Cannot proceed.")
            return {}
        
        # Step 2: Extract review IDs for each item_id
        print(f"\n🔍 Step 2: Extracting review IDs for {len(item_ids)} products...")
        review_ids = self._extract_review_ids_by_item(item_ids, start_fresh)
        if not review_ids:
            print("❌ No review IDs found. Cannot proceed to detailed extraction.")
            return {}
        
        # Step 3: Extract detailed review content
        print(f"\n📖 Step 3: Extracting detailed review content for {len(review_ids)} review IDs...")
        detailed_reviews = self._extract_detailed_reviews_by_id_list(review_ids, start_fresh)
        
        print(f"\n🎉 Complete review extraction finished!")
        print(f"   📊 Total review IDs found: {len(review_ids)}")
        print(f"   📝 Detailed reviews extracted: {len(detailed_reviews) if detailed_reviews else 0}")
        
        return detailed_reviews
    
    def _create_temporary_item_list(self, limit_products=None):
        """Create temporary item_id list from lazada_products_raw.json"""
        products_file = os.path.join(self.staging_dir, 'lazada_products_raw.json')
        temp_item_file = os.path.join(self.staging_dir, 'temp_lazada_item_ids.json')
        
        if not os.path.exists(products_file):
            print(f"❌ Products file not found: {products_file}")
            return []
        
        print(f"📂 Loading products from: {products_file}")
        with open(products_file, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
        
        if not products_data:
            print("❌ No product data available")
            return []
        
        # Extract item_ids
        item_ids = []
        for product in products_data:
            if product.get('item_id'):
                item_ids.append({
                    'item_id': str(product['item_id']),
                    'title': product.get('attributes', {}).get('name', 'Unknown Product')[:50]
                })
        
        if limit_products:
            item_ids = item_ids[:limit_products]
        
        # Save temporary item list
        print(f"💾 Saving {len(item_ids)} item_ids to temporary file: {temp_item_file}")
        with open(temp_item_file, 'w', encoding='utf-8') as f:
            json.dump(item_ids, f, indent=2)
        
        return item_ids
    
    def _extract_review_ids_by_item(self, item_list, start_fresh=False):
        """Extract review IDs using /review/seller/history/list for each item_id"""
        history_file = os.path.join(self.staging_dir, 'lazada_reviewhistorylist_raw.json')
        
        if not start_fresh and os.path.exists(history_file):
            print(f"📋 Loading existing review IDs from {history_file}")
            with open(history_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            if existing_data:
                print(f"✅ Found {len(existing_data)} existing review IDs")
                return existing_data
        
        all_review_ids = {}
        
        # Date range: 30 days back maximum (API limitation)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        print(f"📅 Extracting review IDs from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"⚠️ API Limitations: requires item_id, 7-day chunks max, recent dates only")
        
        for i, item_info in enumerate(item_list):
            if self.api_calls_made >= self.max_daily_calls:
                print("⚠️ Daily API limit reached")
                break
            
            item_id = item_info['item_id']
            product_title = item_info['title']
            
            print(f"\n📦 Product {i+1}/{len(item_list)}: {item_id} - {product_title}")
            
            # Process in 7-day chunks
            current_date = start_date
            chunk_count = 0
            
            while current_date < end_date:
                if self.api_calls_made >= self.max_daily_calls:
                    break
                
                chunk_count += 1
                chunk_end = min(current_date + timedelta(days=7), end_date)
                
                start_timestamp = int(current_date.timestamp() * 1000)
                end_timestamp = int(chunk_end.timestamp() * 1000)
                
                print(f"   📅 Chunk {chunk_count}: {current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
                
                # Rate limiting
                if i > 0 or chunk_count > 1:
                    import time
                    print(f"   ⏳ Rate limiting: waiting 3 seconds...")
                    time.sleep(3)
                
                try:
                    # API call: /review/seller/history/list
                    request = lazop.LazopRequest('/review/seller/history/list', 'GET')
                    request.add_api_param('item_id', str(item_id))
                    request.add_api_param('start_time', str(start_timestamp))
                    request.add_api_param('end_time', str(end_timestamp))
                    request.add_api_param('current', '1')
                    request.add_api_param('limit', '100')
                    
                    print(f"   📡 API Call: /review/seller/history/list for item_id={item_id}")
                    
                    response_data = self._make_api_call(request, f"review-history-{item_id}-{chunk_count}")
                    
                    if response_data and response_data.get('data', {}).get('id_list'):
                        chunk_ids = response_data['data']['id_list']
                        print(f"   ✅ Found {len(chunk_ids)} review IDs")
                        
                        # Store IDs with metadata
                        for review_id in chunk_ids:
                            all_review_ids[str(review_id)] = {
                                'id': review_id,
                                'item_id': item_id,
                                'product_title': product_title,
                                'chunk': chunk_count,
                                'period': f"{current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
                            }
                        
                        # Handle pagination if needed
                        total_count = response_data['data'].get('total_count', len(chunk_ids))
                        if total_count > 100:
                            pages_needed = min(3, (total_count // 100) + 1)  # Limit to 3 pages
                            for page in range(2, pages_needed + 1):
                                request = lazop.LazopRequest('/review/seller/history/list', 'GET')
                                request.add_api_param('item_id', str(item_id))
                                request.add_api_param('start_time', str(start_timestamp))
                                request.add_api_param('end_time', str(end_timestamp))
                                request.add_api_param('current', str(page))
                                request.add_api_param('limit', '100')
                                
                                page_data = self._make_api_call(request, f"review-history-{item_id}-{chunk_count}-p{page}")
                                
                                if page_data and page_data.get('data', {}).get('id_list'):
                                    page_ids = page_data['data']['id_list']
                                    print(f"   ✅ Page {page}: Found {len(page_ids)} additional review IDs")
                                    
                                    for review_id in page_ids:
                                        all_review_ids[str(review_id)] = {
                                            'id': review_id,
                                            'item_id': item_id,
                                            'product_title': product_title,
                                            'chunk': chunk_count,
                                            'page': page,
                                            'period': f"{current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
                                        }
                    else:
                        print(f"   ℹ️ No review IDs found for this period")
                        
                except Exception as e:
                    print(f"   ❌ Error: {e}")
                
                current_date = chunk_end
        
        # Save review IDs to lazada_reviewhistorylist_raw.json
        print(f"\n💾 Saving {len(all_review_ids)} review IDs to lazada_reviewhistorylist_raw.json")
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(all_review_ids, f, indent=2)
        
        return all_review_ids
    
    def _extract_detailed_reviews_by_id_list(self, review_ids_dict, start_fresh=False):
        """
        Extract detailed reviews using /review/seller/list/v2 with id_list (max 10 at a time)
        Following exact code pattern: 
        client = lazop.LazopClient(url, appkey, appSecret)
        request = lazop.LazopRequest('/review/seller/list/v2','GET')
        request.add_api_param('id_list', '[111111111111,11111111112]')
        response = client.execute(request, access_token)
        """
        reviews_file = os.path.join(self.staging_dir, 'lazada_productreview_raw.json')
        
        if not start_fresh and os.path.exists(reviews_file):
            print(f"📋 Loading existing detailed reviews from {reviews_file}")
            with open(reviews_file, 'r', encoding='utf-8') as f:
                existing_reviews = json.load(f)
            if existing_reviews:
                print(f"✅ Found {len(existing_reviews)} existing detailed reviews")
                return existing_reviews
        
        all_detailed_reviews = []
        
        # Convert review_ids_dict to list of IDs
        review_ids = [data['id'] for data in review_ids_dict.values()]
        
        print(f"📝 Processing {len(review_ids)} review IDs in batches of 10...")
        
        # Process in batches of 10 (API limitation)
        batch_size = 10
        total_batches = (len(review_ids) + batch_size - 1) // batch_size
        
        for batch_num in range(0, len(review_ids), batch_size):
            if self.api_calls_made >= self.max_daily_calls:
                print("⚠️ Daily API limit reached")
                break
            
            batch_ids = review_ids[batch_num:batch_num + batch_size]
            batch_index = (batch_num // batch_size) + 1
            
            print(f"\n📦 Batch {batch_index}/{total_batches}: Processing {len(batch_ids)} review IDs")
            
            # Rate limiting
            if batch_num > 0:
                import time
                print(f"   ⏳ Rate limiting: waiting 3 seconds...")
                time.sleep(3)
            
            try:
                # Following exact pattern from code sample:
                # request = lazop.LazopRequest('/review/seller/list/v2','GET')
                # request.add_api_param('id_list', '[111111111111,11111111112]')
                request = lazop.LazopRequest('/review/seller/list/v2', 'GET')
                
                # Format id_list as comma-separated string in brackets (as shown in sample)
                id_list_str = '[' + ','.join(str(id) for id in batch_ids) + ']'
                request.add_api_param('id_list', id_list_str)
                
                print(f"   📡 API Call: /review/seller/list/v2 with {len(batch_ids)} IDs")
                print(f"   📋 ID List: {id_list_str}")
                
                # Use client.execute directly (following sample pattern)
                response = self.client.execute(request, self.access_token)
                
                print(f"   🔍 Response type: {response.type}")
                print(f"   🔍 Response body: {response.body}")
                
                # Parse response body
                if response.body:
                    try:
                        # Check if response.body is already a dict or needs JSON parsing
                        if isinstance(response.body, dict):
                            batch_data = response.body
                        else:
                            import json
                            batch_data = json.loads(response.body)
                        
                        print(f"   🔍 Parsed response data keys: {list(batch_data.keys()) if batch_data else 'None'}")
                        
                        # Check for successful response
                        if batch_data.get('code') == '0' and batch_data.get('data'):
                            data = batch_data['data']
                            print(f"   🔍 Data keys available: {list(data.keys()) if data else 'None'}")
                            
                            # Check different possible response structures
                            reviews = []
                            if data.get('reviews'):
                                reviews = data['reviews']
                                print(f"   ✅ Found 'reviews' key with {len(reviews)} reviews")
                            elif data.get('review_list'):
                                reviews = data['review_list']
                                print(f"   ✅ Found 'review_list' key with {len(reviews)} reviews")
                            elif data.get('list'):
                                reviews = data['list']
                                print(f"   ✅ Found 'list' key with {len(reviews)} reviews")
                            elif isinstance(data, list):
                                reviews = data
                                print(f"   ✅ Data is a list with {len(reviews)} reviews")
                            else:
                                print(f"   ⚠️ No recognizable review data structure found")
                                print(f"   📋 Available data: {data}")
                            
                            if reviews:
                                print(f"   ✅ Retrieved {len(reviews)} detailed reviews")
                                
                                # Process each detailed review
                                for review in reviews:
                                    # Get metadata from original review_ids_dict
                                    review_id = str(review.get('id', ''))
                                    metadata = review_ids_dict.get(review_id, {})
                                    
                                    # Extract rating from ratings object (overall_rating)
                                    ratings = review.get('ratings', {})
                                    overall_rating = ratings.get('overall_rating') if ratings else None
                                    
                                    # Convert timestamps to readable dates
                                    create_time = review.get('create_time')
                                    submit_time = review.get('submit_time')
                                    
                                    review_time = None
                                    if create_time:
                                        try:
                                            from datetime import datetime
                                            review_time = datetime.fromtimestamp(create_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
                                        except:
                                            review_time = str(create_time)
                                    
                                    detailed_review = {
                                        'id': review.get('id'),
                                        'item_id': str(review.get('product_id', metadata.get('item_id', ''))),
                                        'product_title': metadata.get('product_title', ''),
                                        'buyer_name': review.get('buyer_name', ''),
                                        'rating': overall_rating,
                                        'review_comment': review.get('review_content', ''),  # Correct field name
                                        'review_time': review_time,
                                        'reply_comment': review.get('reply_comment', ''),
                                        'reply_time': review.get('reply_time'),
                                        'status': review.get('review_type', ''),
                                        'can_reply': review.get('can_reply', False),
                                        'order_id': review.get('order_id'),
                                        'ratings_detail': ratings,
                                        'review_images': review.get('review_images', []),
                                        'review_videos': review.get('review_videos', []),
                                        'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'batch_number': batch_index,
                                        'metadata': metadata
                                    }
                                    all_detailed_reviews.append(detailed_review)
                            else:
                                print(f"   ⚠️ No reviews found in response data")
                        else:
                            print(f"   ❌ API Error: {batch_data.get('message', 'Unknown error')}")
                            print(f"   📋 Full response: {batch_data}")
                    except json.JSONDecodeError as e:
                        print(f"   ❌ Failed to parse JSON response: {e}")
                        print(f"   📋 Raw response: {response.body}")
                else:
                    print(f"   ⚠️ Empty response body")
                    
            except Exception as e:
                print(f"   ❌ Error processing batch {batch_index}: {e}")
                import traceback
                traceback.print_exc()
        
        # Save detailed reviews to lazada_productreview_raw.json
        print(f"\n💾 Saving {len(all_detailed_reviews)} detailed reviews to lazada_productreview_raw.json")
        import json
        with open(reviews_file, 'w', encoding='utf-8') as f:
            json.dump(all_detailed_reviews, f, indent=2, ensure_ascii=False)
        
        return all_detailed_reviews
    
    def extract_review_history_list(self, start_fresh=False, limit_products=None):
        """
        Legacy method - redirects to new complete process
        """
        print("🔄 Redirecting to complete review extraction process...")
        return self.extract_product_review_complete(start_fresh=start_fresh, limit_products=limit_products)
        
        print(f"🔍 Starting historical review ID extraction using /review/seller/history/list...")
        
        # Load product data to get item_ids (required by API)
        products_file = os.path.join(self.staging_dir, 'lazada_products_raw.json')
        if not os.path.exists(products_file):
            print(f"❌ Products file not found: {products_file}")
            print("   Run extract_products() first to get product data")
            return {}
        
        with open(products_file, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
        
        if not products_data:
            print("❌ No product data available")
            return {}
        
        # Get item_ids from products data
        item_ids = [str(prod.get('item_id')) for prod in products_data if prod.get('item_id')]
        if limit_products:
            item_ids = item_ids[:limit_products]
        
        all_review_ids = {}

        # Calculate date ranges - API limitation: 3 months back maximum, 7-day chunks
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)  # 30 days back to start with a smaller range
        
        print(f"📅 Extracting review IDs from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"📦 Processing reviews for {len(item_ids)} products")
        print(f"⚠️ API Limitations: requires item_id, 7-day chunks maximum, 3-month historical limit")
        
        product_count = 0
        
        # Iterate through each product (item_id is required by API)
        for item_id in item_ids:
            if self.api_calls_made >= self.max_daily_calls:
                print("⚠️ Daily API limit reached")
                break
            
            product_count += 1
            print(f"\n📦 Product {product_count}/{len(item_ids)}: item_id={item_id}")
            
            # Process in 7-day chunks (API requirement)
            current_date = start_date
            chunk_count = 0
            
            while current_date < end_date:
                if self.api_calls_made >= self.max_daily_calls:
                    print("⚠️ Daily API limit reached")
                    break

                chunk_count += 1
                
                # Calculate chunk end date (7-day maximum)
                chunk_end = min(current_date + timedelta(days=7), end_date)
                
                # Format dates for API (timestamp format)
                start_timestamp = int(current_date.timestamp() * 1000)  # milliseconds
                end_timestamp = int(chunk_end.timestamp() * 1000)  # milliseconds
                
                print(f"   📅 Chunk {chunk_count}: {current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
                
                # Add rate limiting between calls
                if product_count > 1 or chunk_count > 1:
                    import time
                    print(f"   ⏳ Rate limiting: waiting 15 seconds...")
                    time.sleep(15)
                
                try:
                    # API call to get historical review IDs - with required item_id
                    request = lazop.LazopRequest('/review/seller/history/list', 'GET')
                    request.add_api_param('item_id', str(item_id))  # Required parameter
                    request.add_api_param('start_time', str(start_timestamp))
                    request.add_api_param('end_time', str(end_timestamp))
                    request.add_api_param('current', '1')  # Page number
                    request.add_api_param('limit', '100')  # Max IDs per page
                    
                    print(f"   📡 API Call: /review/seller/history/list for item_id={item_id}")
                    
                    response_data = self._make_api_call(request, f"review-history-{item_id}-{chunk_count}")
                    
                    if response_data and response_data.get('data'):
                        data = response_data['data']
                        
                        # Check for review ID list
                        if data.get('id_list'):
                            chunk_ids = data['id_list']
                            print(f"   ✅ Found {len(chunk_ids)} review IDs")
                            
                            # Store IDs with metadata
                            for review_id in chunk_ids:
                                all_review_ids[str(review_id)] = {
                                    'id': review_id,
                                    'item_id': item_id,
                                    'chunk': chunk_count,
                                    'period': f"{current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
                                }
                            
                            # Handle pagination if needed
                            total_count = data.get('total_count', len(chunk_ids))
                            if total_count > 100:
                                pages_needed = math.ceil(total_count / 100)
                                print(f"   📄 Processing {pages_needed} pages of results...")
                                
                                # Get additional pages (limit to 3 pages max per chunk)
                                for page in range(2, min(pages_needed + 1, 4)):
                                    request = lazop.LazopRequest('/review/seller/history/list', 'GET')
                                    request.add_api_param('item_id', str(item_id))
                                    request.add_api_param('start_time', str(start_timestamp))
                                    request.add_api_param('end_time', str(end_timestamp))
                                    request.add_api_param('current', str(page))
                                    request.add_api_param('limit', '100')
                                    
                                    page_data = self._make_api_call(request, f"review-history-{item_id}-{chunk_count}-p{page}")
                                    
                                    if page_data and page_data.get('data', {}).get('id_list'):
                                        page_ids = page_data['data']['id_list']
                                        print(f"   ✅ Page {page}: Found {len(page_ids)} additional review IDs")
                                        
                                        for review_id in page_ids:
                                            all_review_ids[str(review_id)] = {
                                                'id': review_id,
                                                'item_id': item_id,
                                                'chunk': chunk_count,
                                                'page': page,
                                                'period': f"{current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
                                            }
                                    else:
                                        print(f"   ⚠️ Page {page}: No data returned")
                                        break
                        else:
                            print(f"   ⚠️ No review IDs found for this time period")
                    else:
                        print(f"   ⚠️ No data returned for this time period")
                        
                except Exception as e:
                    print(f"   ❌ Error processing chunk {chunk_count}: {e}")
                
                # Move to next chunk
                current_date = chunk_end
        
        # Save all collected review IDs
                            
            
        # Save all collected review IDs
        filename = 'lazada_reviewhistorylist_raw.json'
        
        if start_fresh or not os.path.exists(os.path.join(self.staging_dir, filename)):
            print(f"\n💾 Saving {len(all_review_ids)} review IDs to {filename}")
            self._save_to_json(all_review_ids, filename)
        else:
            # Append mode - merge with existing data
            existing_ids = self._load_from_json(filename) or {}
            existing_ids.update(all_review_ids)
            
            print(f"\n💾 Appending {len(all_review_ids)} new review IDs (total: {len(existing_ids)}) to {filename}")
            self._save_to_json(existing_ids, filename)
            all_review_ids = existing_ids
        
        print(f"\n� Review History Extraction Summary:")
        print(f"   📊 Total review IDs collected: {len(all_review_ids)}")
        print(f"   📦 Products processed: {product_count}/{len(item_ids)}")
        print(f"   📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"   🔥 API calls made: {self.api_calls_made}")
        print(f"   ⚠️ Note: These are IDs only. Use extract_review_details() to get content.")
        
        return all_review_ids
    
    def extract_review_details(self, review_ids=None, start_fresh=False):
        """
        Step 2: Extract detailed review content using GetReviewListByIdList API
        Takes the review IDs from extract_review_history_list() and fetches full review content
        Uses batched approach for efficiency
        Saves detailed reviews to lazada_productreview_raw.json
        
        Args:
            review_ids (list): List of review entries with review_id fields
            start_fresh (bool): Whether to start fresh or append to existing data
        
        Returns:
            list: List of detailed review data with full content
        """
        filename = 'lazada_productreview_raw.json'
        
        if not start_fresh:
            existing_reviews = self._load_from_json(filename)
            if existing_reviews:
                print(f"📋 Found {len(existing_reviews)} existing reviews. Use start_fresh=True to overwrite.")
                return existing_reviews
        
        # Load review IDs if not provided
        if review_ids is None:
            review_ids = self._load_from_json('lazada_reviewhistorylist_raw.json')
        
        if not review_ids:
            print("❌ No review IDs found. Please run extract_review_history_list() first.")
            return []
        
        print(f"🔍 Starting detailed review content extraction for {len(review_ids)} review IDs...")
        
        # Extract just the review IDs from the historical data
        id_list = []
        if isinstance(review_ids, dict):
            # New format: dictionary with review_id as key and metadata as value
            for review_id, metadata in review_ids.items():
                if isinstance(metadata, dict) and 'id' in metadata:
                    id_list.append(metadata['id'])
                else:
                    id_list.append(review_id)  # Use key as review_id
        elif isinstance(review_ids, list):
            # Legacy format: list of review entries
            for item in review_ids:
                if isinstance(item, dict):
                    # Try different possible key names
                    if 'id' in item:
                        id_list.append(item['id'])
                    elif 'review_id' in item:
                        id_list.append(item['review_id'])
                elif isinstance(item, (str, int)):
                    id_list.append(item)
        
        if not id_list:
            print("❌ No valid review IDs found in the data.")
            return []
        
        print(f"📋 Found {len(id_list)} review IDs to fetch detailed content for...")
        
        all_reviews = []
        batch_size = 20  # Reasonable batch size for review content API
        total_batches = math.ceil(len(id_list) / batch_size)
        
        print(f"📦 Processing {len(id_list)} review IDs in {total_batches} batches of {batch_size}...")
        
        for i in range(0, len(id_list), batch_size):
            if self.api_calls_made >= self.max_daily_calls:
                print("⚠️ Daily API limit reached during review details extraction")
                break
            
            batch_ids = id_list[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            print(f"\n📦 Batch {batch_num}/{total_batches}: Processing {len(batch_ids)} review details...")
            
            # Add rate limiting between batches
            if batch_num > 1:
                import time
                print(f"   ⏳ Rate limiting: waiting 5 seconds...")
                time.sleep(5)
            
            try:
                # API call to get detailed review content using GetReviewListByIdList
                request = lazop.LazopRequest('/review/seller/list/v2', 'GET')
                id_list_str = ','.join(str(id) for id in batch_ids)
                request.add_api_param('id_list', id_list_str)
                
                print(f"   📡 API Call: GetReviewListByIdList with {len(batch_ids)} IDs")
                
                batch_data = self._make_api_call(request, f"review-details-batch-{batch_num}")
                
                if batch_data and batch_data.get('data', {}).get('reviews'):
                    reviews = batch_data['data']['reviews']
                    print(f"   ✅ Retrieved {len(reviews)} detailed reviews")
                    
                    # Process each detailed review
                    for review in reviews:
                        detailed_review = {
                            # Basic review info
                            'review_id': review.get('review_id'),
                            'product_id': review.get('product_id'),
                            'item_id': review.get('item_id'),
                            'buyer_id': review.get('buyer_id'),
                            
                            # Review content
                            'review_content': review.get('review_content', ''),
                            'review_title': review.get('review_title', ''),
                            'rating': review.get('rating', 0),
                            
                            # Dates
                            'created_at': review.get('created_at', ''),
                            'updated_at': review.get('updated_at', ''),
                            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            
                            # Seller response (if any)
                            'seller_response': review.get('seller_response', ''),
                            'seller_response_date': review.get('seller_response_date', ''),
                            
                            # Additional metadata
                            'review_status': review.get('status', ''),
                            'buyer_name': review.get('buyer_name', ''),
                            'product_name': review.get('product_name', ''),
                            
                            # Platform identifier
                            'platform': 'Lazada',
                            'data_source': 'review_seller_list_v2_api',
                            'extraction_method': 'historical_id_list'
                        }
                        all_reviews.append(detailed_review)
                    
                    # Save progress every 10 batches
                    if batch_num % 10 == 0:
                        self._save_to_json(all_reviews, filename)
                        print(f"   💾 Progress saved: {len(all_reviews)} reviews")
                        
                else:
                    print(f"   ⚠️ No review details found for batch {batch_num}")
                    
            except Exception as e:
                print(f"   ❌ Error processing batch {batch_num}: {e}")
                continue
        
        # Final save
        self._save_to_json(all_reviews, filename)
        
        # Summary
        print(f"\n🎉 Detailed review content extraction complete!")
        print(f"   Review IDs processed: {len(id_list)}")
        print(f"   Detailed reviews extracted: {len(all_reviews)}")
        print(f"   Success rate: {(len(all_reviews)/len(id_list)*100):.1f}%")
        print(f"   Saved to: {filename}")
        
        return all_reviews
    
    def extract_product_reviews(self, start_fresh=False, limit_products=None):
        """
        Complete product review extraction using product-based approach:
        1. Extract reviews for each product using item_ids from lazada_products_raw.json
        2. Process and save detailed review information
        
        Args:
            start_fresh (bool): Whether to start fresh or append to existing data
            limit_products (int): Limit number of products to process (for testing)
        
        Returns:
            list: List of detailed review data
        """
        print(f"🔍 Starting complete product review extraction (product-based approach)...")
        
        # Step 1: Extract reviews by product
        print(f"\n📋 Step 1: Extracting reviews for each product...")
        review_entries = self.extract_review_history_list(start_fresh=start_fresh, limit_products=limit_products)
        
        if not review_entries:
            print("❌ No review data found. Cannot proceed with processing.")
            return []
        
        # Step 2: Process and format detailed reviews
        print(f"\n📋 Step 2: Processing detailed review information...")
        reviews = self.extract_review_details(review_ids=review_entries, start_fresh=start_fresh)
        
        print(f"\n🎉 Complete review extraction finished!")
        print(f"   Review entries collected: {len(review_entries)}")
        print(f"   Final reviews processed: {len(reviews)}")
        
        return reviews
    
    def run_incremental_extraction(self, end_date='2025-10-31'):
        """
        Run intelligent incremental data extraction that:
        1. Detects last extraction dates from existing data
        2. Starts from beginning of month containing last date
        3. Continues until end_date (default: Oct 31, 2025)
        4. Handles duplicates using unique IDs
        5. Updates orders with changed statuses
        
        Args:
            end_date (str): End date for extraction (YYYY-MM-DD)
        """
        print("🚀 Starting INCREMENTAL Lazada data extraction...")
        print("=" * 70)
        
        # Check existing data and find last dates
        print("\n🔍 Step 1: Analyzing existing data...")
        last_dates = self._find_last_extraction_date()
        
        print(f"📊 Last extraction dates found:")
        for data_type, date_val in last_dates.items():
            if date_val:
                print(f"  • {data_type}: {date_val.strftime('%Y-%m-%d')}")
            else:
                print(f"  • {data_type}: No data found")
        
        extraction_results = {}
        
        # Extract Orders (with incremental logic)
        print("\n📋 Step 2: Extracting Orders...")
        print("-" * 40)
        try:
            orders = self.extract_all_orders(end_date=end_date, incremental=True)
            extraction_results['orders'] = orders
            print(f"✅ Orders: {len(orders)} total records")
        except Exception as e:
            print(f"❌ Orders extraction failed: {e}")
            extraction_results['orders'] = []
        
        # Extract Order Items (incremental based on orders)
        if extraction_results['orders']:
            print("\n📦 Step 3: Extracting Order Items...")
            print("-" * 40)
            try:
                order_items = self.extract_all_order_items(
                    orders_data=extraction_results['orders'], 
                    incremental=True
                )
                extraction_results['order_items'] = order_items
                print(f"✅ Order Items: {len(order_items)} total records")
            except Exception as e:
                print(f"❌ Order Items extraction failed: {e}")
                extraction_results['order_items'] = []
        
        # Extract Traffic Metrics (incremental)
        print("\n📈 Step 4: Extracting Traffic Metrics...")
        print("-" * 40)
        try:
            traffic = self.extract_traffic_metrics(
                end_date=end_date, 
                incremental=True, 
                monthly_aggregate=True
            )
            extraction_results['traffic'] = traffic
            print(f"✅ Traffic: {len(traffic)} total records")
        except Exception as e:
            print(f"❌ Traffic extraction failed: {e}")
            extraction_results['traffic'] = []
        
        # Extract Products (if needed - this is usually stable)
        print("\n🛍️ Step 5: Checking Products...")
        print("-" * 40)
        existing_products = self._load_from_json('lazada_products_raw.json')
        if not existing_products or len(existing_products) < 100:
            print("No products found or very few. Extracting fresh product data...")
            try:
                products = self.extract_all_products(start_fresh=False)
                extraction_results['products'] = products
                print(f"✅ Products: {len(products)} total records")
            except Exception as e:
                print(f"❌ Products extraction failed: {e}")
                extraction_results['products'] = existing_products
        else:
            print(f"✅ Products: {len(existing_products)} existing records (skipping)")
            extraction_results['products'] = existing_products
        
        # Extract Product Details (if we have products)
        if extraction_results.get('products'):
            print("\n🔍 Step 6: Extracting Product Details...")
            print("-" * 40)
            try:
                product_details = self.extract_product_details(start_fresh=False)
                extraction_results['product_details'] = product_details
                print(f"✅ Product Details: {len(product_details)} total records")
            except Exception as e:
                print(f"❌ Product Details extraction failed: {e}")
                extraction_results['product_details'] = []
        
        # Extract Product Reviews (incremental approach)
        print("\n⭐ Step 7: Extracting Product Reviews...")
        print("-" * 40)
        try:
            reviews = self.extract_product_reviews(start_fresh=False, limit_products=None)
            extraction_results['reviews'] = reviews
            print(f"✅ Reviews: {len(reviews)} total records")
        except Exception as e:
            print(f"❌ Reviews extraction failed: {e}")
            extraction_results['reviews'] = []
        
        # Summary
        print("\n" + "=" * 70)
        print("📊 INCREMENTAL EXTRACTION SUMMARY")
        print("=" * 70)
        
        total_api_calls = self.api_calls_made
        for data_type, data in extraction_results.items():
            count = len(data) if isinstance(data, list) else 0
            print(f"✅ {data_type.replace('_', ' ').title()}: {count:,} records")
        
        print(f"\n📡 Total API calls used: {total_api_calls:,}/{self.max_daily_calls:,}")
        print(f"📁 All data saved to: {self.staging_dir}")
        
        # Data quality checks
        print(f"\n🔍 Data Quality Summary:")
        if extraction_results.get('orders'):
            unique_order_ids = len(set(str(o.get('order_id', '')) for o in extraction_results['orders'] if o.get('order_id')))
            print(f"  • Unique Order IDs: {unique_order_ids:,}")
        
        if extraction_results.get('order_items'):
            order_items_with_order_id = sum(1 for item in extraction_results['order_items'] if item.get('order_id'))
            print(f"  • Order Items with Order ID: {order_items_with_order_id:,}")
        
        # Check for recent data
        if extraction_results.get('orders'):
            recent_orders = [o for o in extraction_results['orders'] 
                           if o.get('created_at') and o['created_at'] >= (datetime.now() - timedelta(days=30)).isoformat()]
            print(f"  • Orders from last 30 days: {len(recent_orders):,}")
        
        print(f"\n🎉 Incremental extraction completed successfully!")
        print(f"💡 Tip: Run this same command again to get only new data since this extraction")
        
        return extraction_results

    def run_complete_extraction(self, start_fresh=False, end_date='2025-10-31'):
        """
        Run complete data extraction in optimal order
        Updated to extract until Oct 31, 2025
        """
        print("🚀 Starting COMPLETE Lazada data extraction...")
        print("=" * 60)
        
        extraction_plan = [
            ("Products", lambda: self.extract_all_products(start_fresh=start_fresh)),
            ("Orders", lambda: self.extract_all_orders(start_fresh=start_fresh, end_date=end_date)),
            ("Order Items", lambda: self.extract_all_order_items(start_fresh=start_fresh)),
            ("Traffic Metrics", lambda: self.extract_traffic_metrics(start_fresh=start_fresh, end_date=end_date)),
            ("Product Details", lambda: self.extract_product_details(start_fresh=start_fresh)),
            ("Product Reviews", lambda: self.extract_product_reviews(start_fresh=start_fresh))
        ]
        
        results = {}
        
        for step_name, extraction_func in extraction_plan:
            print(f"\n🔄 Step: {step_name}")
            print("-" * 40)
            
            if self.api_calls_made >= self.max_daily_calls:
                print(f"⚠️ Daily API limit reached. Stopping at {step_name}")
                break
            
            try:
                results[step_name] = extraction_func()
                print(f"✅ {step_name} completed")
                print(f"📡 API calls used: {self.api_calls_made}/{self.max_daily_calls}")
                
            except Exception as e:
                print(f"❌ Error in {step_name}: {e}")
                continue
        
        print("\n" + "=" * 60)
        print("📊 EXTRACTION SUMMARY")
        print("=" * 60)
        
        for step_name, data in results.items():
            count = len(data) if isinstance(data, list) else 0
            print(f"✅ {step_name}: {count:,} records")
        
        print(f"\n📡 Total API calls used: {self.api_calls_made}/{self.max_daily_calls}")
        print(f"📁 All data saved to: {self.staging_dir}")
        
        return results

# Convenience functions
def run_incremental_extraction(end_date='2025-10-31'):
    """
    Run incremental extraction that intelligently updates from last extraction date
    Handles duplicates and continues until Oct 31, 2025
    """
    extractor = LazadaDataExtractor()
    return extractor.run_incremental_extraction(end_date=end_date)

def run_full_extraction(start_fresh=False, end_date='2025-10-31'):
    """Run complete extraction with all data until Oct 31, 2025"""
    extractor = LazadaDataExtractor()
    return extractor.run_complete_extraction(start_fresh=start_fresh, end_date=end_date)

def extract_recent_data(days_back=30):
    """Extract only recent data (last N days) to save API calls"""
    extractor = LazadaDataExtractor()
    
    # Extract recent orders
    recent_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT%H:%M:%S+08:00')
    recent_end = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00')
    
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
    extractor = LazadaDataExtractor()
    
    print("📊 Lazada Extraction Status Report")
    print("=" * 50)
    
    # Check each data type
    data_files = {
        'Orders': 'lazada_orders_raw.json',
        'Order Items': 'lazada_multiple_order_items_raw.json', 
        'Products': 'lazada_products_raw.json',
        'Product Details': 'lazada_productitem_raw.json',
        'Traffic': 'lazada_reportoverview_raw.json',
        'Reviews': 'lazada_productreview_raw.json'
    }
    
    total_records = 0
    status_report = {}
    
    for data_type, filename in data_files.items():
        data = extractor._load_from_json(filename)
        count = len(data) if data else 0
        total_records += count
        
        # Get date range for this data type
        if data and count > 0:
            if data_type in ['Orders', 'Reviews']:
                last_date = extractor._get_last_date_from_data(data, 'created_at')
                first_date = min([
                    extractor._get_last_date_from_data([record], 'created_at') 
                    for record in data 
                    if extractor._get_last_date_from_data([record], 'created_at')
                ], default=None)
            elif data_type == 'Traffic':
                dates = []
                for record in data:
                    if 'time_key' in record:
                        try:
                            time_key_str = str(record['time_key'])
                            if len(time_key_str) >= 8:
                                dates.append(datetime.strptime(time_key_str[:8], '%Y%m%d'))
                        except ValueError:
                            continue
                last_date = max(dates) if dates else None
                first_date = min(dates) if dates else None
            else:
                last_date = None
                first_date = None
            
            status_report[data_type] = {
                'count': count,
                'first_date': first_date,
                'last_date': last_date
            }
        else:
            status_report[data_type] = {
                'count': 0,
                'first_date': None,
                'last_date': None
            }
        
        print(f"📂 {data_type}: {count:,} records")
        if status_report[data_type]['first_date'] and status_report[data_type]['last_date']:
            print(f"   📅 Date range: {status_report[data_type]['first_date'].strftime('%Y-%m-%d')} to {status_report[data_type]['last_date'].strftime('%Y-%m-%d')}")
        elif count == 0:
            print(f"   ⚠️ No data found")
        else:
            print(f"   📅 Date range: Could not determine")
    
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
        print(f"   🔄 Run incremental extraction: run_incremental_extraction()")
        print(f"   📅 No recent data found (last 30 days)")
    else:
        print(f"   ✅ Recent data found - incremental extraction recommended")
        print(f"   🔄 Run: run_incremental_extraction() to get latest updates")
    
    # Check for missing data types
    missing_data = [data_type for data_type, info in status_report.items() if info['count'] == 0]
    if missing_data:
        print(f"   ⚠️ Missing data types: {', '.join(missing_data)}")
        print(f"   🔄 Run complete extraction: run_full_extraction(start_fresh=True)")
    
    return status_report

def extract_product_reviews_only(start_fresh=False, limit_products=None):
    """Extract only product reviews (standalone function)"""
    extractor = LazadaDataExtractor()
    return extractor.extract_product_reviews(start_fresh=start_fresh, limit_products=limit_products)

def extract_product_review_complete_only(start_fresh=False, limit_products=None):
    """
    Convenience function to run complete Lazada review extraction
    
    This creates the complete two-step process:
    1. Extract review IDs for each item_id -> lazada_reviewhistorylist_raw.json
    2. Extract detailed reviews by id_list -> lazada_productreview_raw.json
    """
    extractor = LazadaDataExtractor()
    return extractor.extract_product_review_complete(start_fresh=start_fresh, limit_products=limit_products)

def extract_review_history_only(start_fresh=False, limit_products=None):
    """Extract only review history by product (Step 1)"""
    extractor = LazadaDataExtractor()
    return extractor.extract_review_history_list(start_fresh=start_fresh, limit_products=limit_products)

def extract_review_details_only(review_ids=None, start_fresh=False):
    """Extract only detailed reviews (Step 2)"""
    extractor = LazadaDataExtractor()
    return extractor.extract_review_details(review_ids=review_ids, start_fresh=start_fresh)

if __name__ == "__main__":
    print("🚀 Lazada Data Extraction with Incremental Updates")
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
    print("5. ⭐ Product reviews - complete process")
    print("   - Extract product reviews using 2-step process")
    print("")
    print("6. 📋 Product reviews - Step 1 only (IDs)")
    print("   - Extract review IDs by product")
    print("")
    print("7. 📃 Product reviews - Step 2 only (details)")
    print("   - Process detailed review information")
    
    choice = input("\nEnter choice (1-7): ").strip()
    
    if choice == "1":
        print("🔄 Running incremental extraction...")
        print("This will automatically detect your last extraction date and continue from there.")
        results = run_incremental_extraction()
        print(f"\n✅ Incremental extraction completed!")
        
    elif choice == "2":
        print("📊 Checking extraction status...")
        status = check_extraction_status()
        
    elif choice == "3":
        confirm = input("⚠️ This will re-extract ALL data and use many API calls. Continue? (y/N): ")
        if confirm.lower() in ['y', 'yes']:
            print("🆕 Running complete fresh extraction...")
            results = run_full_extraction(start_fresh=True)
        else:
            print("❌ Cancelled complete extraction")
            
    elif choice == "4":
        days = input("Enter number of days back (default 30): ").strip()
        try:
            days_back = int(days) if days else 30
        except ValueError:
            days_back = 30
        
        print(f"📈 Running recent data extraction (last {days_back} days)...")
        results = extract_recent_data(days_back=days_back)
        print(f"📝 Extracted recent data:")
        for data_type, data in results.items():
            print(f"  • {data_type}: {len(data)} records")
            
    elif choice == "5":
        limit = input("Limit to N products for testing (press Enter for all): ").strip()
        try:
            limit_products = int(limit) if limit else None
        except ValueError:
            limit_products = None
            
        print("⭐ Running complete product reviews extraction...")
        results = extract_product_reviews_only(start_fresh=False, limit_products=limit_products)
        print(f"📝 Extracted {len(results)} reviews")
        
    elif choice == "6":
        limit = input("Limit to N products for testing (press Enter for all): ").strip()
        try:
            limit_products = int(limit) if limit else None
        except ValueError:
            limit_products = None
            
        print("📋 Running review history extraction (Step 1)...")
        results = extract_review_history_only(start_fresh=False, limit_products=limit_products)
        print(f"📝 Extracted {len(results)} review entries")
        
    elif choice == "7":
        print("📃 Running review details extraction (Step 2)...")
        results = extract_review_details_only(start_fresh=False)
        print(f"📝 Extracted {len(results)} detailed reviews")
        
    else:
        print("❌ Invalid choice. Running incremental extraction by default...")
        results = run_incremental_extraction()
    
    print(f"\n🎉 Extraction completed!")
    print(f"📁 Check the app/Staging/ directory for JSON files")
    print(f"💡 Next time, just run option 1 (incremental) to get only new data!")

