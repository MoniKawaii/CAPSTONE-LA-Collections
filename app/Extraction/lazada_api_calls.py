try:
    import lazop
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
        self.client = lazop.LazopClient(self.url, self.app_key, self.app_secret)
        
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
    
    def _make_api_call(self, request, call_type="general"):
        """Make API call with rate limiting and tracking"""
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
                return None
            
            # Small delay to be respectful to API
            time.sleep(0.1)
            
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
    
    def extract_all_orders(self, start_date=None, end_date=None, start_fresh=False):
        """
        Extract ALL orders from Lazada with 90-day chunks (API limitation)
        Automatically chunks large date ranges into 90-day batches
        Saves to lazada_orders_raw.json
        """
        filename = 'lazada_orders_raw.json'
        
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
        
        # Calculate total days and number of 90-day chunks needed
        total_days = (end_date - start_date).days
        chunk_days = 90  # API maximum
        total_chunks = (total_days // chunk_days) + (1 if total_days % chunk_days > 0 else 0)
        
        print(f"📊 Total period: {total_days} days")
        print(f"📦 Breaking into {total_chunks} chunks of {chunk_days} days each (API limit)")
        
        all_orders = []
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
            all_orders.extend(chunk_orders)
            
            print(f"✅ Chunk {chunk_num}: Got {len(chunk_orders)} orders (Total: {len(all_orders)})")
            
            # Check API limit
            if self.api_calls_made >= self.max_daily_calls:
                print(f"Daily API limit reached! Stopping at chunk {chunk_num}")
                break
            
            # Move to next chunk
            current_start = chunk_end + timedelta(days=1)
        
        # Final save
        self._save_to_json(all_orders, filename)
        print(f"Order extraction complete! Total: {len(all_orders)} orders across {chunk_num} chunks")
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
    
    def extract_all_order_items(self, orders_data=None, start_fresh=False):
        """
        Extract order items for all orders using /orders/items/get API
        Processes up to 50 order IDs per API call (API limitation)
        Saves to lazada_multiple_order_items_raw.json
        """
        filename = 'lazada_multiple_order_items_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"Found {len(existing_data)} existing order items. Use start_fresh=True to re-extract.")
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
    
    def extract_traffic_metrics(self, start_date=None, end_date=None, start_fresh=False, monthly_aggregate=True):
        """
        Extract traffic/advertising metrics - Monthly Aggregates
        Saves to lazada_reportoverview_raw.json
        
        Args:
            start_date: Start date (YYYY-MM-DD or datetime object)
            end_date: End date (YYYY-MM-DD or datetime object)
            start_fresh: Whether to re-extract all data
            monthly_aggregate: Whether to extract monthly data (True) or single period (False)
        """
        filename = 'lazada_reportoverview_raw.json'
        
        if not start_fresh:
            existing_data = self._load_from_json(filename)
            if existing_data:
                print(f"Found existing traffic data. Use start_fresh=True to re-extract.")
                return existing_data
        
        # Default date range: 2022-10-01 to 2025-04-30 for comprehensive historical data
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
            print(f"Extracting monthly traffic metrics from {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}...")
            return self._extract_monthly_traffic(start_dt, end_dt, filename)
        else:
            print(f"Extracting single period traffic metrics from {start_date} to {end_date}...")
            return self._extract_single_period_traffic(start_date, end_date, filename)
    
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
        print(f" Product details extraction complete! Total: {len(all_product_details)} items")
        return all_product_details
    
    def run_complete_extraction(self, start_fresh=False):
        """
        Run complete data extraction in optimal order
        """
        print(" Starting COMPLETE Lazada data extraction...")
        print("=" * 60)
        
        extraction_plan = [
            ("Products", self.extract_all_products),
            ("Orders", self.extract_all_orders),
            ("Order Items", self.extract_all_order_items),
            ("Traffic Metrics", self.extract_traffic_metrics),
            ("Product Details", self.extract_product_details)
        ]
        
        results = {}
        
        for step_name, extraction_func in extraction_plan:
            print(f"\n Step: {step_name}")
            print("-" * 40)
            
            if self.api_calls_made >= self.max_daily_calls:
                print(f" Daily API limit reached. Stopping at {step_name}")
                break
            
            try:
                if step_name == "Product Details":
                    # Only extract details for first 50 products to save API calls
                    results[step_name] = extraction_func(start_fresh=start_fresh)
                else:
                    results[step_name] = extraction_func(start_fresh=start_fresh)
                    
                print(f" {step_name} completed")
                print(f" API calls used: {self.api_calls_made}/{self.max_daily_calls}")
                
            except Exception as e:
                print(f" Error in {step_name}: {e}")
                continue
        
        print("\n" + "=" * 60)
        print(" EXTRACTION SUMMARY")
        print("=" * 60)
        
        for step_name, data in results.items():
            count = len(data) if isinstance(data, list) else 0
            print(f" {step_name}: {count} records")
        
        print(f" Total API calls used: {self.api_calls_made}/{self.max_daily_calls}")
        print(f" All data saved to: {self.staging_dir}")
        
        return results

# Convenience functions
def run_full_extraction(start_fresh=False):
    """Run complete extraction with all data"""
    extractor = LazadaDataExtractor()
    return extractor.run_complete_extraction(start_fresh=start_fresh)

def extract_recent_data():
    """Extract only recent data (last 30 days) to save API calls"""
    extractor = LazadaDataExtractor()
    
    # Extract recent orders
    recent_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S+08:00')
    recent_end = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+08:00')
    
    orders = extractor.extract_all_orders(start_date=recent_start, end_date=recent_end)
    order_items = extractor.extract_all_order_items(orders_data=orders)
    traffic = extractor.extract_traffic_metrics()
    
    return {
        'orders': orders,
        'order_items': order_items,
        'traffic': traffic
    }

if __name__ == "__main__":
    print(" Lazada Complete Data Extraction")
    print("Choose extraction mode:")
    print("1. Complete historical extraction (uses more API calls)")
    print("2. Recent data only (last 30 days)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        print(" Running complete extraction...")
        results = run_full_extraction(start_fresh=False)
    elif choice == "2":
        print(" Running recent data extraction...")
        results = extract_recent_data()
    else:
        print(" Invalid choice. Running recent data extraction by default...")
        results = extract_recent_data()
    
    print("\n Extraction completed!")
    print(" Check the app/Staging/ directory for JSON files")

