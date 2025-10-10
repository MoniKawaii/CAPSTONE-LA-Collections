"""
Lazada Dimensional ETL Script
Fetches data from Lazada APIs to populate the Enhanced Dimensional Schema

This script fetches:
- Orders (/orders/get)
- Order Items (/orders/items/get)  
- Products (/products/get)
- Vouchers (/promotion/vouchers/get)
- Voucher Products (/promotion/voucher/products/get)

And transforms them into DataFrames matching the dimensional schema.
"""

import pandas as pd
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from app.lazada.lazada_api_functions import (
    get_orders,
    get_order_items,
    get_products,
    get_seller_vouchers,
    get_voucher_products
)
from app.lazada.get_lazada_tokens import get_valid_token
from app.lazada.lazada_dimensional_transformer import LazadaDimensionalTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LazadaDimensionalETL:
    """
    Lazada ETL processor for dimensional data warehouse
    """
    
    def __init__(self):
        self.access_token = None
        self.batch_size = 50  # Lazada API limit for products is 50
        self.data_cache = {
            'orders': [],
            'order_items': [],
            'products': [],
            'vouchers': [],
            'voucher_products': []
        }
        self.transformer = LazadaDimensionalTransformer()
        
    def get_access_token(self) -> str:
        """Get valid access token"""
        logger.info("🔑 Getting access token...")
        token = get_valid_token()
        if not token:
            raise Exception("Failed to get valid access token")
        self.access_token = token
        logger.info("✅ Access token obtained")
        return token
    
    def create_time_dimension_key(self, date_str: str) -> int:
        """Create time dimension key in YYYYMMDD format"""
        try:
            if pd.isna(date_str) or not date_str:
                return None
            dt = pd.to_datetime(date_str)
            return int(dt.strftime('%Y%m%d'))
        except:
            return None
    
    def fetch_orders_batch(self, created_after: str, created_before: str, 
                          offset: int = 0, limit: int = 100) -> Dict:
        """Fetch orders in batches"""
        logger.info(f"📦 Fetching orders batch: offset={offset}, limit={limit}")
        logger.info(f"   Date range: {created_after} to {created_before}")
        
        result = get_orders(
            access_token=self.access_token,
            created_after=created_after,
            created_before=created_before,
            offset=offset,
            limit=limit
        )
        
        if result['success']:
            orders = result.get('data', {}).get('orders', [])
            logger.info(f"✅ Retrieved {len(orders)} orders")
            return {'success': True, 'orders': orders, 'total_count': result.get('data', {}).get('count_total', 0)}
        else:
            logger.error(f"❌ Failed to fetch orders: {result.get('error', 'Unknown error')}")
            return {'success': False, 'orders': [], 'error': result.get('error')}
    
    def fetch_all_orders(self, days_back: int = 30) -> List[Dict]:
        """Fetch all orders using pagination"""
        logger.info(f"📅 Fetching orders from last {days_back} days...")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        created_after = start_date.strftime('%Y-%m-%dT%H:%M:%S%z') or start_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
        created_before = end_date.strftime('%Y-%m-%dT%H:%M:%S%z') or end_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
        
        all_orders = []
        offset = 0
        
        while True:
            # Add delay to respect rate limits
            time.sleep(1)
            
            batch_result = self.fetch_orders_batch(
                created_after=created_after,
                created_before=created_before,
                offset=offset,
                limit=self.batch_size
            )
            
            if not batch_result['success']:
                logger.error("Failed to fetch orders batch")
                break
                
            orders = batch_result['orders']
            if not orders:
                logger.info("No more orders to fetch")
                break
                
            all_orders.extend(orders)
            offset += len(orders)
            
            logger.info(f"📊 Total orders fetched so far: {len(all_orders)}")
            
            # Check if we've reached the end
            if len(orders) < self.batch_size:
                break
        
        logger.info(f"🎉 Completed orders fetch: {len(all_orders)} total orders")
        self.data_cache['orders'] = all_orders
        return all_orders
    
    def fetch_order_items_for_orders(self, order_ids: List[int]) -> List[Dict]:
        """Fetch order items for specific orders"""
        logger.info(f"📋 Fetching order items for {len(order_ids)} orders...")
        
        all_order_items = []
        
        for i, order_id in enumerate(order_ids):
            time.sleep(0.5)  # Rate limiting
            
            logger.info(f"📄 Fetching items for order {order_id} ({i+1}/{len(order_ids)})")
            
            result = get_order_items(
                access_token=self.access_token,
                order_id=order_id
            )
            
            if result['success']:
                items = result.get('data', [])
                logger.info(f"   ✅ Retrieved {len(items)} items")
                all_order_items.extend(items)
            else:
                logger.error(f"   ❌ Failed to fetch items for order {order_id}: {result.get('error')}")
        
        logger.info(f"🎉 Completed order items fetch: {len(all_order_items)} total items")
        self.data_cache['order_items'] = all_order_items
        return all_order_items
    
    def fetch_all_products(self) -> List[Dict]:
        """Fetch all products using pagination"""
        logger.info("🛍️ Fetching all products...")
        
        all_products = []
        offset = 0
        
        while True:
            time.sleep(1)  # Rate limiting
            
            logger.info(f"📦 Fetching products batch: offset={offset}, limit={self.batch_size}")
            
            result = get_products(
                access_token=self.access_token,
                offset=offset,
                limit=self.batch_size
            )
            
            if not result['success']:
                logger.error(f"❌ Failed to fetch products: {result.get('error')}")
                break
            
            products = result.get('data', {}).get('products', [])
            if not products:
                logger.info("No more products to fetch")
                break
            
            all_products.extend(products)
            offset += len(products)
            
            logger.info(f"📊 Total products fetched so far: {len(all_products)}")
            
            if len(products) < self.batch_size:
                break
        
        logger.info(f"🎉 Completed products fetch: {len(all_products)} total products")
        self.data_cache['products'] = all_products
        return all_products
    
    def fetch_all_vouchers(self) -> List[Dict]:
        """Fetch all seller vouchers"""
        logger.info("🎫 Fetching all vouchers...")
        
        result = get_seller_vouchers(access_token=self.access_token)
        
        if result['success']:
            vouchers = result.get('data', [])
            logger.info(f"✅ Retrieved {len(vouchers)} vouchers")
            self.data_cache['vouchers'] = vouchers
            return vouchers
        else:
            logger.error(f"❌ Failed to fetch vouchers: {result.get('error')}")
            return []
    
    def fetch_voucher_products_for_vouchers(self, voucher_ids: List[str]) -> List[Dict]:
        """Fetch products for each voucher"""
        logger.info(f"🛍️ Fetching voucher products for {len(voucher_ids)} vouchers...")
        
        all_voucher_products = []
        
        for voucher_id in voucher_ids:
            if not voucher_id:
                continue
                
            time.sleep(0.5)  # Rate limiting
            
            result = get_voucher_products(access_token=self.access_token, voucher_id=voucher_id)
            
            if result['success']:
                voucher_products = result.get('data', [])
                if voucher_products:
                    # Add voucher_id to each product mapping
                    for product in voucher_products:
                        product['voucher_id'] = voucher_id
                    all_voucher_products.extend(voucher_products)
                    logger.info(f"📊 Voucher {voucher_id}: {len(voucher_products)} products")
            else:
                logger.warning(f"⚠️ Failed to fetch products for voucher {voucher_id}: {result.get('error')}")
        
        logger.info(f"🎉 Completed voucher products fetch: {len(all_voucher_products)} total mappings")
        self.data_cache['voucher_products'] = all_voucher_products
        return all_voucher_products
    
    def run_full_etl(self, days_back: int = 30, output_dir: str = "data/lazada_dimensional") -> Dict[str, pd.DataFrame]:
        """Run the complete ETL process"""
        logger.info("🚀 Starting Lazada Dimensional ETL Process...")
        
        try:
            # Step 1: Get access token
            self.get_access_token()
            
            # Step 2: Fetch all data
            logger.info("📊 Phase 1: Data Extraction")
            orders = self.fetch_all_orders(days_back=days_back)
            
            order_items = []
            if orders:
                order_ids = [order.get('order_id') for order in orders if order.get('order_id')]
                order_items = self.fetch_order_items_for_orders(order_ids)
            
            products = self.fetch_all_products()
            vouchers = self.fetch_all_vouchers()
            
            # Fetch voucher products if we have vouchers
            voucher_products = []
            if vouchers:
                voucher_ids = [v.get('voucher_id') for v in vouchers if v.get('voucher_id')]
                if voucher_ids:
                    voucher_products = self.fetch_voucher_products_for_vouchers(voucher_ids)
            
            # Step 3: Transform to dimensional format
            logger.info("🔄 Phase 2: Data Transformation")
            raw_dataframes = {
                'orders': pd.DataFrame(self.data_cache.get('orders', [])),
                'order_items': pd.DataFrame(self.data_cache.get('order_items', [])),
                'products': pd.DataFrame(self.data_cache.get('products', [])),
                'vouchers': pd.DataFrame(self.data_cache.get('vouchers', [])),
                'voucher_products': pd.DataFrame(self.data_cache.get('voucher_products', []))
            }
            
            # Transform to dimensional schema
            dimensional_data = self.transformer.transform_all_dimensions(raw_dataframes)
            
            # Step 4: Save to CSV
            logger.info("💾 Phase 3: Data Output")
            import os
            os.makedirs(output_dir, exist_ok=True)
            self.transformer.save_to_csv(dimensional_data, output_dir)
            
            logger.info("🎉 ETL Process Completed Successfully!")
            logger.info(f"📁 Output saved to: {output_dir}")
            
            return dimensional_data
            
        except Exception as e:
            logger.error(f"❌ ETL Process Failed: {str(e)}")
            raise


def main():
    """Main execution function"""
    etl = LazadaDimensionalETL()
    
    try:
        # Run ETL for last 30 days
        dimensional_data = etl.run_full_etl(days_back=30)
        
        print("\n" + "="*60)
        print("🎯 LAZADA DIMENSIONAL ETL COMPLETED")
        print("="*60)
        print("\nDimensional Tables Created:")
        for name, df in dimensional_data.items():
            print(f"  • {name}: {len(df)} records")
        print(f"\n📁 Files saved to: data/lazada_dimensional/")
        print("✅ Ready for data warehouse loading!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ ETL Failed: {str(e)}")
        print("Please check your tokens and API connectivity.")


if __name__ == "__main__":
    main()