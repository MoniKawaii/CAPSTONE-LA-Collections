"""
Lazada Data Retrieval Service

This service fetches data from Lazada API and integrates with the ETL pipeline.
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

from app.lazada_service import lazada_service
from app.csv_etl import process_csv_file
from app.supabase_client import get_supabase_client

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LazadaDataService:
    def __init__(self):
        self.supabase = get_supabase_client()
    
    async def fetch_and_process_products(self, 
                                       limit: int = 100, 
                                       save_to_db: bool = True) -> Dict[str, Any]:
        """
        Fetch products from Lazada API and process through ETL pipeline
        
        Args:
            limit (int): Number of products to fetch
            save_to_db (bool): Whether to save to database
            
        Returns:
            Dict: Processing results
        """
        try:
            logger.info(f"🔄 Fetching {limit} products from Lazada API...")
            
            # Fetch products in batches
            all_products = []
            offset = 0
            batch_size = min(50, limit)  # Lazada API limit per request
            
            while len(all_products) < limit:
                remaining = limit - len(all_products)
                current_batch_size = min(batch_size, remaining)
                
                # Fetch batch
                products_response = await lazada_service.get_products(
                    limit=current_batch_size, 
                    offset=offset
                )
                
                # Extract products from response
                products = products_response.get('data', {}).get('products', [])
                
                if not products:
                    logger.info("📝 No more products to fetch")
                    break
                
                all_products.extend(products)
                offset += current_batch_size
                
                logger.info(f"📦 Fetched {len(all_products)} products so far...")
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)
            
            if not all_products:
                return {
                    "status": "success",
                    "message": "No products found",
                    "products_fetched": 0,
                    "products_processed": 0
                }
            
            # Convert to DataFrame format expected by ETL
            df = self._convert_products_to_dataframe(all_products)
            
            # Process through ETL pipeline
            if save_to_db:
                logger.info("💾 Processing products through ETL pipeline...")
                
                # Create a CSV-like string from DataFrame for ETL processing
                csv_string = df.to_csv(index=False)
                csv_io = pd.StringIO(csv_string)
                
                # Process through existing ETL pipeline
                etl_result = process_csv_file(csv_io, platform="Lazada", save_to_db=True)
                
                return {
                    "status": "success",
                    "message": f"Fetched and processed {len(all_products)} products",
                    "products_fetched": len(all_products),
                    "products_processed": etl_result.get("inserted", 0),
                    "etl_result": etl_result
                }
            else:
                return {
                    "status": "success",
                    "message": f"Fetched {len(all_products)} products",
                    "products_fetched": len(all_products),
                    "dataframe": df,
                    "columns": list(df.columns),
                    "sample_data": df.head(5).to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error fetching products: {e}")
            return {
                "status": "error",
                "message": str(e),
                "products_fetched": 0,
                "products_processed": 0
            }
    
    async def fetch_and_process_orders(self, 
                                     days_back: int = 7,
                                     save_to_db: bool = True) -> Dict[str, Any]:
        """
        Fetch orders from Lazada API and process through ETL pipeline
        
        Args:
            days_back (int): Number of days back to fetch orders
            save_to_db (bool): Whether to save to database
            
        Returns:
            Dict: Processing results
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            created_after = start_date.strftime("%Y-%m-%dT%H:%M:%S%z")
            created_before = end_date.strftime("%Y-%m-%dT%H:%M:%S%z")
            
            logger.info(f"🔄 Fetching orders from {start_date.date()} to {end_date.date()}...")
            
            # Fetch orders
            orders_response = await lazada_service.get_orders(
                created_after=created_after,
                created_before=created_before,
                limit=100
            )
            
            # Extract orders from response
            orders = orders_response.get('data', {}).get('orders', [])
            
            if not orders:
                return {
                    "status": "success",
                    "message": "No orders found for the specified period",
                    "orders_fetched": 0,
                    "orders_processed": 0
                }
            
            # Convert to DataFrame format
            df = self._convert_orders_to_dataframe(orders)
            
            # Process through ETL pipeline if requested
            if save_to_db:
                logger.info("💾 Processing orders through ETL pipeline...")
                
                # Create a CSV-like string from DataFrame
                csv_string = df.to_csv(index=False)
                csv_io = pd.StringIO(csv_string)
                
                # Process through ETL pipeline (orders might need special handling)
                etl_result = process_csv_file(csv_io, platform="Lazada", save_to_db=True)
                
                return {
                    "status": "success",
                    "message": f"Fetched and processed {len(orders)} orders",
                    "orders_fetched": len(orders),
                    "orders_processed": etl_result.get("inserted", 0),
                    "etl_result": etl_result
                }
            else:
                return {
                    "status": "success",
                    "message": f"Fetched {len(orders)} orders",
                    "orders_fetched": len(orders),
                    "dataframe": df,
                    "columns": list(df.columns),
                    "sample_data": df.head(5).to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error fetching orders: {e}")
            return {
                "status": "error",
                "message": str(e),
                "orders_fetched": 0,
                "orders_processed": 0
            }
    
    def _convert_products_to_dataframe(self, products: List[Dict]) -> pd.DataFrame:
        """
        Convert Lazada products API response to DataFrame format expected by ETL
        
        Args:
            products (List[Dict]): Products from Lazada API
            
        Returns:
            pd.DataFrame: Formatted DataFrame
        """
        # Extract relevant fields and flatten nested data
        flattened_products = []
        
        for product in products:
            # Get the first SKU for main product info
            skus = product.get('skus', [])
            main_sku = skus[0] if skus else {}
            
            # Extract price information
            price = main_sku.get('price', 0)
            special_price = main_sku.get('special_price')
            
            flattened_product = {
                'product_id': str(product.get('item_id', '')),
                'lazada_item_id': str(product.get('item_id', '')),
                'product_name': product.get('attributes', {}).get('name', ''),
                'brand': product.get('attributes', {}).get('brand', ''),
                'category': product.get('primary_category', ''),
                'price': float(price) if price else 0.0,
                'special_price': float(special_price) if special_price else None,
                'quantity': int(main_sku.get('quantity', 0)),
                'status': product.get('status', ''),
                'created_time': product.get('created_time', ''),
                'updated_time': product.get('updated_time', ''),
                'weight': main_sku.get('package_weight', ''),
                'description': product.get('attributes', {}).get('description', ''),
                'images': '; '.join(product.get('images', [])),
                'platform': 'Lazada'
            }
            
            flattened_products.append(flattened_product)
        
        df = pd.DataFrame(flattened_products)
        
        # Ensure data types
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
        
        logger.info(f"📊 Converted {len(df)} products to DataFrame with columns: {list(df.columns)}")
        
        return df
    
    def _convert_orders_to_dataframe(self, orders: List[Dict]) -> pd.DataFrame:
        """
        Convert Lazada orders API response to DataFrame format
        
        Args:
            orders (List[Dict]): Orders from Lazada API
            
        Returns:
            pd.DataFrame: Formatted DataFrame
        """
        flattened_orders = []
        
        for order in orders:
            # Get order items
            order_items = order.get('order_items', [])
            
            for item in order_items:
                flattened_order = {
                    'order_id': str(order.get('order_id', '')),
                    'order_number': str(order.get('order_number', '')),
                    'customer_first_name': order.get('customer_first_name', ''),
                    'customer_last_name': order.get('customer_last_name', ''),
                    'order_status': order.get('statuses', [None])[0] if order.get('statuses') else '',
                    'created_at': order.get('created_at', ''),
                    'updated_at': order.get('updated_at', ''),
                    'item_id': str(item.get('order_item_id', '')),
                    'lazada_item_id': str(item.get('shop_sku', '')),
                    'product_name': item.get('name', ''),
                    'sku': item.get('sku', ''),
                    'quantity': int(item.get('quantity', 0)),
                    'item_price': float(item.get('item_price', 0)),
                    'total_amount': float(order.get('price', 0)),
                    'shipping_fee': float(order.get('shipping_fee', 0)),
                    'payment_method': order.get('payment_method', ''),
                    'delivery_type': order.get('delivery_type', ''),
                    'platform': 'Lazada'
                }
                
                flattened_orders.append(flattened_order)
        
        df = pd.DataFrame(flattened_orders)
        
        # Ensure data types
        numeric_columns = ['quantity', 'item_price', 'total_amount', 'shipping_fee']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        logger.info(f"📊 Converted {len(df)} order items to DataFrame with columns: {list(df.columns)}")
        
        return df
    
    async def run_automated_sync(self, 
                               products_limit: int = 100,
                               orders_days_back: int = 7) -> Dict[str, Any]:
        """
        Run automated data synchronization
        
        Args:
            products_limit (int): Number of products to sync
            orders_days_back (int): Days back for orders sync
            
        Returns:
            Dict: Sync results
        """
        logger.info("🚀 Starting automated Lazada data sync...")
        
        results = {
            "sync_started_at": datetime.now().isoformat(),
            "products_result": None,
            "orders_result": None,
            "overall_status": "success"
        }
        
        try:
            # Sync products
            logger.info("📦 Syncing products...")
            products_result = await self.fetch_and_process_products(
                limit=products_limit, 
                save_to_db=True
            )
            results["products_result"] = products_result
            
            # Sync orders
            logger.info("📋 Syncing orders...")
            orders_result = await self.fetch_and_process_orders(
                days_back=orders_days_back,
                save_to_db=True
            )
            results["orders_result"] = orders_result
            
            # Check if any operations failed
            if (products_result.get("status") == "error" or 
                orders_result.get("status") == "error"):
                results["overall_status"] = "partial_success"
            
            results["sync_completed_at"] = datetime.now().isoformat()
            
            logger.info("✅ Automated sync completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Automated sync failed: {e}")
            results["overall_status"] = "error"
            results["error"] = str(e)
            results["sync_failed_at"] = datetime.now().isoformat()
        
        return results

# Global service instance
lazada_data_service = LazadaDataService()