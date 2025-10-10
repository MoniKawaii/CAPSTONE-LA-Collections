"""
Lazada Dimensional Data Transformer
Transforms raw Lazada API data into dimensional schema format

Transforms data for:
- Fact Sales table 
- Dim Customer, Product, Time, Promotion tables
- Proper Lazada price calculations and mappings
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class LazadaDimensionalTransformer:
    """
    Transforms raw Lazada API data into dimensional schema format
    """
    
    def __init__(self):
        self.current_date = datetime.now()
        
    def create_time_dimension_key(self, date_str: str) -> Optional[int]:
        """Create time dimension key in YYYYMMDD format"""
        try:
            if pd.isna(date_str) or not date_str:
                return None
            dt = pd.to_datetime(date_str)
            return int(dt.strftime('%Y%m%d'))
        except:
            return None
    
    def calculate_lazada_prices(self, row: pd.Series) -> Dict[str, float]:
        """
        Calculate Lazada pricing components according to schema
        Returns: unit_price, item_price, paid_price, original_price, wallet_credits
        """
        try:
            # Get base values
            item_price = float(row.get('item_price', 0) or 0)
            paid_price = float(row.get('paid_price', 0) or 0)
            product_main_sku = row.get('product_main_sku', '')
            
            # Calculate unit price (item_price / quantity)
            quantity = int(row.get('purchase_order_number', 1) or 1)
            unit_price = item_price / quantity if quantity > 0 else item_price
            
            # Original price (before any discounts)
            original_price = float(row.get('product_detail_url', 0) or item_price)  # Using available field
            
            # Wallet credits (difference between item_price and paid_price)
            wallet_credits = max(0, item_price - paid_price)
            
            return {
                'unit_price': round(unit_price, 2),
                'item_price': round(item_price, 2),
                'paid_price': round(paid_price, 2),
                'original_price': round(original_price, 2),
                'wallet_credits': round(wallet_credits, 2)
            }
        except Exception as e:
            logger.warning(f"Price calculation error: {e}")
            return {
                'unit_price': 0.0,
                'item_price': 0.0,
                'paid_price': 0.0,
                'original_price': 0.0,
                'wallet_credits': 0.0
            }
    
    def transform_fact_sales(self, order_items_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Transform order items and orders into fact_sales table"""
        logger.info("🔄 Transforming Fact Sales data...")
        
        if order_items_df.empty:
            return pd.DataFrame()
        
        # Merge order items with order data
        merged_df = order_items_df.merge(
            orders_df, 
            left_on='order_id', 
            right_on='order_id', 
            how='left',
            suffixes=('_item', '_order')
        )
        
        fact_sales = []
        
        for _, row in merged_df.iterrows():
            # Calculate pricing components
            prices = self.calculate_lazada_prices(row)
            
            # Create time dimension keys
            order_date_key = self.create_time_dimension_key(row.get('created_at'))
            
            fact_record = {
                'sales_id': f"SALE_{row.get('order_item_id', 'UNKNOWN')}",
                'order_id': row.get('order_id'),
                'order_item_id': row.get('order_item_id'),
                'customer_id': f"CUST_{row.get('address_billing', {}).get('first_name', 'UNKNOWN')}",
                'product_id': f"PROD_{row.get('sku', 'UNKNOWN')}",
                'time_id': order_date_key,
                'promotion_id': f"PROMO_{row.get('voucher_code', 'NONE')}" if row.get('voucher_code') else None,
                
                # Quantity and Pricing
                'quantity_ordered': int(row.get('purchase_order_number', 1) or 1),
                'unit_price': prices['unit_price'],
                'item_price': prices['item_price'],
                'paid_price': prices['paid_price'],
                'original_price': prices['original_price'],
                'wallet_credits': prices['wallet_credits'],
                
                # Calculated fields
                'total_discount': prices['original_price'] - prices['paid_price'],
                'gross_revenue': prices['item_price'],
                'net_revenue': prices['paid_price'],
                
                # Status fields
                'order_status': row.get('status_order', 'unknown'),
                'payment_method': row.get('payment_method', 'unknown'),
                
                # Timestamps
                'order_created_at': row.get('created_at'),
                'order_updated_at': row.get('updated_at'),
                
                # Additional metadata
                'currency': 'PHP',  # Assuming Philippines
                'tracking_code': row.get('tracking_code_pre', ''),
                'shop_sku': row.get('shop_sku', ''),
                'variation': row.get('variation', ''),
                'product_main_sku': row.get('product_main_sku', ''),
                'order_type': row.get('order_type', 'normal')
            }
            
            fact_sales.append(fact_record)
        
        result_df = pd.DataFrame(fact_sales)
        logger.info(f"✅ Transformed {len(result_df)} fact sales records")
        return result_df
    
    def transform_dim_customer(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Transform orders into dim_customer table"""
        logger.info("🔄 Transforming Customer Dimension...")
        
        if orders_df.empty:
            return pd.DataFrame()
        
        customers = []
        processed_customers = set()
        
        for _, row in orders_df.iterrows():
            billing = row.get('address_billing', {}) or {}
            shipping = row.get('address_shipping', {}) or {}
            
            # Create customer ID
            customer_key = f"{billing.get('first_name', 'Unknown')}_{billing.get('last_name', '')}"
            
            if customer_key in processed_customers:
                continue
            processed_customers.add(customer_key)
            
            customer_record = {
                'customer_id': f"CUST_{customer_key}",
                'customer_name': f"{billing.get('first_name', '')} {billing.get('last_name', '')}".strip(),
                'email': billing.get('customer_email', ''),
                'phone': billing.get('phone', ''),
                
                # Billing Address
                'billing_address': billing.get('address1', ''),
                'billing_city': billing.get('city', ''),
                'billing_region': billing.get('address2', ''),
                'billing_country': billing.get('country', ''),
                'billing_postcode': billing.get('post_code', ''),
                
                # Shipping Address
                'shipping_address': shipping.get('address1', ''),
                'shipping_city': shipping.get('city', ''),
                'shipping_region': shipping.get('address2', ''),
                'shipping_country': shipping.get('country', ''),
                'shipping_postcode': shipping.get('post_code', ''),
                
                # Metadata
                'customer_since': row.get('created_at'),
                'last_order_date': row.get('updated_at'),
                'total_orders': 1  # Will be aggregated later
            }
            
            customers.append(customer_record)
        
        result_df = pd.DataFrame(customers)
        logger.info(f"✅ Transformed {len(result_df)} customer records")
        return result_df
    
    def transform_dim_product(self, order_items_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """Transform products into dim_product table"""
        logger.info("🔄 Transforming Product Dimension...")
        
        products = []
        processed_products = set()
        
        # Process from order items first
        for _, row in order_items_df.iterrows():
            sku = row.get('sku', '')
            if not sku or sku in processed_products:
                continue
            processed_products.add(sku)
            
            product_record = {
                'product_id': f"PROD_{sku}",
                'sku': sku,
                'product_name': row.get('name', 'Unknown Product'),
                'shop_sku': row.get('shop_sku', ''),
                'product_main_sku': row.get('product_main_sku', ''),
                'variation': row.get('variation', ''),
                'category': 'Unknown',  # Will be filled from products API
                'brand': 'Unknown',
                'status': 'active',
                'weight': 0.0,
                'dimensions': '',
                'created_at': self.current_date.isoformat(),
                'updated_at': self.current_date.isoformat()
            }
            products.append(product_record)
        
        # Enhance with products API data
        if not products_df.empty:
            for _, product_row in products_df.iterrows():
                sku = product_row.get('seller_sku', '')
                
                # Find existing product record
                for product in products:
                    if product['sku'] == sku:
                        product.update({
                            'category': product_row.get('primary_category', 'Unknown'),
                            'brand': product_row.get('brand', 'Unknown'),
                            'status': product_row.get('status', 'active'),
                            'weight': float(product_row.get('package_weight', 0) or 0),
                            'dimensions': f"{product_row.get('package_length', 0)}x{product_row.get('package_width', 0)}x{product_row.get('package_height', 0)}"
                        })
                        break
        
        result_df = pd.DataFrame(products)
        logger.info(f"✅ Transformed {len(result_df)} product records")
        return result_df
    
    def transform_dim_time(self, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """Generate time dimension table"""
        logger.info("🔄 Generating Time Dimension...")
        
        if not start_date:
            start_date = datetime.now() - timedelta(days=365)
        if not end_date:
            end_date = datetime.now() + timedelta(days=30)
        
        time_records = []
        current_date = start_date
        
        while current_date <= end_date:
            time_record = {
                'time_id': int(current_date.strftime('%Y%m%d')),
                'date': current_date.date(),
                'year': current_date.year,
                'quarter': f"Q{((current_date.month - 1) // 3) + 1}",
                'month': current_date.month,
                'month_name': current_date.strftime('%B'),
                'week': current_date.isocalendar()[1],
                'day': current_date.day,
                'day_name': current_date.strftime('%A'),
                'day_of_week': current_date.weekday() + 1,
                'day_of_year': current_date.timetuple().tm_yday,
                'is_weekend': current_date.weekday() >= 5,
                'is_holiday': False  # Could be enhanced with holiday calendar
            }
            time_records.append(time_record)
            current_date += timedelta(days=1)
        
        result_df = pd.DataFrame(time_records)
        logger.info(f"✅ Generated {len(result_df)} time dimension records")
        return result_df
    
    def transform_dim_promotion(self, vouchers_df: pd.DataFrame, order_items_df: pd.DataFrame) -> pd.DataFrame:
        """Transform vouchers into dim_promotion table"""
        logger.info("🔄 Transforming Promotion Dimension...")
        
        promotions = []
        processed_promos = set()
        
        # Add default "no promotion" record
        promotions.append({
            'promotion_id': 'PROMO_NONE',
            'promotion_name': 'No Promotion',
            'promotion_type': 'none',
            'discount_type': 'none',
            'discount_value': 0.0,
            'start_date': None,
            'end_date': None,
            'status': 'active',
            'description': 'No promotion applied'
        })
        processed_promos.add('PROMO_NONE')
        
        # Process vouchers from API
        for _, row in vouchers_df.iterrows():
            voucher_code = row.get('voucher_code', '')
            promotion_id = f"PROMO_{voucher_code}"
            
            if promotion_id in processed_promos:
                continue
            processed_promos.add(promotion_id)
            
            promotion_record = {
                'promotion_id': promotion_id,
                'promotion_name': row.get('voucher_name', voucher_code),
                'promotion_type': 'voucher',
                'discount_type': row.get('discount_type', 'unknown'),
                'discount_value': float(row.get('discount_value', 0) or 0),
                'start_date': row.get('start_time'),
                'end_date': row.get('end_time'),
                'status': row.get('status', 'unknown'),
                'description': row.get('description', ''),
                'voucher_code': voucher_code
            }
            promotions.append(promotion_record)
        
        # Process promotions found in order items
        for _, row in order_items_df.iterrows():
            voucher_code = row.get('voucher_code', '')
            if voucher_code:
                promotion_id = f"PROMO_{voucher_code}"
                if promotion_id not in processed_promos:
                    processed_promos.add(promotion_id)
                    
                    promotion_record = {
                        'promotion_id': promotion_id,
                        'promotion_name': f"Voucher {voucher_code}",
                        'promotion_type': 'voucher',
                        'discount_type': 'unknown',
                        'discount_value': 0.0,
                        'start_date': None,
                        'end_date': None,
                        'status': 'used',
                        'description': f"Voucher code: {voucher_code}",
                        'voucher_code': voucher_code
                    }
                    promotions.append(promotion_record)
        
        result_df = pd.DataFrame(promotions)
        logger.info(f"✅ Transformed {len(result_df)} promotion records")
        return result_df
    
    def transform_orders_table(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw orders data into Orders operational table"""
        logger.info("🔄 Transforming Orders table...")
        
        if orders_df.empty:
            logger.warning("⚠️ No orders data to transform")
            return pd.DataFrame()
        
        orders_list = []
        
        for _, row in orders_df.iterrows():
            order_record = {
                'order_key': None,  # Will be set by database
                'order_id': row.get('order_id'),
                'order_number': row.get('order_number', row.get('order_id')),
                'time_key': self.create_time_dimension_key(row.get('created_at')),
                'customer_key': None,  # Will be resolved via customer_id lookup
                'order_date': pd.to_datetime(row.get('created_at')),
                'order_status': row.get('statuses', 'unknown'),
                'payment_method': row.get('payment_method', 'unknown'),
                'shipping_address': str(row.get('address_shipping', '')),
                'billing_address': str(row.get('address_billing', '')),
                
                # Pricing according to Lazada schema
                'order_total_price': float(row.get('price', 0) or 0),
                'shipping_fee': float(row.get('shipping_fee', 0) or 0),
                'voucher_seller': float(row.get('voucher_seller', 0) or 0),
                'voucher_platform': float(row.get('voucher_platform', 0) or 0),
                'voucher_total': float(row.get('voucher', 0) or 0),
                'buyer_paid_price': float(row.get('price', 0) or 0) - float(row.get('voucher', 0) or 0) + float(row.get('shipping_fee', 0) or 0),
                
                'items_count': int(row.get('items_count', 0) or 0),
                'warehouse_code': row.get('warehouse_code', ''),
                'tracking_code': row.get('tracking_code', ''),
                'promised_shipping_date': pd.to_datetime(row.get('promised_shipping_time'), errors='coerce'),
                'actual_delivery_date': pd.to_datetime(row.get('delivery_time'), errors='coerce'),
                'cancellation_reason': row.get('reason', ''),
                'platform': 'Lazada',
                'platform_region': 'Philippines',
                'created_at': pd.to_datetime(row.get('created_at')),
                'updated_at': pd.to_datetime(row.get('updated_at', row.get('created_at')))
            }
            orders_list.append(order_record)
        
        result_df = pd.DataFrame(orders_list)
        logger.info(f"✅ Transformed {len(result_df)} orders records")
        return result_df
    
    def transform_order_items_table(self, order_items_df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw order items data into Order_Items operational table"""
        logger.info("🔄 Transforming Order_Items table...")
        
        if order_items_df.empty:
            logger.warning("⚠️ No order items data to transform")
            return pd.DataFrame()
        
        items_list = []
        
        for _, row in order_items_df.iterrows():
            # Calculate pricing
            quantity = int(row.get('purchase_order_number', 1) or 1)
            item_price = float(row.get('item_price', 0) or 0)
            paid_price = float(row.get('paid_price', 0) or 0)
            
            item_record = {
                'order_item_key': None,  # Will be set by database
                'order_item_id': row.get('order_item_id'),
                'order_key': None,  # Will be resolved via order_id lookup
                'product_key': None,  # Will be resolved via sku lookup
                'time_key': self.create_time_dimension_key(row.get('created_at')),
                
                'product_name': row.get('name', ''),
                'product_sku': row.get('sku', ''),
                'shop_sku': row.get('shop_sku', ''),
                'variation': row.get('variation', ''),
                'quantity': quantity,
                
                # Item pricing
                'item_price': round(item_price, 2),
                'total_item_price': round(item_price * quantity, 2),
                'paid_price': round(paid_price, 2),
                'item_voucher_discount': round(max(0, item_price - paid_price), 2),
                'shipping_amount': float(row.get('shipping_amount', 0) or 0),
                'tax_amount': float(row.get('tax_amount', 0) or 0),
                
                'item_status': row.get('status', 'unknown'),
                'cancellation_reason': row.get('reason', ''),
                'tracking_code': row.get('tracking_code', ''),
                'warehouse_code': row.get('warehouse_code', ''),
                'created_at': pd.to_datetime(row.get('created_at')),
                'updated_at': pd.to_datetime(row.get('updated_at', row.get('created_at')))
            }
            items_list.append(item_record)
        
        result_df = pd.DataFrame(items_list)
        logger.info(f"✅ Transformed {len(result_df)} order items records")
        return result_df
    
    def transform_voucher_products_table(self, voucher_products_df: pd.DataFrame) -> pd.DataFrame:
        """Transform voucher products mapping into Voucher_Products table"""
        logger.info("🔄 Transforming Voucher_Products table...")
        
        if voucher_products_df.empty:
            logger.warning("⚠️ No voucher products data to transform")
            return pd.DataFrame()
        
        voucher_products_list = []
        
        for _, row in voucher_products_df.iterrows():
            voucher_product_record = {
                'voucher_product_key': None,  # Will be set by database
                'voucher_key': None,  # Will be resolved via voucher_id lookup
                'product_key': None,  # Will be resolved via sku_id lookup
                'voucher_id': row.get('voucher_id', ''),
                'sku_id': row.get('sku_id', ''),
                'discount_value': float(row.get('discount_value', 0) or 0),
                'discount_percentage': float(row.get('discount_percentage', 0) or 0),
                'created_at': datetime.now()
            }
            voucher_products_list.append(voucher_product_record)
        
        result_df = pd.DataFrame(voucher_products_list)
        logger.info(f"✅ Transformed {len(result_df)} voucher products records")
        return result_df
    
    def transform_voucher_usage_table(self, orders_df: pd.DataFrame, order_items_df: pd.DataFrame) -> pd.DataFrame:
        """Transform order data to track voucher usage in Voucher_Usage table"""
        logger.info("🔄 Transforming Voucher_Usage table...")
        
        if orders_df.empty:
            logger.warning("⚠️ No orders data to analyze voucher usage")
            return pd.DataFrame()
        
        voucher_usage_list = []
        
        for _, row in orders_df.iterrows():
            voucher_amount = float(row.get('voucher', 0) or 0)
            
            # Only create records for orders that actually used vouchers
            if voucher_amount > 0:
                order_total = float(row.get('price', 0) or 0)
                
                usage_record = {
                    'usage_key': None,  # Will be set by database
                    'voucher_key': None,  # Will need to be resolved via voucher matching
                    'order_key': None,  # Will be resolved via order_id lookup
                    'customer_key': None,  # Will be resolved via customer_id lookup
                    'time_key': self.create_time_dimension_key(row.get('created_at')),
                    
                    'voucher_code': row.get('voucher_code', 'unknown'),
                    'discount_amount': voucher_amount,
                    'order_value_before_discount': order_total + voucher_amount,
                    'order_value_after_discount': order_total,
                    'usage_date': pd.to_datetime(row.get('created_at')),
                    'customer_segment_at_usage': 'unknown',  # Would need customer analytics
                    'first_time_customer': False,  # Would need customer history analysis
                    'created_at': pd.to_datetime(row.get('created_at'))
                }
                voucher_usage_list.append(usage_record)
        
        result_df = pd.DataFrame(voucher_usage_list)
        logger.info(f"✅ Transformed {len(result_df)} voucher usage records")
        return result_df
    
    def transform_all_dimensions(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform all raw data into dimensional format"""
        logger.info("🌟 Starting full dimensional transformation...")
        
        transformed_data = {}
        
        try:
            # Get DataFrames
            orders_df = raw_data.get('orders', pd.DataFrame())
            order_items_df = raw_data.get('order_items', pd.DataFrame())
            products_df = raw_data.get('products', pd.DataFrame())
            vouchers_df = raw_data.get('vouchers', pd.DataFrame())
            voucher_products_df = raw_data.get('voucher_products', pd.DataFrame())
            
            # Transform dimensional tables
            transformed_data['dim_customer'] = self.transform_dim_customer(orders_df)
            transformed_data['dim_product'] = self.transform_dim_product(order_items_df, products_df)
            transformed_data['dim_time'] = self.transform_dim_time()
            transformed_data['dim_promotion'] = self.transform_dim_promotion(vouchers_df, order_items_df)
            
            # Transform fact table (depends on dimensions)
            transformed_data['fact_sales'] = self.transform_fact_sales(order_items_df, orders_df)
            
            # Transform operational tables
            transformed_data['orders'] = self.transform_orders_table(orders_df)
            transformed_data['order_items'] = self.transform_order_items_table(order_items_df)
            transformed_data['voucher_products'] = self.transform_voucher_products_table(voucher_products_df)
            transformed_data['voucher_usage'] = self.transform_voucher_usage_table(orders_df, order_items_df)
            
            logger.info("🎉 Dimensional transformation completed successfully!")
            
            # Print summary
            print("\n" + "="*70)
            print("📊 DIMENSIONAL TRANSFORMATION SUMMARY")
            print("="*70)
            print("DIMENSIONAL TABLES:")
            for table_name in ['dim_customer', 'dim_product', 'dim_time', 'dim_promotion', 'fact_sales']:
                df = transformed_data.get(table_name, pd.DataFrame())
                print(f"  • {table_name}: {len(df)} records")
            print("\nOPERATIONAL TABLES:")
            for table_name in ['orders', 'order_items', 'voucher_products', 'voucher_usage']:
                df = transformed_data.get(table_name, pd.DataFrame())
                print(f"  • {table_name}: {len(df)} records")
            print("="*70)
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"❌ Transformation failed: {str(e)}")
            raise
    
    def save_to_csv(self, transformed_data: Dict[str, pd.DataFrame], output_dir: str = "data/dimensional_output"):
        """Save transformed data to CSV files"""
        import os
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"💾 Saving dimensional data to {output_dir}...")
        
        for table_name, df in transformed_data.items():
            if df.empty:
                logger.warning(f"⚠️ Skipping empty table: {table_name}")
                continue
                
            file_path = os.path.join(output_dir, f"{table_name}.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"✅ Saved {table_name}: {len(df)} records → {file_path}")
        
        logger.info("💾 All dimensional data saved successfully!")


def main():
    """Test the transformer with sample data"""
    transformer = LazadaDimensionalTransformer()
    
    # Create sample data for testing
    sample_orders = pd.DataFrame([
        {
            'order_id': 12345,
            'created_at': '2024-01-15T10:30:00',
            'updated_at': '2024-01-15T10:30:00',
            'status': 'delivered',
            'address_billing': {
                'first_name': 'Juan',
                'last_name': 'Dela Cruz',
                'customer_email': 'juan@email.com',
                'phone': '09123456789'
            }
        }
    ])
    
    sample_order_items = pd.DataFrame([
        {
            'order_id': 12345,
            'order_item_id': 67890,
            'sku': 'PROD001',
            'name': 'Sample Product',
            'item_price': 1500.0,
            'paid_price': 1350.0,
            'purchase_order_number': 2
        }
    ])
    
    raw_data = {
        'orders': sample_orders,
        'order_items': sample_order_items,
        'products': pd.DataFrame(),
        'vouchers': pd.DataFrame()
    }
    
    # Transform data
    dimensional_data = transformer.transform_all_dimensions(raw_data)
    
    print("\n🎯 Transformer test completed!")


if __name__ == "__main__":
    main()