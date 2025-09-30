"""
Star Schema ETL Pipeline for LA Collections

This script transforms CSV data into the star schema format and loads it into
the dimension and fact tables.
"""

import pandas as pd
import sys
import os
from datetime import datetime
sys.path.append('./app')

from supabase_client import supabase

def get_or_create_time_dimension(date_str):
    """Get or create time dimension entry and return time_key"""
    try:
        # Parse date string
        date_obj = pd.to_datetime(date_str).date()
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
                'is_mega_sale_day': False  # TODO: Add logic for sale days (11.11, 12.12, etc.)
            }
            
            supabase.table('Dim_Time').insert(time_data).execute()
            
        return time_key
    except Exception as e:
        print(f"Error creating time dimension: {e}")
        return None

def get_or_create_customer_dimension(customer_name, platform_id):
    """Get or create customer dimension entry and return customer_key"""
    try:
        # Create platform buyer ID (anonymized)
        platform_buyer_id = f"BUYER_{platform_id}_{abs(hash(customer_name)) % 1000000}"
        
        # Check if customer exists
        result = supabase.table('Dim_Customer').select('customer_key').eq('platform_buyer_id', platform_buyer_id).execute()
        
        if result.data:
            return result.data[0]['customer_key']
        else:
            # Create new customer dimension record
            customer_data = {
                'platform_buyer_id': platform_buyer_id,
                'city': None,  # TODO: Extract from shipping data if available
                'region': 'Philippines',  # Default for now
                'buyer_segment': 'New Buyer',  # Default, TODO: Calculate based on history
                'LTV_tier': 'Bronze',  # Default, TODO: Calculate based on purchase history
                'last_order_date': datetime.now().date().isoformat()
            }
            
            result = supabase.table('Dim_Customer').insert(customer_data).execute()
            return result.data[0]['customer_key']
            
    except Exception as e:
        print(f"Error creating customer dimension: {e}")
        return None

def get_or_create_product_dimension(product_name, platform_id):
    """Get or create product dimension entry and return product_key"""
    try:
        # Check if product exists by name
        result = supabase.table('Dim_Product').select('product_key').eq('product_name', product_name).execute()
        
        if result.data:
            return result.data[0]['product_key']
        else:
            # Create new product dimension record
            product_data = {
                'product_name': product_name,
                'lazada_item_id': None if platform_id != 1 else f"LZ_{abs(hash(product_name)) % 1000000}",
                'shopee_item_id': None if platform_id != 2 else f"SP_{abs(hash(product_name)) % 1000000}",
                'category_l2': 'General',  # Default category
                'product_rating': None,
                'review_count': 0,
                'stock_on_hand': 100,  # Default stock
                'promo_type': None
            }
            
            result = supabase.table('Dim_Product').insert(product_data).execute()
            return result.data[0]['product_key']
            
    except Exception as e:
        print(f"Error creating product dimension: {e}")
        return None

def transform_csv_to_star_schema(df, source_platform):
    """Transform CSV analytics data to star schema format"""
    print(f"Transforming {len(df)} analytics records for {source_platform} platform...")
    
    # Get platform key
    platform_key = 1 if source_platform.lower() == 'lazada' else 2
    
    fact_orders = []
    fact_traffic = []
    
    for _, row in df.iterrows():
        try:
            # Get dimension keys
            time_key = get_or_create_time_dimension(row['date'])
            
            # For analytics data, we create aggregate records
            if time_key:
                # Create fact traffic record (page views, visitors)
                fact_traffic_record = {
                    'time_key': time_key,
                    'product_key': 1,  # Default product for aggregated data
                    'customer_key': 1,  # Default customer for aggregated data
                    'platform_key': platform_key,
                    'page_views': int(row.get('page_views', 0)),
                    'visits': int(row.get('unique_visitors', 0)),
                    'add_to_cart_count': int(row.get('add_to_cart_units', 0)),
                    'wishlist_add_count': int(row.get('wishlist_count', 0))
                }
                fact_traffic.append(fact_traffic_record)
                
                # Create fact orders record (sales data)
                if row.get('total_orders', 0) > 0:
                    fact_order_record = {
                        'time_key': time_key,
                        'product_key': 1,  # Default product for aggregated data
                        'customer_key': 1,  # Default customer for aggregated data  
                        'platform_key': platform_key,
                        'paid_price': float(row.get('total_sales_value', 0)),
                        'item_quantity': int(row.get('units_sold', 0)),
                        'cancellation_reason': None,
                        'return_reason': None,
                        'seller_commission_fee': float(row.get('total_sales_value', 0)) * 0.05,
                        'platform_subsidy_amount': 0.0
                    }
                    fact_orders.append(fact_order_record)
                
        except Exception as e:
            print(f"Error processing row: {e}")
            continue
    
    return fact_orders, fact_traffic

def create_default_dimensions():
    """Create default dimension records for aggregated data"""
    try:
        # Create default customer for aggregated data
        default_customer = {
            'platform_buyer_id': 'AGGREGATED_DATA',
            'city': 'Philippines',
            'region': 'Philippines',
            'buyer_segment': 'Aggregated',
            'LTV_tier': 'N/A',
            'last_order_date': datetime.now().date().isoformat()
        }
        
        result = supabase.table('Dim_Customer').select('customer_key').eq('platform_buyer_id', 'AGGREGATED_DATA').execute()
        if not result.data:
            supabase.table('Dim_Customer').insert(default_customer).execute()
        
        # Create default product for aggregated data
        default_product = {
            'product_name': 'Aggregated Sales Data',
            'lazada_item_id': 'AGG_DATA',
            'shopee_item_id': 'AGG_DATA',
            'category_l2': 'Aggregated',
            'product_rating': None,
            'review_count': 0,
            'stock_on_hand': 0,
            'promo_type': 'N/A'
        }
        
        result = supabase.table('Dim_Product').select('product_key').eq('product_name', 'Aggregated Sales Data').execute()
        if not result.data:
            supabase.table('Dim_Product').insert(default_product).execute()
            
        print("Default dimensions created for aggregated data")
        
    except Exception as e:
        print(f"Warning: Could not create default dimensions: {e}")

def load_fact_data(fact_orders, fact_traffic):
    """Load fact data into the database"""
    success = True
    
    try:
        if fact_orders:
            result = supabase.table('Fact_Orders').insert(fact_orders).execute()
            print(f"Successfully loaded {len(fact_orders)} order records into Fact_Orders table")
        
        if fact_traffic:
            result = supabase.table('Fact_Traffic').insert(fact_traffic).execute()
            print(f"Successfully loaded {len(fact_traffic)} traffic records into Fact_Traffic table")
            
        return success
        
    except Exception as e:
        print(f"Error loading fact data: {e}")
        return False

def process_csv_to_star_schema(file_path, source_platform='manual'):
    """Process CSV file and load into star schema"""
    try:
        # Read and transform CSV
        from etl import process_csv_file
        df_result = process_csv_file(file_path, source_platform, save_to_db=False)
        
        if df_result["status"] == "success":
            df = df_result["dataframe"]
            
            # Create default dimensions for aggregated data
            create_default_dimensions()
            
            # Transform to star schema
            fact_orders, fact_traffic = transform_csv_to_star_schema(df, source_platform)
            
            # Load into database
            if load_fact_data(fact_orders, fact_traffic):
                return {
                    'success': True,
                    'records_processed': len(df),
                    'fact_orders_created': len(fact_orders),
                    'fact_traffic_created': len(fact_traffic),
                    'message': f'Successfully loaded {len(fact_orders)} orders and {len(fact_traffic)} traffic records into star schema'
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to load fact data'
                }
        else:
            return {
                'success': False,
                'message': f'ETL processing failed: {df_result.get("detail", "Unknown error")}'
            }
            
    except Exception as e:
        return {
            'success': False,
            'message': f'Error processing CSV: {str(e)}'
        }

if __name__ == "__main__":
    # Test with sample data
    print("=== Star Schema ETL Pipeline Test ===")
    
    # Check if sample file exists
    sample_file = "data/samplelazada.csv"
    if os.path.exists(sample_file):
        result = process_csv_to_star_schema(sample_file, 'lazada')
        print(f"Result: {result}")
    else:
        print(f"Sample file {sample_file} not found")
        print("Place a CSV file in the data folder to test the ETL pipeline")