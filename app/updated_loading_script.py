"""
Updated Loading Script for Star Schema
Uses Supabase client instead of direct psycopg2 to avoid connection issues
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add app directory to path
sys.path.append('./app')
sys.path.append('.')

from dotenv import load_dotenv
from supabase_client import supabase

# Load environment variables
load_dotenv()

def get_sample_transactions():
    """Generate sample transaction data for testing"""
    print("Generating sample transaction data...")
    
    sample_data = [
        {
            'transaction_id': 'TXN001',
            'product_name': 'Sample Product 1',
            'quantity': 2,
            'price': 29.99,
            'customer_name': 'John Doe',
            'customer_email': 'john@example.com',
            'transaction_date': '2025-09-30',
            'source': 'Lazada'
        },
        {
            'transaction_id': 'TXN002',
            'product_name': 'Sample Product 2',
            'quantity': 1,
            'price': 49.99,
            'customer_name': 'Jane Smith',
            'customer_email': 'jane@example.com',
            'transaction_date': '2025-09-30',
            'source': 'Shopee'
        }
    ]
    
    return pd.DataFrame(sample_data)

def load_transactions_to_star_schema(df):
    """Load transaction data into star schema tables"""
    print("Loading data into star schema...")
    
    try:
        # Convert transaction data to star schema format
        orders_loaded = 0
        
        for _, row in df.iterrows():
            # Create time dimension
            trans_date = datetime.strptime(row['transaction_date'], '%Y-%m-%d')
            time_key = int(trans_date.strftime('%Y%m%d'))
            
            time_sql = f"""
            INSERT INTO "Dim_Time" (time_id, date, year, month, day, quarter, day_of_week, is_weekend)
            VALUES (
                {time_key},
                '{row['transaction_date']}',
                {trans_date.year},
                {trans_date.month},
                {trans_date.day},
                {(trans_date.month - 1) // 3 + 1},
                {trans_date.weekday()},
                {trans_date.weekday() >= 5}
            )
            ON CONFLICT (time_id) DO NOTHING;
            """
            supabase.rpc('exec_sql', {'sql': time_sql}).execute()
            
            # Create customer dimension
            customer_sql = f"""
            INSERT INTO "Dim_Customer" (name, email, customer_segment, registration_date)
            VALUES (
                '{row['customer_name']}',
                '{row['customer_email']}',
                'Regular',
                '{row['transaction_date']}'
            )
            ON CONFLICT (email) DO NOTHING;
            """
            supabase.rpc('exec_sql', {'sql': customer_sql}).execute()
            
            # Create product dimension
            product_sql = f"""
            INSERT INTO "Dim_Product" (sku, name, price, description)
            VALUES (
                '{row['transaction_id']}_SKU',
                '{row['product_name']}',
                {row['price']},
                'Sample product from transaction {row['transaction_id']}'
            )
            ON CONFLICT (sku) DO NOTHING;
            """
            supabase.rpc('exec_sql', {'sql': product_sql}).execute()
            
            # Get platform ID
            platform_id = 1 if row['source'] == 'Lazada' else 2
            
            # Create fact order
            order_sql = f"""
            INSERT INTO "Fact_Orders" (
                platform_id, time_id, customer_id, product_id, order_id,
                quantity, unit_price, total_amount, order_status, payment_method
            )
            SELECT 
                {platform_id} as platform_id,
                {time_key} as time_id,
                c.customer_id,
                p.product_id,
                '{row['transaction_id']}',
                {row['quantity']},
                {row['price']},
                {row['quantity'] * row['price']},
                'completed',
                'credit_card'
            FROM "Dim_Customer" c, "Dim_Product" p
            WHERE c.email = '{row['customer_email']}'
            AND p.sku = '{row['transaction_id']}_SKU'
            LIMIT 1;
            """
            
            result = supabase.rpc('exec_sql', {'sql': order_sql}).execute()
            orders_loaded += 1
            print(f"   ✓ Loaded order {row['transaction_id']}")
        
        return {
            'success': True,
            'orders_loaded': orders_loaded,
            'message': f'Successfully loaded {orders_loaded} orders into star schema'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Failed to load data: {str(e)}'
        }

def verify_data():
    """Verify data was loaded correctly"""
    print("\nVerifying loaded data...")
    
    try:
        # Check orders
        orders_sql = """
        SELECT 
            fo.order_id,
            p.name as product_name,
            c.name as customer_name,
            fo.total_amount,
            t.date as order_date,
            pl.platform_name
        FROM "Fact_Orders" fo
        JOIN "Dim_Product" p ON fo.product_id = p.product_id
        JOIN "Dim_Customer" c ON fo.customer_id = c.customer_id
        JOIN "Dim_Time" t ON fo.time_id = t.time_id
        JOIN "Dim_Platform" pl ON fo.platform_id = pl.platform_id
        ORDER BY fo.order_id;
        """
        
        result = supabase.rpc('exec_sql', {'sql': orders_sql}).execute()
        
        if result.data:
            print("Orders in database:")
            for order in result.data:
                print(f"  - {order['order_id']}: {order['product_name']} by {order['customer_name']} (${order['total_amount']}) on {order['platform_name']}")
        else:
            print("No orders found in database")
            
    except Exception as e:
        print(f"Error verifying data: {e}")

if __name__ == "__main__":
    print("=== Updated Loading Script for Star Schema ===")
    
    # Test Supabase connection
    try:
        test_result = supabase.rpc('exec_sql', {'sql': 'SELECT current_timestamp;'}).execute()
        print("✅ Supabase connection successful")
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        exit(1)
    
    # Get sample data (replace with your actual data sources)
    print("\n1. Getting transaction data...")
    transactions_df = get_sample_transactions()
    print(f"   Found {len(transactions_df)} transactions")
    
    # Load to star schema
    print("\n2. Loading to star schema...")
    result = load_transactions_to_star_schema(transactions_df)
    
    if result['success']:
        print(f"   ✅ {result['message']}")
    else:
        print(f"   ❌ {result['message']}")
    
    # Verify the data
    print("\n3. Verifying data...")
    verify_data()
    
    print("\n=== Loading process complete ===")