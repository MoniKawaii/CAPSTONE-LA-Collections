"""
Simple Mock Data Loader using Direct SQL
Bypasses Supabase schema cache issues
"""

import random
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

def setup_supabase():
    """Initialize Supabase client"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        raise ValueError("Missing Supabase credentials in environment variables")
    
    return create_client(url, key)

def execute_sql(supabase: Client, sql: str) -> dict:
    """Execute raw SQL using Supabase RPC"""
    try:
        response = supabase.rpc('exec_sql', {'sql': sql}).execute()
        return {"success": True, "data": response.data}
    except Exception as e:
        return {"success": False, "error": str(e)}

def load_simple_mock_data():
    """Load a few mock records directly via SQL"""
    print("=== Loading Simple Mock Data ===")
    
    supabase = setup_supabase()
    
    try:
        # Generate today's time dimension
        today = datetime.now()
        time_key = today.strftime('%Y%m%d')
        
        print("1. Creating time dimension...")
        time_sql = f"""
        INSERT INTO public."Dim_Time" (time_id, date, year, month, day, quarter, day_of_week, is_weekend)
        VALUES (
            {time_key},
            '{today.strftime('%Y-%m-%d')}',
            {today.year},
            {today.month},
            {today.day},
            {(today.month - 1) // 3 + 1},
            {today.weekday()},
            {today.weekday() >= 5}
        )
        ON CONFLICT (time_id) DO NOTHING;
        """
        result = execute_sql(supabase, time_sql)
        print(f"Time dimension result: {result}")
        
        # Create a test customer
        print("2. Creating customer dimension...")
        customer_sql = """
        INSERT INTO public."Dim_Customer" (name, email, phone, address, customer_segment, registration_date)
        VALUES (
            'Test Customer',
            'test@example.com',
            '+639123456789',
            'Test Address, Manila, Philippines',
            'Regular',
            CURRENT_DATE
        )
        ON CONFLICT (email) DO NOTHING;
        """
        result = execute_sql(supabase, customer_sql)
        print(f"Customer dimension result: {result}")
        
        # Create a test product
        print("3. Creating product dimension...")
        product_sql = """
        INSERT INTO public."Dim_Product" (sku, name, category, brand, price, description)
        VALUES (
            'TEST001',
            'Test Product',
            'Electronics',
            'Test Brand',
            99.99,
            'A test product for verification'
        )
        ON CONFLICT (sku) DO NOTHING;
        """
        result = execute_sql(supabase, product_sql)
        print(f"Product dimension result: {result}")
        
        # Create a fact order
        print("4. Creating fact order...")
        order_sql = f"""
        INSERT INTO public."Fact_Orders" (
            platform_id, time_id, customer_id, product_id, order_id, 
            quantity, unit_price, total_amount, shipping_fee, order_status, payment_method
        )
        SELECT 
            1 as platform_id,  -- Lazada
            {time_key} as time_id,
            c.customer_id,
            p.product_id,
            'TEST{random.randint(10000, 99999)}',
            2,
            99.99,
            209.98,
            10.00,
            'completed',
            'credit_card'
        FROM public."Dim_Customer" c, public."Dim_Product" p
        WHERE c.email = 'test@example.com'
        AND p.sku = 'TEST001'
        LIMIT 1;
        """
        result = execute_sql(supabase, order_sql)
        print(f"Fact order result: {result}")
        
        # Create fact traffic
        print("5. Creating fact traffic...")
        traffic_sql = f"""
        INSERT INTO public."Fact_Traffic" (
            platform_id, time_id, page_views, unique_visitors, 
            bounce_rate, avg_session_duration, conversion_rate
        )
        VALUES (
            1,  -- Lazada
            {time_key},
            {random.randint(100, 500)},
            {random.randint(50, 200)},
            {round(random.uniform(0.3, 0.7), 2)},
            {random.randint(120, 300)},
            {round(random.uniform(0.02, 0.08), 3)}
        )
        ON CONFLICT (platform_id, time_id) DO NOTHING;
        """
        result = execute_sql(supabase, traffic_sql)
        print(f"Fact traffic result: {result}")
        
        # Verify data was loaded
        print("6. Verifying data...")
        verify_sql = """
        SELECT 
            fo.order_id,
            p.name as product_name,
            c.name as customer_name,
            fo.total_amount,
            t.date as order_date
        FROM public."Fact_Orders" fo
        JOIN public."Dim_Product" p ON fo.product_id = p.product_id
        JOIN public."Dim_Customer" c ON fo.customer_id = c.customer_id
        JOIN public."Dim_Time" t ON fo.time_id = t.time_id
        WHERE fo.order_id LIKE 'TEST%'
        LIMIT 5;
        """
        verification = execute_sql(supabase, verify_sql)
        print(f"Verification result: {verification}")
        
        return {
            "success": True,
            "message": "Mock data loaded successfully via direct SQL",
            "verification": verification.get("data", [])
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to load mock data: {str(e)}"
        }

if __name__ == "__main__":
    result = load_simple_mock_data()
    print(f"\nFinal result: {result}")