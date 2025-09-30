"""
Database Schema Creation using Supabase Client

This script creates all the tables according to the LA_Collections_Schema.sql
using the existing Supabase client setup.
"""

import sys
import os
sys.path.append('./app')

from supabase_client import supabase

def execute_sql(sql_command, description=""):
    """Execute a SQL command using Supabase client"""
    try:
        result = supabase.rpc('exec_sql', {'sql': sql_command})
        if description:
            print(f"   ✓ {description}")
        return True
    except Exception as e:
        print(f"   ✗ Error {description}: {e}")
        return False

def create_tables():
    """Create all tables according to the schema"""
    
    print("=== LA Collections Database Schema Setup ===")
    print("Creating tables using Supabase client...\n")
    
    # Create tables one by one
    tables_sql = [
        # Dimension Tables
        {
            'sql': '''
            CREATE TABLE IF NOT EXISTS "Dim_Platform" (
                "platform_key" SERIAL PRIMARY KEY,
                "platform_name" VARCHAR UNIQUE NOT NULL,
                "platform_region" VARCHAR NOT NULL
            );
            ''',
            'description': 'Created Dim_Platform table'
        },
        
        {
            'sql': '''
            CREATE TABLE IF NOT EXISTS "Dim_Time" (
                "time_key" INT PRIMARY KEY NOT NULL,
                "date" DATE UNIQUE NOT NULL,
                "day_of_week" INT,
                "month" INT,
                "year" INT,
                "is_mega_sale_day" BOOLEAN
            );
            ''',
            'description': 'Created Dim_Time table'
        },
        
        {
            'sql': '''
            CREATE TABLE IF NOT EXISTS "Dim_Customer" (
                "customer_key" SERIAL PRIMARY KEY,
                "platform_buyer_id" VARCHAR UNIQUE NOT NULL,
                "city" VARCHAR,
                "region" VARCHAR,
                "buyer_segment" VARCHAR,
                "LTV_tier" VARCHAR,
                "last_order_date" DATE
            );
            ''',
            'description': 'Created Dim_Customer table'
        },
        
        {
            'sql': '''
            CREATE TABLE IF NOT EXISTS "Dim_Product" (
                "product_key" SERIAL PRIMARY KEY,
                "lazada_item_id" VARCHAR,
                "shopee_item_id" VARCHAR,
                "product_name" VARCHAR,
                "category_l2" VARCHAR,
                "product_rating" DECIMAL,
                "review_count" INT,
                "stock_on_hand" INT,
                "promo_type" VARCHAR
            );
            ''',
            'description': 'Created Dim_Product table'
        },
        
        # Fact Tables
        {
            'sql': '''
            CREATE TABLE IF NOT EXISTS "Fact_Orders" (
                "order_item_key" BIGSERIAL PRIMARY KEY,
                "time_key" INT NOT NULL,
                "product_key" INT NOT NULL,
                "customer_key" INT NOT NULL,
                "platform_key" INT NOT NULL,
                "paid_price" DECIMAL NOT NULL,
                "item_quantity" INT NOT NULL,
                "cancellation_reason" VARCHAR,
                "return_reason" VARCHAR,
                "seller_commission_fee" DECIMAL,
                "platform_subsidy_amount" DECIMAL
            );
            ''',
            'description': 'Created Fact_Orders table'
        },
        
        {
            'sql': '''
            CREATE TABLE IF NOT EXISTS "Fact_Traffic" (
                "traffic_event_key" BIGSERIAL PRIMARY KEY,
                "time_key" INT NOT NULL,
                "product_key" INT NOT NULL,
                "customer_key" INT NOT NULL,
                "platform_key" INT NOT NULL,
                "page_views" INT NOT NULL,
                "visits" INT NOT NULL,
                "add_to_cart_count" INT,
                "wishlist_add_count" INT
            );
            ''',
            'description': 'Created Fact_Traffic table'
        },
        
        {
            'sql': '''
            CREATE TABLE IF NOT EXISTS "Fact_Activity" (
                "activity_event_key" BIGSERIAL PRIMARY KEY,
                "time_key" INT NOT NULL,
                "customer_key" INT NOT NULL,
                "platform_key" INT NOT NULL,
                "activity_type" VARCHAR NOT NULL,
                "chat_response_time_seconds" INT,
                "follower_count_change" INT
            );
            ''',
            'description': 'Created Fact_Activity table'
        }
    ]
    
    # Execute table creation
    print("1. Creating dimension and fact tables...")
    success_count = 0
    for table in tables_sql:
        if execute_sql(table['sql'], table['description']):
            success_count += 1
    
    print(f"\n✅ Successfully created {success_count}/{len(tables_sql)} tables")
    
    # Insert initial platform data
    print("\n2. Inserting initial platform data...")
    platform_insert_sql = '''
    INSERT INTO "Dim_Platform" ("platform_key", "platform_name", "platform_region")
    VALUES 
        (1, 'Lazada', 'PH'),
        (2, 'Shopee', 'PH')
    ON CONFLICT ("platform_name") DO NOTHING;
    '''
    
    if execute_sql(platform_insert_sql, "Inserted platform data (Lazada=1, Shopee=2)"):
        print("   • Platform key 1 = Lazada")
        print("   • Platform key 2 = Shopee")
    
    # Show created tables
    print("\n3. Verifying created tables...")
    try:
        # Query to get all tables
        result = supabase.rpc('exec_sql', {
            'sql': '''
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE 'Dim_%' OR table_name LIKE 'Fact_%')
            ORDER BY table_name;
            '''
        })
        
        if result and result.data:
            print(f"   ✓ Found {len(result.data)} tables:")
            for table in result.data:
                print(f"      • {table['table_name']}")
        else:
            print("   ⚠ Could not verify tables (but they should be created)")
            
    except Exception as e:
        print(f"   ⚠ Verification error: {e}")
    
    print("\n🎉 Database schema setup complete!")
    print("\nNext steps:")
    print("1. Your star schema is ready for analytics")
    print("2. Load data using ETL pipeline")
    print("3. Platform keys: 1=Lazada, 2=Shopee")
    print("4. Time keys use YYYYMMDD format")

def test_connection():
    """Test the Supabase connection"""
    try:
        # Simple test query
        result = supabase.rpc('exec_sql', {'sql': 'SELECT current_timestamp;'})
        print("✅ Supabase connection successful")
        return True
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Supabase connection...")
    if test_connection():
        print()
        create_tables()
    else:
        print("Please check your Supabase configuration in .env file")