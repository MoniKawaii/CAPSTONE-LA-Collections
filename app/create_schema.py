"""
Database Schema Creation Script for LA Collections

This script creates all the tables according to the LA_Collections_Schema.sql
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get database connection using environment variables"""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL not found in environment variables")
    return psycopg2.connect(db_url)

def create_tables():
    """Create all tables according to the schema"""
    
    # SQL commands to create tables
    create_table_commands = [
        # Dimension Tables
        '''
        CREATE TABLE IF NOT EXISTS "Dim_Platform" (
            "platform_key" SERIAL PRIMARY KEY,
            "platform_name" VARCHAR UNIQUE NOT NULL,
            "platform_region" VARCHAR NOT NULL
        );
        ''',
        
        '''
        CREATE TABLE IF NOT EXISTS "Dim_Time" (
            "time_key" INT PRIMARY KEY NOT NULL,
            "date" DATE UNIQUE NOT NULL,
            "day_of_week" INT,
            "month" INT,
            "year" INT,
            "is_mega_sale_day" BOOLEAN
        );
        ''',
        
        '''
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
        
        '''
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
        
        # Fact Tables
        '''
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
        
        '''
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
        
        '''
        CREATE TABLE IF NOT EXISTS "Fact_Activity" (
            "activity_event_key" BIGSERIAL PRIMARY KEY,
            "time_key" INT NOT NULL,
            "customer_key" INT NOT NULL,
            "platform_key" INT NOT NULL,
            "activity_type" VARCHAR NOT NULL,
            "chat_response_time_seconds" INT,
            "follower_count_change" INT
        );
        '''
    ]
    
    # Foreign key constraints
    foreign_key_commands = [
        'ALTER TABLE "Fact_Orders" ADD CONSTRAINT IF NOT EXISTS fk_orders_time FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");',
        'ALTER TABLE "Fact_Orders" ADD CONSTRAINT IF NOT EXISTS fk_orders_product FOREIGN KEY ("product_key") REFERENCES "Dim_Product" ("product_key");',
        'ALTER TABLE "Fact_Orders" ADD CONSTRAINT IF NOT EXISTS fk_orders_customer FOREIGN KEY ("customer_key") REFERENCES "Dim_Customer" ("customer_key");',
        'ALTER TABLE "Fact_Orders" ADD CONSTRAINT IF NOT EXISTS fk_orders_platform FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");',
        
        'ALTER TABLE "Fact_Traffic" ADD CONSTRAINT IF NOT EXISTS fk_traffic_time FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");',
        'ALTER TABLE "Fact_Traffic" ADD CONSTRAINT IF NOT EXISTS fk_traffic_product FOREIGN KEY ("product_key") REFERENCES "Dim_Product" ("product_key");',
        'ALTER TABLE "Fact_Traffic" ADD CONSTRAINT IF NOT EXISTS fk_traffic_customer FOREIGN KEY ("customer_key") REFERENCES "Dim_Customer" ("customer_key");',
        'ALTER TABLE "Fact_Traffic" ADD CONSTRAINT IF NOT EXISTS fk_traffic_platform FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");',
        
        'ALTER TABLE "Fact_Activity" ADD CONSTRAINT IF NOT EXISTS fk_activity_time FOREIGN KEY ("time_key") REFERENCES "Dim_Time" ("time_key");',
        'ALTER TABLE "Fact_Activity" ADD CONSTRAINT IF NOT EXISTS fk_activity_customer FOREIGN KEY ("customer_key") REFERENCES "Dim_Customer" ("customer_key");',
        'ALTER TABLE "Fact_Activity" ADD CONSTRAINT IF NOT EXISTS fk_activity_platform FOREIGN KEY ("platform_key") REFERENCES "Dim_Platform" ("platform_key");'
    ]
    
    # Comments for documentation
    comment_commands = [
        'COMMENT ON COLUMN "Dim_Platform"."platform_key" IS \'Surrogate key for the marketplace (1=Lazada, 2=Shopee)\';',
        'COMMENT ON COLUMN "Dim_Platform"."platform_region" IS \'e.g., PH, MY, SG\';',
        'COMMENT ON COLUMN "Dim_Time"."time_key" IS \'Surrogate key, format YYYYMMDD\';',
        'COMMENT ON COLUMN "Dim_Time"."is_mega_sale_day" IS \'TRUE for 11.11, 12.12, etc.\';',
        'COMMENT ON COLUMN "Dim_Customer"."customer_key" IS \'Internal anonymous surrogate ID\';',
        'COMMENT ON COLUMN "Dim_Customer"."platform_buyer_id" IS \'Masked, unique buyer ID from the marketplace API (PII-compliant)\';',
        'COMMENT ON COLUMN "Dim_Customer"."city" IS \'Customer Location derived from shipping address\';',
        'COMMENT ON COLUMN "Dim_Customer"."region" IS \'e.g., Metro Manila, Provincial\';',
        'COMMENT ON COLUMN "Dim_Customer"."buyer_segment" IS \'Calculated: New Buyer or Returning Buyer\';',
        'COMMENT ON COLUMN "Dim_Customer"."LTV_tier" IS \'Calculated: Gold, Silver, Bronze\';',
        'COMMENT ON COLUMN "Dim_Customer"."last_order_date" IS \'For Recency (RFM) analysis\';',
        'COMMENT ON COLUMN "Dim_Product"."product_key" IS \'Your internal universal SKU ID\';',
        'COMMENT ON COLUMN "Dim_Product"."lazada_item_id" IS \'Lazada specific product ID\';',
        'COMMENT ON COLUMN "Dim_Product"."shopee_item_id" IS \'Shopee specific product ID\';',
        'COMMENT ON COLUMN "Dim_Product"."category_l2" IS \'Specific product category\';',
        'COMMENT ON COLUMN "Dim_Product"."stock_on_hand" IS \'Snapshot of inventory level\';',
        'COMMENT ON COLUMN "Dim_Product"."promo_type" IS \'e.g., Flash Sale, Platform Voucher\';',
        'COMMENT ON COLUMN "Fact_Orders"."order_item_key" IS \'Unique ID for each line item in an order\';',
        'COMMENT ON COLUMN "Fact_Orders"."paid_price" IS \'Total revenue for this item (for AOV and Sales Revenue)\';',
        'COMMENT ON COLUMN "Fact_Orders"."item_quantity" IS \'Units Sold\';',
        'COMMENT ON COLUMN "Fact_Orders"."seller_commission_fee" IS \'Fee paid by seller to platform\';',
        'COMMENT ON COLUMN "Fact_Orders"."platform_subsidy_amount" IS \'Voucher/discount amount subsidized by the platform\';',
        'COMMENT ON COLUMN "Fact_Activity"."activity_type" IS \'e.g., CHAT_SENT, SHOP_FOLLOWED, COUPON_CLAIMED\';',
        'COMMENT ON COLUMN "Fact_Activity"."follower_count_change" IS \'e.g., +1 when shop is followed\';'
    ]
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print("Creating LA Collections database schema...")
        
        # Create tables
        print("\n1. Creating dimension and fact tables...")
        for i, command in enumerate(create_table_commands, 1):
            try:
                cursor.execute(command)
                table_name = command.split('"')[1]
                print(f"   ✓ Created table: {table_name}")
            except Exception as e:
                print(f"   ✗ Error creating table: {e}")
        
        # Add foreign keys
        print("\n2. Adding foreign key constraints...")
        for command in foreign_key_commands:
            try:
                cursor.execute(command)
            except Exception as e:
                print(f"   ⚠ Foreign key constraint (might already exist): {e}")
        print("   ✓ Foreign key constraints processed")
        
        # Add comments
        print("\n3. Adding table and column comments...")
        for command in comment_commands:
            try:
                cursor.execute(command)
            except Exception as e:
                print(f"   ⚠ Comment warning: {e}")
        print("   ✓ Comments added")
        
        # Commit changes
        conn.commit()
        print("\n✅ Schema creation completed successfully!")
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'Dim_%' OR table_name LIKE 'Fact_%'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print(f"\n📊 Created {len(tables)} tables:")
        for table in tables:
            print(f"   • {table[0]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def insert_initial_data():
    """Insert initial platform data"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print("\n4. Inserting initial platform data...")
        
        # Insert platform data
        platform_data = [
            (1, 'Lazada', 'PH'),
            (2, 'Shopee', 'PH')
        ]
        
        for platform_key, platform_name, platform_region in platform_data:
            cursor.execute('''
                INSERT INTO "Dim_Platform" ("platform_key", "platform_name", "platform_region")
                VALUES (%s, %s, %s)
                ON CONFLICT ("platform_name") DO NOTHING;
            ''', (platform_key, platform_name, platform_region))
        
        conn.commit()
        print("   ✓ Platform data inserted")
        
    except Exception as e:
        print(f"   ⚠ Initial data warning: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("=== LA Collections Database Schema Setup ===")
    create_tables()
    insert_initial_data()
    print("\n🎉 Database setup complete!")
    print("\nNext steps:")
    print("1. Your dimension and fact tables are ready")
    print("2. You can now load data using the ETL pipeline")
    print("3. Platform keys: 1=Lazada, 2=Shopee")