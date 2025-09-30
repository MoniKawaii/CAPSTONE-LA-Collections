-- LA Collections Star Schema - Complete SQL Script
-- Copy this entire script into Supabase SQL Editor and run it

-- Drop existing tables if they exist (optional - only if recreating)
DROP TABLE IF EXISTS "Fact_Activity" CASCADE;
DROP TABLE IF EXISTS "Fact_Traffic" CASCADE;
DROP TABLE IF EXISTS "Fact_Orders" CASCADE;
DROP TABLE IF EXISTS "Dim_Product" CASCADE;
DROP TABLE IF EXISTS "Dim_Customer" CASCADE;
DROP TABLE IF EXISTS "Dim_Time" CASCADE;
DROP TABLE IF EXISTS "Dim_Platform" CASCADE;

-- Create Dimension Tables
CREATE TABLE "Dim_Platform" (
    "platform_id" SERIAL PRIMARY KEY,
    "platform_name" VARCHAR UNIQUE NOT NULL,
    "platform_region" VARCHAR NOT NULL
);

CREATE TABLE "Dim_Time" (
    "time_id" INT PRIMARY KEY NOT NULL,
    "date" DATE UNIQUE NOT NULL,
    "day_of_week" INT,
    "month" INT,
    "quarter" INT,
    "year" INT,
    "is_weekend" BOOLEAN
);

CREATE TABLE "Dim_Customer" (
    "customer_id" SERIAL PRIMARY KEY,
    "name" VARCHAR NOT NULL,
    "email" VARCHAR UNIQUE NOT NULL,
    "phone" VARCHAR,
    "address" TEXT,
    "customer_segment" VARCHAR,
    "registration_date" DATE
);

CREATE TABLE "Dim_Product" (
    "product_id" SERIAL PRIMARY KEY,
    "sku" VARCHAR UNIQUE NOT NULL,
    "name" VARCHAR NOT NULL,
    "category" VARCHAR,
    "brand" VARCHAR,
    "price" DECIMAL(10,2),
    "description" TEXT
);

-- Create Fact Tables
CREATE TABLE "Fact_Orders" (
    "order_fact_id" SERIAL PRIMARY KEY,
    "platform_id" INT REFERENCES "Dim_Platform"("platform_id"),
    "time_id" INT REFERENCES "Dim_Time"("time_id"),
    "customer_id" INT REFERENCES "Dim_Customer"("customer_id"),
    "product_id" INT REFERENCES "Dim_Product"("product_id"),
    "order_id" VARCHAR NOT NULL,
    "quantity" INT,
    "unit_price" DECIMAL(10,2),
    "total_amount" DECIMAL(10,2),
    "shipping_fee" DECIMAL(10,2),
    "order_status" VARCHAR,
    "payment_method" VARCHAR
);

CREATE TABLE "Fact_Traffic" (
    "traffic_fact_id" SERIAL PRIMARY KEY,
    "platform_id" INT REFERENCES "Dim_Platform"("platform_id"),
    "time_id" INT REFERENCES "Dim_Time"("time_id"),
    "page_views" INT,
    "unique_visitors" INT,
    "bounce_rate" DECIMAL(5,4),
    "avg_session_duration" INT,
    "conversion_rate" DECIMAL(6,5),
    UNIQUE("platform_id", "time_id")
);

CREATE TABLE "Fact_Activity" (
    "activity_fact_id" SERIAL PRIMARY KEY,
    "platform_id" INT REFERENCES "Dim_Platform"("platform_id"),
    "time_id" INT REFERENCES "Dim_Time"("time_id"),
    "customer_id" INT REFERENCES "Dim_Customer"("customer_id"),
    "product_id" INT REFERENCES "Dim_Product"("product_id"),
    "activity_type" VARCHAR NOT NULL,
    "activity_count" INT DEFAULT 1,
    "activity_value" DECIMAL(10,2)
);

-- Insert initial platform data
INSERT INTO "Dim_Platform" ("platform_name", "platform_region") 
VALUES 
    ('Lazada', 'Southeast Asia'),
    ('Shopee', 'Southeast Asia')
ON CONFLICT ("platform_name") DO NOTHING;

-- Create helper function for exec_sql (used by Python scripts)
CREATE OR REPLACE FUNCTION exec_sql(sql text)
RETURNS TABLE(result text)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    EXECUTE sql;
    RETURN QUERY SELECT 'SQL executed successfully'::text;
EXCEPTION 
    WHEN OTHERS THEN
        RETURN QUERY SELECT format('Error: %s', SQLERRM)::text;
END;
$$;

-- Verify tables were created
SELECT 
    table_name,
    table_type
FROM information_schema.tables 
WHERE table_schema = 'public' 
    AND (table_name LIKE 'Dim_%' OR table_name LIKE 'Fact_%')
ORDER BY table_name;