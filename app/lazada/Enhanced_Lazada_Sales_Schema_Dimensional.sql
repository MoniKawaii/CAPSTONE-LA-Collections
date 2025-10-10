-- Enhanced Lazada Sales Database Schema with Dimensional Modeling
-- Designed for comprehensive sales analytics, voucher tracking, and time-based analysis
-- Includes fact tables, dimension tables, and voucher management with proper price calculations

-- =============================================================================
-- DIMENSION TABLES (Master Data)
-- =============================================================================

-- TIME DIMENSION TABLE - For time-based analysis and OLAP cubes
CREATE TABLE "Dim_Time" (
  "time_key" int PRIMARY KEY NOT NULL, -- Format: YYYYMMDD
  "date" date NOT NULL,
  "year" int NOT NULL,
  "quarter" int NOT NULL,
  "month" int NOT NULL,
  "month_name" varchar(20) NOT NULL,
  "week" int NOT NULL,
  "day_of_month" int NOT NULL,
  "day_of_week" int NOT NULL,
  "day_name" varchar(20) NOT NULL,
  "is_weekend" boolean NOT NULL,
  "is_holiday" boolean DEFAULT FALSE,
  "fiscal_year" int,
  "fiscal_quarter" int,
  "season" varchar(20), -- Spring, Summer, Fall, Winter
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP
);

-- CUSTOMER DIMENSION TABLE
CREATE TABLE "Dim_Customers" (
  "customer_key" serial PRIMARY KEY,
  "customer_id" varchar UNIQUE NOT NULL, -- Business key
  "first_name" varchar,
  "last_name" varchar,
  "full_name" varchar,
  "city" varchar,
  "region" varchar,
  "country" varchar,
  "phone" varchar,
  "total_orders" int DEFAULT 0,
  "total_spent" decimal(12,2) DEFAULT 0,
  "average_order_value" decimal(10,2) DEFAULT 0,
  "first_order_date" date,
  "last_order_date" date,
  "customer_segment" varchar DEFAULT 'New', -- New, Regular, VIP, Inactive
  "customer_lifetime_value" decimal(12,2) DEFAULT 0,
  "platform" varchar DEFAULT 'Lazada',
  "is_active" boolean DEFAULT TRUE,
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP
);

-- PRODUCT DIMENSION TABLE
CREATE TABLE "Dim_Products" (
  "product_key" serial PRIMARY KEY,
  "sku_id" varchar UNIQUE NOT NULL, -- Business key
  "product_name" varchar NOT NULL,
  "product_sku" varchar,
  "shop_sku" varchar,
  "category" varchar,
  "last_price" decimal(10,2),
  "cost_price" decimal(10,2), 
  "product_image_url" text,
  "product_detail_url" text,
  "total_sales_quantity" int DEFAULT 0,
  "total_sales_amount" decimal(12,2) DEFAULT 0,
  "average_selling_price" decimal(10,2),
  "first_sold_date" date,
  "last_sold_date" date,
  "inventory_status" varchar DEFAULT 'Active', -- Active, Inactive, Discontinued
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP
);

-- VOUCHER DIMENSION TABLE
CREATE TABLE "Dim_Vouchers" (
  "voucher_key" serial PRIMARY KEY,
  "voucher_id" varchar UNIQUE NOT NULL, -- From /promotion/vouchers/get
  "voucher_code" varchar,
  "voucher_name" varchar,
  "voucher_type" varchar, -- percentage, fixed_amount, shipping, etc.
  "discount_type" varchar, -- seller, platform, both
  "discount_value" decimal(10,2),
  "discount_percentage" decimal(5,2),
  "minimum_spend" decimal(10,2),
  "maximum_discount" decimal(10,2),
  "usage_limit" int,
  "usage_limit_per_customer" int,
  "start_date" timestamp,
  "end_date" timestamp,
  "target_audience" varchar, -- all, new_customers, vip, etc.
  "applicable_categories" text, -- JSON array of category IDs
  "applicable_products" text, -- JSON array of product IDs
  "status" varchar DEFAULT 'Active', -- Active, Inactive, Expired
  "created_by" varchar,
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- TRANSACTIONAL TABLES
-- =============================================================================

-- ORDERS TABLE - Transactional data
CREATE TABLE "Orders" (
  "order_key" serial PRIMARY KEY,
  "order_id" bigint UNIQUE NOT NULL, -- Business key
  "order_number" bigint NOT NULL,
  "time_key" int REFERENCES "Dim_Time"("time_key"),
  "customer_key" int REFERENCES "Dim_Customers"("customer_key"),
  "order_date" timestamp NOT NULL,
  "order_status" varchar NOT NULL,
  "payment_method" varchar,
  
  -- PRICE CALCULATIONS (as per Lazada formula)
  "order_total_price" decimal(10,2) NOT NULL, -- Original price (excluding discounts)
  "shipping_fee" decimal(10,2) DEFAULT 0,
  "voucher_seller" decimal(10,2) DEFAULT 0,
  "voucher_platform" decimal(10,2) DEFAULT 0,
  "voucher_total" decimal(10,2) DEFAULT 0, -- Total voucher discount
  "buyer_paid_price" decimal(10,2) NOT NULL, -- Final amount: price - voucher + shipping_fee
  "items_count" int DEFAULT 0,
  "warehouse_code" varchar,
  "tracking_code" varchar,
  "cancellation_reason" varchar,
  "platform" varchar DEFAULT 'Lazada',
  "platform_region" varchar DEFAULT 'Philippines',
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  
  INDEX idx_orders_date ("order_date"),
  INDEX idx_orders_status ("order_status"),
  INDEX idx_orders_customer ("customer_key"),
  INDEX idx_orders_time ("time_key")
);

-- ORDER ITEMS TABLE - Individual product sales
CREATE TABLE "Order_Items" (
  "order_item_key" serial PRIMARY KEY,
  "order_item_id" bigint UNIQUE NOT NULL, -- Business key
  "order_key" int REFERENCES "Orders"("order_key"),
  "product_key" int REFERENCES "Dim_Products"("product_key"),
  "time_key" int REFERENCES "Dim_Time"("time_key"),
  
  "product_name" varchar NOT NULL,
  "product_sku" varchar,
  "shop_sku" varchar,
  "variation" text, -- Color, size, etc.
  "quantity" int DEFAULT 1,
  
  -- ITEM PRICING
  "item_price" decimal(10,2) NOT NULL, -- Unit price
  "total_item_price" decimal(10,2) NOT NULL, -- item_price * quantity
  "paid_price" decimal(10,2) NOT NULL, -- After discounts
  "item_voucher_discount" decimal(10,2) DEFAULT 0,
  "shipping_amount" decimal(10,2) DEFAULT 0,
  "tax_amount" decimal(10,2) DEFAULT 0,
  
  "item_status" varchar,
  "cancellation_reason" varchar,
  "tracking_code" varchar,
  "warehouse_code" varchar,
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  
  INDEX idx_order_items_order ("order_key"),
  INDEX idx_order_items_product ("product_key"),
  INDEX idx_order_items_date ("time_key")
);

-- VOUCHER PRODUCTS MAPPING - From /promotion/voucher/products/get
CREATE TABLE "Voucher_Products" (
  "voucher_product_key" serial PRIMARY KEY,
  "voucher_key" int REFERENCES "Dim_Vouchers"("voucher_key"),
  "product_key" int REFERENCES "Dim_Products"("product_key"),
  "voucher_id" varchar NOT NULL,
  "sku_id" varchar NOT NULL,
  "discount_value" decimal(10,2),
  "discount_percentage" decimal(5,2),
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  
  UNIQUE(voucher_key, product_key)
);

-- VOUCHER USAGE TRACKING - Track voucher effectiveness
CREATE TABLE "Voucher_Usage" (
  "usage_key" serial PRIMARY KEY,
  "voucher_key" int REFERENCES "Dim_Vouchers"("voucher_key"),
  "order_key" int REFERENCES "Orders"("order_key"),
  "customer_key" int REFERENCES "Dim_Customers"("customer_key"),
  "time_key" int REFERENCES "Dim_Time"("time_key"),
  
  "voucher_code" varchar NOT NULL,
  "discount_amount" decimal(10,2) NOT NULL,
  "order_value_before_discount" decimal(10,2),
  "order_value_after_discount" decimal(10,2),
  "usage_date" timestamp NOT NULL,
  "customer_segment_at_usage" varchar,
  "first_time_customer" boolean DEFAULT FALSE,
  
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  
  INDEX idx_voucher_usage_voucher ("voucher_key"),
  INDEX idx_voucher_usage_date ("time_key"),
  INDEX idx_voucher_usage_customer ("customer_key")
);

-- =============================================================================
-- FACT TABLE - Main analytical table
-- =============================================================================

-- FACT SALES TABLE - Central fact table for OLAP analysis
CREATE TABLE "Fact_Sales" (
  "sales_key" serial PRIMARY KEY,
  "time_key" int REFERENCES "Dim_Time"("time_key"),
  "customer_key" int REFERENCES "Dim_Customers"("customer_key"),
  "product_key" int REFERENCES "Dim_Products"("product_key"),
  "order_key" int REFERENCES "Orders"("order_key"),
  "voucher_key" int REFERENCES "Dim_Vouchers"("voucher_key"),
  
  -- MEASURES (Additive facts)
  "quantity_sold" int NOT NULL,
  "gross_sales_amount" decimal(12,2) NOT NULL, -- Before discounts
  "discount_amount" decimal(12,2) DEFAULT 0,
  "net_sales_amount" decimal(12,2) NOT NULL, -- After discounts
  "shipping_revenue" decimal(10,2) DEFAULT 0,
  "profit_amount" decimal(12,2), -- If cost data available
  
  -- SEMI-ADDITIVE MEASURES
  "unit_price" decimal(10,2) NOT NULL,
  "unit_cost" decimal(10,2),
  
  -- FLAGS
  "is_cancelled" boolean DEFAULT FALSE,
  "is_returned" boolean DEFAULT FALSE,
  "is_voucher_used" boolean DEFAULT FALSE,
  
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  
  INDEX idx_fact_sales_time ("time_key"),
  INDEX idx_fact_sales_customer ("customer_key"),
  INDEX idx_fact_sales_product ("product_key"),
  INDEX idx_fact_sales_order ("order_key")
);

-- =============================================================================
-- SUMMARY/AGGREGATE TABLES
-- =============================================================================

-- DAILY SALES SUMMARY - Pre-aggregated for performance
CREATE TABLE "Sales_Summary" (
  "summary_key" serial PRIMARY KEY,
  "time_key" int REFERENCES "Dim_Time"("time_key"),
  "date" date NOT NULL,
  "platform" varchar DEFAULT 'Lazada',
  
  -- ORDER METRICS
  "total_orders" int DEFAULT 0,
  "successful_orders" int DEFAULT 0,
  "cancelled_orders" int DEFAULT 0,
  "returned_orders" int DEFAULT 0,
  
  -- SALES METRICS
  "total_items_sold" int DEFAULT 0,
  "gross_revenue" decimal(12,2) DEFAULT 0,
  "total_discounts" decimal(12,2) DEFAULT 0,
  "net_revenue" decimal(12,2) DEFAULT 0,
  "shipping_revenue" decimal(10,2) DEFAULT 0,
  "average_order_value" decimal(10,2) DEFAULT 0,
  
  -- CUSTOMER METRICS
  "unique_customers" int DEFAULT 0,
  "new_customers" int DEFAULT 0,
  "returning_customers" int DEFAULT 0,
  
  -- VOUCHER METRICS
  "vouchers_used" int DEFAULT 0,
  "voucher_discount_total" decimal(12,2) DEFAULT 0,
  "voucher_adoption_rate" decimal(5,2) DEFAULT 0, -- Percentage of orders using vouchers
  
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  
  UNIQUE(time_key, platform)
);

-- =============================================================================
-- ANALYTICAL VIEWS
-- =============================================================================

-- View: Sales Performance by Product
CREATE VIEW "v_product_performance" AS
SELECT 
  p.product_name,
  p.product_sku,
  p.category,
  COUNT(DISTINCT f.order_key) as orders_count,
  SUM(f.quantity_sold) as total_quantity_sold,
  SUM(f.gross_sales_amount) as gross_sales,
  SUM(f.discount_amount) as total_discounts,
  SUM(f.net_sales_amount) as net_sales,
  AVG(f.unit_price) as avg_selling_price,
  SUM(f.net_sales_amount) / NULLIF(SUM(f.quantity_sold), 0) as revenue_per_unit
FROM "Fact_Sales" f
JOIN "Dim_Products" p ON f.product_key = p.product_key
WHERE f.is_cancelled = FALSE
GROUP BY p.product_key, p.product_name, p.product_sku, p.category;

-- View: Voucher Effectiveness Analysis
CREATE VIEW "v_voucher_effectiveness" AS
SELECT 
  v.voucher_code,
  v.voucher_name,
  v.voucher_type,
  v.start_date,
  v.end_date,
  COUNT(vu.usage_key) as total_usage,
  COUNT(DISTINCT vu.customer_key) as unique_customers,
  SUM(vu.discount_amount) as total_discount_given,
  AVG(vu.discount_amount) as avg_discount_per_use,
  SUM(vu.order_value_after_discount) as total_order_value_generated,
  COUNT(CASE WHEN vu.first_time_customer THEN 1 END) as new_customer_acquisitions,
  (SUM(vu.order_value_after_discount) - SUM(vu.discount_amount)) as net_revenue_generated
FROM "Dim_Vouchers" v
LEFT JOIN "Voucher_Usage" vu ON v.voucher_key = vu.voucher_key
GROUP BY v.voucher_key, v.voucher_code, v.voucher_name, v.voucher_type, v.start_date, v.end_date;

-- View: Customer Segmentation Analysis
CREATE VIEW "v_customer_segments" AS
SELECT 
  c.customer_segment,
  COUNT(*) as customer_count,
  AVG(c.total_orders) as avg_orders_per_customer,
  AVG(c.average_order_value) as avg_order_value,
  AVG(c.total_spent) as avg_customer_lifetime_value,
  SUM(c.total_spent) as segment_total_revenue,
  AVG(EXTRACT(DAYS FROM (c.last_order_date - c.first_order_date))) as avg_customer_lifespan_days
FROM "Dim_Customers" c
WHERE c.is_active = TRUE
GROUP BY c.customer_segment;

-- View: Time-based Sales Trends
CREATE VIEW "v_sales_trends" AS
SELECT 
  t.year,
  t.quarter,
  t.month_name,
  t.week,
  t.day_name,
  t.is_weekend,
  ss.total_orders,
  ss.net_revenue,
  ss.average_order_value,
  ss.unique_customers,
  ss.voucher_adoption_rate
FROM "Dim_Time" t
JOIN "Sales_Summary" ss ON t.time_key = ss.time_key
ORDER BY t.time_key;

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Additional indexes for common queries
CREATE INDEX idx_dim_customers_segment ON "Dim_Customers"("customer_segment");
CREATE INDEX idx_dim_products_category ON "Dim_Products"("category");
CREATE INDEX idx_dim_vouchers_type ON "Dim_Vouchers"("voucher_type");
CREATE INDEX idx_dim_vouchers_dates ON "Dim_Vouchers"("start_date", "end_date");
CREATE INDEX idx_dim_time_year_month ON "Dim_Time"("year", "month");

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================

COMMENT ON TABLE "Dim_Time" IS 'Time dimension for OLAP analysis and date-based reporting';
COMMENT ON TABLE "Dim_Customers" IS 'Customer master data with segmentation and lifetime value';
COMMENT ON TABLE "Dim_Products" IS 'Product catalog with performance metrics and inventory status';
COMMENT ON TABLE "Dim_Vouchers" IS 'Voucher master data for promotion tracking and analysis';
COMMENT ON TABLE "Orders" IS 'Order transactions with proper price calculations per Lazada formula';
COMMENT ON TABLE "Order_Items" IS 'Individual item sales with detailed pricing and discounts';
COMMENT ON TABLE "Fact_Sales" IS 'Central fact table for OLAP cube analysis';
COMMENT ON TABLE "Sales_Summary" IS 'Pre-aggregated daily metrics for dashboard performance';
COMMENT ON TABLE "Voucher_Usage" IS 'Tracks voucher usage patterns and effectiveness over time';

-- Sample queries for price calculations:
-- Buyer Paid Price = order_total_price - voucher_total + shipping_fee
-- Net Revenue = SUM(buyer_paid_price) 
-- Voucher Effectiveness = (Total Order Value Generated - Total Discount Given) / Total Discount Given