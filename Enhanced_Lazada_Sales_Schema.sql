-- Enhanced Lazada Sales Schema
-- This schema provides a more intuitive structure for sales data analysis
-- combining orders and order items for complete transaction visibility

-- 1. ORDERS TABLE - Main order information
CREATE TABLE "Orders" (
  "order_id" bigint PRIMARY KEY NOT NULL,
  "order_number" bigint NOT NULL,
  "order_date" timestamp NOT NULL,
  "order_status" varchar NOT NULL,
  "customer_first_name" varchar,
  "customer_last_name" varchar,
  "customer_city" varchar,
  "customer_country" varchar,
  "customer_phone" varchar,
  "payment_method" varchar,
  "shipping_address" text,
  "billing_address" text,
  "order_total_amount" decimal(10,2) NOT NULL,
  "shipping_fee" decimal(10,2),
  "voucher_total" decimal(10,2),
  "voucher_seller" decimal(10,2),
  "voucher_platform" decimal(10,2),
  "items_count" int,
  "warehouse_code" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  "platform" varchar DEFAULT 'Lazada',
  "platform_region" varchar DEFAULT 'Philippines'
);

-- 2. ORDER ITEMS TABLE - Individual product items within orders
CREATE TABLE "Order_Items" (
  "order_item_id" bigint PRIMARY KEY NOT NULL,
  "order_id" bigint NOT NULL,
  "order_number" bigint NOT NULL,
  "product_name" varchar NOT NULL,
  "product_sku" varchar,
  "shop_sku" varchar,
  "sku_id" varchar,
  "variation" varchar,
  "item_price" decimal(10,2) NOT NULL,
  "paid_price" decimal(10,2) NOT NULL,
  "quantity" int DEFAULT 1,
  "item_status" varchar NOT NULL,
  "cancellation_reason" varchar,
  "product_image_url" varchar,
  "product_detail_url" varchar,
  "shipping_type" varchar,
  "shipping_amount" decimal(10,2),
  "voucher_amount" decimal(10,2),
  "tax_amount" decimal(10,2),
  "tracking_code" varchar,
  "warehouse_code" varchar,
  "created_at" timestamp,
  "updated_at" timestamp,
  FOREIGN KEY ("order_id") REFERENCES "Orders" ("order_id")
);

-- 3. PRODUCTS TABLE - Product master data (derived from order items)
CREATE TABLE "Products" (
  "sku_id" varchar PRIMARY KEY NOT NULL,
  "product_name" varchar NOT NULL,
  "product_sku" varchar,
  "shop_sku" varchar,
  "category" varchar,
  "last_price" decimal(10,2),
  "product_image_url" varchar,
  "product_detail_url" varchar,
  "total_sales_quantity" int DEFAULT 0,
  "total_sales_amount" decimal(10,2) DEFAULT 0,
  "average_selling_price" decimal(10,2),
  "first_sold_date" timestamp,
  "last_sold_date" timestamp,
  "status" varchar DEFAULT 'Active'
);

-- 4. CUSTOMERS TABLE - Customer information
CREATE TABLE "Customers" (
  "customer_id" varchar PRIMARY KEY NOT NULL,
  "first_name" varchar,
  "last_name" varchar,
  "city" varchar,
  "country" varchar,
  "phone" varchar,
  "total_orders" int DEFAULT 0,
  "total_spent" decimal(10,2) DEFAULT 0,
  "average_order_value" decimal(10,2),
  "first_order_date" timestamp,
  "last_order_date" timestamp,
  "customer_segment" varchar DEFAULT 'Regular',
  "platform" varchar DEFAULT 'Lazada'
);

-- 5. SALES SUMMARY TABLE - Aggregated daily sales data for analytics
CREATE TABLE "Sales_Summary" (
  "summary_id" serial PRIMARY KEY,
  "date" date NOT NULL,
  "platform" varchar DEFAULT 'Lazada',
  "total_orders" int DEFAULT 0,
  "total_items_sold" int DEFAULT 0,
  "total_revenue" decimal(10,2) DEFAULT 0,
  "total_shipping" decimal(10,2) DEFAULT 0,
  "total_vouchers" decimal(10,2) DEFAULT 0,
  "average_order_value" decimal(10,2),
  "unique_customers" int DEFAULT 0,
  "canceled_orders" int DEFAULT 0,
  "canceled_revenue" decimal(10,2) DEFAULT 0,
  "ready_to_ship_orders" int DEFAULT 0,
  "shipped_orders" int DEFAULT 0,
  "delivered_orders" int DEFAULT 0,
  "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
  UNIQUE("date", "platform")
);

-- Indexes for better performance
CREATE INDEX idx_orders_date ON "Orders" ("order_date");
CREATE INDEX idx_orders_status ON "Orders" ("order_status");
CREATE INDEX idx_orders_customer ON "Orders" ("customer_first_name", "customer_city");
CREATE INDEX idx_order_items_order_id ON "Order_Items" ("order_id");
CREATE INDEX idx_order_items_sku ON "Order_Items" ("sku_id");
CREATE INDEX idx_order_items_status ON "Order_Items" ("item_status");
CREATE INDEX idx_products_name ON "Products" ("product_name");
CREATE INDEX idx_customers_name ON "Customers" ("first_name", "last_name");
CREATE INDEX idx_sales_summary_date ON "Sales_Summary" ("date");

-- Views for common analytics queries

-- Daily Sales Performance View
CREATE VIEW "Daily_Sales_Performance" AS
SELECT 
    DATE(order_date) as sales_date,
    COUNT(*) as total_orders,
    SUM(order_total_amount) as total_revenue,
    AVG(order_total_amount) as average_order_value,
    SUM(items_count) as total_items_sold,
    COUNT(DISTINCT CONCAT(customer_first_name, customer_city)) as unique_customers,
    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) as canceled_orders,
    SUM(CASE WHEN order_status = 'ready_to_ship' THEN 1 ELSE 0 END) as ready_to_ship_orders
FROM "Orders"
GROUP BY DATE(order_date)
ORDER BY sales_date DESC;

-- Product Performance View  
CREATE VIEW "Product_Performance" AS
SELECT 
    p.sku_id,
    p.product_name,
    p.product_sku,
    COUNT(oi.order_item_id) as times_ordered,
    SUM(oi.paid_price) as total_revenue,
    AVG(oi.paid_price) as average_selling_price,
    SUM(CASE WHEN oi.item_status = 'canceled' THEN 1 ELSE 0 END) as canceled_items,
    MAX(oi.created_at) as last_ordered_date
FROM "Products" p
LEFT JOIN "Order_Items" oi ON p.sku_id = oi.sku_id
GROUP BY p.sku_id, p.product_name, p.product_sku
ORDER BY total_revenue DESC;

-- Customer Analysis View
CREATE VIEW "Customer_Analysis" AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    COUNT(o.order_id) as total_orders,
    SUM(o.order_total_amount) as total_spent,
    AVG(o.order_total_amount) as average_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    CASE 
        WHEN COUNT(o.order_id) >= 5 THEN 'VIP'
        WHEN COUNT(o.order_id) >= 3 THEN 'Regular'
        ELSE 'New'
    END as customer_tier
FROM "Customers" c
LEFT JOIN "Orders" o ON c.customer_id = CONCAT(o.customer_first_name, '_', o.customer_city)
GROUP BY c.customer_id, c.first_name, c.last_name, c.city
ORDER BY total_spent DESC;