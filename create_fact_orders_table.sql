-- =========================================================================
-- PostgreSQL CREATE TABLE Script for FACT_ORDERS
-- Generated: November 5, 2025
-- Source: LA Collections Capstone Project
-- Description: Fact table storing individual order line items with pricing,
--              voucher discounts, and foreign key relationships to dimensions
-- =========================================================================

CREATE TABLE fact_orders (
    -- Primary Key
    order_item_key VARCHAR(20) PRIMARY KEY,
    
    -- Foreign Keys to Dimension Tables
    orders_key DECIMAL(10,1) NOT NULL,           -- References dim_order.orders_key
    product_key DECIMAL(10,1) NOT NULL,          -- References dim_product.product_key  
    product_variant_key DECIMAL(10,1) NOT NULL,  -- References dim_product_variant.product_variant_key
    time_key INTEGER NOT NULL,                   -- References dim_time.time_key (YYYYMMDD format)
    customer_key DECIMAL(10,1) NOT NULL,         -- References dim_customer.customer_key
    platform_key INTEGER NOT NULL,               -- References dim_platform.platform_key (1=Lazada, 2=Shopee)
    
    -- Order Item Metrics
    item_quantity INTEGER NOT NULL DEFAULT 1,
    
    -- Pricing Information (in Philippine Peso)
    paid_price DECIMAL(12,2) NOT NULL,                    -- Final amount customer paid
    original_unit_price DECIMAL(12,2) NOT NULL,           -- Original price before discounts
    voucher_platform_amount DECIMAL(12,2) DEFAULT 0.00,   -- Platform voucher discount amount
    voucher_seller_amount DECIMAL(12,2) DEFAULT 0.00,     -- Seller voucher discount amount
    shipping_fee_paid_by_buyer DECIMAL(12,2) DEFAULT 0.00, -- Shipping fee charged to customer
    
    -- Constraints
    CONSTRAINT chk_platform_key CHECK (platform_key IN (1, 2)),
    CONSTRAINT chk_item_quantity CHECK (item_quantity > 0),
    CONSTRAINT chk_paid_price CHECK (paid_price >= 0),
    CONSTRAINT chk_original_unit_price CHECK (original_unit_price >= 0),
    CONSTRAINT chk_voucher_platform_amount CHECK (voucher_platform_amount >= 0),
    CONSTRAINT chk_voucher_seller_amount CHECK (voucher_seller_amount >= 0),
    CONSTRAINT chk_shipping_fee CHECK (shipping_fee_paid_by_buyer >= 0),
    
    -- Business Rule: Paid price should not exceed original price (after accounting for possible fees)
    CONSTRAINT chk_discount_logic CHECK (
        paid_price <= original_unit_price + shipping_fee_paid_by_buyer
    )
);

-- =========================================================================
-- INDEXES FOR PERFORMANCE
-- =========================================================================

-- Primary access patterns for analytics queries
CREATE INDEX idx_fact_orders_platform_time ON fact_orders(platform_key, time_key);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_key);
CREATE INDEX idx_fact_orders_product ON fact_orders(product_key);
CREATE INDEX idx_fact_orders_product_variant ON fact_orders(product_variant_key);
CREATE INDEX idx_fact_orders_orders ON fact_orders(orders_key);

-- Composite index for revenue analysis by platform and time
CREATE INDEX idx_fact_orders_revenue_analysis ON fact_orders(platform_key, time_key, paid_price);

-- Index for discount analysis
CREATE INDEX idx_fact_orders_discounts ON fact_orders(voucher_platform_amount, voucher_seller_amount) 
WHERE voucher_platform_amount > 0 OR voucher_seller_amount > 0;

-- =========================================================================
-- COMMENTS FOR DOCUMENTATION
-- =========================================================================

COMMENT ON TABLE fact_orders IS 'Fact table storing individual order line items with pricing, discounts, and foreign key relationships to dimension tables. Each record represents one product variant within an order.';

COMMENT ON COLUMN fact_orders.order_item_key IS 'Unique identifier for each order line item (format: LO/SO + 8-digit number)';
COMMENT ON COLUMN fact_orders.orders_key IS 'Foreign key to dim_order table (decimal with .1 for Lazada, .2 for Shopee)';
COMMENT ON COLUMN fact_orders.product_key IS 'Foreign key to dim_product table';
COMMENT ON COLUMN fact_orders.product_variant_key IS 'Foreign key to dim_product_variant table (includes DEFAULT variants)';
COMMENT ON COLUMN fact_orders.time_key IS 'Foreign key to dim_time table in YYYYMMDD format';
COMMENT ON COLUMN fact_orders.customer_key IS 'Foreign key to dim_customer table';
COMMENT ON COLUMN fact_orders.platform_key IS 'Foreign key to dim_platform table (1=Lazada, 2=Shopee)';
COMMENT ON COLUMN fact_orders.item_quantity IS 'Quantity of this product variant ordered';
COMMENT ON COLUMN fact_orders.paid_price IS 'Final amount customer paid after all discounts (PHP)';
COMMENT ON COLUMN fact_orders.original_unit_price IS 'Original price before any discounts (PHP)';
COMMENT ON COLUMN fact_orders.voucher_platform_amount IS 'Platform voucher discount amount (PHP)';
COMMENT ON COLUMN fact_orders.voucher_seller_amount IS 'Seller voucher discount amount (PHP)';
COMMENT ON COLUMN fact_orders.shipping_fee_paid_by_buyer IS 'Shipping fee charged to customer (PHP)';

-- =========================================================================
-- SAMPLE USAGE QUERIES
-- =========================================================================

/*
-- Total revenue by platform
SELECT 
    platform_key,
    COUNT(*) as total_orders,
    SUM(paid_price) as total_revenue,
    SUM(voucher_platform_amount + voucher_seller_amount) as total_discounts
FROM fact_orders 
GROUP BY platform_key;

-- Monthly revenue trend
SELECT 
    SUBSTRING(time_key::text, 1, 6) as year_month,
    platform_key,
    SUM(paid_price) as monthly_revenue
FROM fact_orders 
GROUP BY SUBSTRING(time_key::text, 1, 6), platform_key
ORDER BY year_month, platform_key;

-- Top products by revenue
SELECT 
    fo.product_key,
    dp.product_name,
    COUNT(*) as order_count,
    SUM(fo.paid_price) as total_revenue
FROM fact_orders fo
JOIN dim_product dp ON fo.product_key = dp.product_key
GROUP BY fo.product_key, dp.product_name
ORDER BY total_revenue DESC
LIMIT 10;
*/

-- =========================================================================
-- END OF SCRIPT
-- =========================================================================