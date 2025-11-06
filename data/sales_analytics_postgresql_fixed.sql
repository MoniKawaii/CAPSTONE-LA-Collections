-- ============================================================================
-- LAZADA & SHOPEE SALES ANALYTICS - PostgreSQL VERSION FOR SUPABASE
-- ============================================================================
-- This script generates comprehensive sales analytics for COMPLETED orders only
-- Run this in your Supabase SQL editor to get the same insights as the Python script
-- ============================================================================

-- Create a temporary function to format currency (optional, for better display)
CREATE OR REPLACE FUNCTION format_currency(amount NUMERIC)
RETURNS TEXT AS $$
BEGIN
    RETURN '₱ ' || TO_CHAR(amount, 'FM999,999,999.00');
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 1. EXECUTIVE SUMMARY - OVERALL KPIs
-- ============================================================================
SELECT 
    '🚀 EXECUTIVE SUMMARY - COMPLETED ORDERS ONLY' AS section,
    '' AS metric,
    '' AS value;

-- Overall Business Metrics
WITH overall_stats AS (
    SELECT 
        COUNT(DISTINCT fo.orders_key) AS total_records,
        MIN(dt.date) AS date_range_start,
        MAX(dt.date) AS date_range_end,
        SUM(do_table.price_total) AS total_gross_revenue,
        SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) AS total_discounts,
        SUM(fo.paid_price) AS total_net_sales,
        SUM(fo.item_quantity) AS total_items_sold,
        ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS avg_order_value
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
)
SELECT 
    'OVERALL METRICS' AS section,
    'Total COMPLETED Orders' AS metric,
    total_records::TEXT AS value
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Date Range' AS metric,
    date_range_start::TEXT || ' to ' || date_range_end::TEXT AS value
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Gross Revenue' AS metric,
    format_currency(total_gross_revenue) AS value
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Discounts' AS metric,
    format_currency(total_discounts) AS value
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Net Sales' AS metric,
    format_currency(total_net_sales) AS value
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Items Sold' AS metric,
    total_items_sold::TEXT AS value
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Average Order Value' AS metric,
    format_currency(avg_order_value) AS value
FROM overall_stats;

-- ============================================================================
-- 2. PLATFORM-SPECIFIC KPIs
-- ============================================================================
SELECT 
    '🏪 PLATFORM BREAKDOWN' AS section,
    '' AS metric,
    '' AS value;

WITH platform_stats AS (
    SELECT 
        CASE 
            WHEN fo.platform_key = 1 THEN 'Lazada'
            WHEN fo.platform_key = 2 THEN 'Shopee'
            ELSE 'Platform ' || fo.platform_key::TEXT
        END AS platform,
        COUNT(DISTINCT fo.orders_key) AS total_orders,
        SUM(do_table.price_total) AS gross_revenue,
        SUM(COALESCE(fo.voucher_platform_amount + fo.voucher_seller_amount, 0)) AS total_discounts,
        SUM(fo.paid_price) AS net_sales,
        SUM(fo.item_quantity) AS items_sold,
        ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS avg_order_value,
        ROUND(SUM(fo.paid_price) * 100.0 / SUM(SUM(fo.paid_price)) OVER()::NUMERIC, 1) AS revenue_percentage
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY fo.platform_key
    ORDER BY fo.platform_key
)
SELECT 
    platform || ' METRICS' AS section,
    'Orders' AS metric,
    total_orders::TEXT AS value
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Gross Revenue' AS metric,
    format_currency(gross_revenue) AS value
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Net Sales' AS metric,
    format_currency(net_sales) AS value
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Items Sold' AS metric,
    items_sold::TEXT AS value
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'AOV' AS metric,
    format_currency(avg_order_value) AS value
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Revenue Share' AS metric,
    revenue_percentage::TEXT || '%' AS value
FROM platform_stats;

-- ============================================================================
-- 3. TOP SELLING PRODUCTS BY PLATFORM
-- ============================================================================
SELECT 
    '🏆 TOP SELLING PRODUCTS' AS section,
    '' AS metric,
    '' AS value;

-- Overall Top 5 by Units
WITH overall_top_units AS (
    SELECT 
        'OVERALL' AS platform,
        dp.product_name,
        SUM(fo.item_quantity) AS total_units,
        SUM(fo.paid_price) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(fo.item_quantity) DESC) AS rank
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_product dp ON fo.product_key = dp.product_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY dp.product_name
    ORDER BY total_units DESC
    LIMIT 5
)
SELECT 
    'OVERALL TOP BY UNITS' AS section,
    LEFT(product_name, 40) AS metric,
    total_units::TEXT || ' units' AS value
FROM overall_top_units;

-- Overall Top 5 by Revenue
WITH overall_top_revenue AS (
    SELECT 
        dp.product_name,
        SUM(fo.paid_price) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(fo.paid_price) DESC) AS rank
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_product dp ON fo.product_key = dp.product_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY dp.product_name
    ORDER BY total_revenue DESC
    LIMIT 5
)
SELECT 
    'OVERALL TOP BY REVENUE' AS section,
    LEFT(product_name, 40) AS metric,
    format_currency(total_revenue) AS value
FROM overall_top_revenue;

-- Platform-specific Top Products
WITH platform_top_products AS (
    SELECT 
        CASE 
            WHEN fo.platform_key = 1 THEN 'LAZADA'
            WHEN fo.platform_key = 2 THEN 'SHOPEE'
            ELSE 'PLATFORM ' || fo.platform_key::TEXT
        END AS platform,
        dp.product_name,
        SUM(fo.item_quantity) AS total_units,
        SUM(fo.paid_price) AS total_revenue,
        ROW_NUMBER() OVER (PARTITION BY fo.platform_key ORDER BY SUM(fo.item_quantity) DESC) AS units_rank,
        ROW_NUMBER() OVER (PARTITION BY fo.platform_key ORDER BY SUM(fo.paid_price) DESC) AS revenue_rank
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_product dp ON fo.product_key = dp.product_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY fo.platform_key, dp.product_name
)
-- Top by units per platform
SELECT 
    platform || ' TOP BY UNITS' AS section,
    LEFT(product_name, 40) AS metric,
    total_units::TEXT || ' units' AS value
FROM platform_top_products
WHERE units_rank <= 5
UNION ALL
-- Top by revenue per platform
SELECT 
    platform || ' TOP BY REVENUE' AS section,
    LEFT(product_name, 40) AS metric,
    format_currency(total_revenue) AS value
FROM platform_top_products
WHERE revenue_rank <= 5
ORDER BY section, metric;

-- ============================================================================
-- 4. GEOGRAPHIC DISTRIBUTION
-- ============================================================================
SELECT 
    '🌍 GEOGRAPHIC DISTRIBUTION' AS section,
    '' AS metric,
    '' AS value;

WITH location_stats AS (
    SELECT 
        do_table.shipping_city,
        COUNT(DISTINCT fo.orders_key) AS total_orders,
        SUM(fo.paid_price) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT fo.orders_key) DESC) AS rank
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    WHERE do_table.order_status = 'COMPLETED'
      AND do_table.shipping_city IS NOT NULL 
      AND do_table.shipping_city != 'N/A'
    GROUP BY do_table.shipping_city
    ORDER BY total_orders DESC
    LIMIT 10
)
SELECT 
    'TOP CITIES BY ORDERS' AS section,
    LEFT(shipping_city, 35) AS metric,
    total_orders::TEXT || ' orders' AS value
FROM location_stats;

-- ============================================================================
-- 5. TIME SERIES ANALYSIS (CHRONOLOGICAL ORDER)
-- ============================================================================
SELECT 
    '📅 TIME SERIES ANALYSIS' AS section,
    '' AS metric,
    '' AS value;

-- Monthly trends with proper chronological ordering
WITH monthly_trends AS (
    SELECT 
        dt.year,
        dt.month_name,
        CASE dt.month_name
            WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
            WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
            WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
            WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
        END AS month_num,
        COUNT(DISTINCT fo.orders_key) AS monthly_orders,
        SUM(fo.paid_price) AS monthly_revenue,
        SUM(fo.item_quantity) AS monthly_items
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY dt.year, dt.month_name
    ORDER BY dt.year, month_num
)
SELECT 
    'MONTHLY TRENDS' AS section,
    year::TEXT || ' ' || LEFT(month_name, 12) AS metric,
    format_currency(monthly_revenue) || ' (' || monthly_orders::TEXT || ' orders)' AS value
FROM monthly_trends;

-- Platform-specific monthly trends for current year (2025)
WITH platform_monthly_2025 AS (
    SELECT 
        CASE 
            WHEN fo.platform_key = 1 THEN 'LAZADA'
            WHEN fo.platform_key = 2 THEN 'SHOPEE'
            ELSE 'PLATFORM ' || fo.platform_key::TEXT
        END AS platform,
        dt.month_name,
        CASE dt.month_name
            WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
            WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
            WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
            WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
        END AS month_num,
        COUNT(DISTINCT fo.orders_key) AS monthly_orders,
        SUM(fo.paid_price) AS monthly_revenue
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
      AND dt.year = 2025
    GROUP BY fo.platform_key, dt.month_name
    ORDER BY fo.platform_key, month_num
)
SELECT 
    platform || ' 2025 TRENDS' AS section,
    LEFT(month_name, 12) AS metric,
    format_currency(monthly_revenue) || ' (' || monthly_orders::TEXT || ' orders)' AS value
FROM platform_monthly_2025;

-- ============================================================================
-- 6. DATA QUALITY METRICS
-- ============================================================================
SELECT 
    '📊 DATA QUALITY METRICS' AS section,
    '' AS metric,
    '' AS value;

WITH data_quality AS (
    SELECT 
        COUNT(*) AS total_fact_records,
        COUNT(DISTINCT fo.orders_key) AS unique_orders,
        COUNT(CASE WHEN fo.orders_key IS NOT NULL THEN 1 END) AS orders_key_coverage,
        COUNT(CASE WHEN fo.product_key IS NOT NULL THEN 1 END) AS product_key_coverage,
        COUNT(CASE WHEN fo.customer_key IS NOT NULL THEN 1 END) AS customer_key_coverage,
        COUNT(CASE WHEN fo.time_key IS NOT NULL THEN 1 END) AS time_key_coverage,
        COUNT(CASE WHEN dp.product_name IS NOT NULL THEN 1 END) AS product_join_success,
        COUNT(CASE WHEN do_table.order_status IS NOT NULL THEN 1 END) AS order_join_success,
        COUNT(CASE WHEN dt.date IS NOT NULL THEN 1 END) AS time_join_success
    FROM la_collections.fact_orders fo
    LEFT JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    LEFT JOIN la_collections.dim_product dp ON fo.product_key = dp.product_key
    LEFT JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
)
SELECT 
    'DATA QUALITY' AS section,
    'Total Fact Records' AS metric,
    total_fact_records::TEXT AS value
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Unique Orders' AS metric,
    unique_orders::TEXT AS value
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Orders Key Coverage' AS metric,
    ROUND(orders_key_coverage * 100.0 / total_fact_records::NUMERIC, 1)::TEXT || '%' AS value
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Product Key Coverage' AS metric,
    ROUND(product_key_coverage * 100.0 / total_fact_records::NUMERIC, 1)::TEXT || '%' AS value
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Customer Key Coverage' AS metric,
    ROUND(customer_key_coverage * 100.0 / total_fact_records::NUMERIC, 1)::TEXT || '%' AS value
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Time Key Coverage' AS metric,
    ROUND(time_key_coverage * 100.0 / total_fact_records::NUMERIC, 1)::TEXT || '%' AS value
FROM data_quality;

-- ============================================================================
-- 7. EXECUTIVE INSIGHTS & RECOMMENDATIONS
-- ============================================================================
SELECT 
    '💡 EXECUTIVE INSIGHTS' AS section,
    '' AS metric,
    '' AS value;

-- Growth rate calculation (comparing current vs previous year)
WITH yearly_comparison AS (
    SELECT 
        dt.year,
        SUM(fo.paid_price) AS yearly_revenue,
        COUNT(DISTINCT fo.orders_key) AS yearly_orders
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
      AND dt.year IN (2024, 2025)
    GROUP BY dt.year
),
growth_calc AS (
    SELECT 
        (SELECT yearly_revenue FROM yearly_comparison WHERE year = 2025) AS revenue_2025,
        (SELECT yearly_revenue FROM yearly_comparison WHERE year = 2024) AS revenue_2024,
        (SELECT yearly_orders FROM yearly_comparison WHERE year = 2025) AS orders_2025,
        (SELECT yearly_orders FROM yearly_comparison WHERE year = 2024) AS orders_2024
)
SELECT 
    'GROWTH INSIGHTS' AS section,
    '2025 YoY Revenue Growth' AS metric,
    CASE 
        WHEN revenue_2024 > 0 THEN 
            ROUND(((revenue_2025 - revenue_2024) * 100.0 / revenue_2024)::NUMERIC, 1)::TEXT || '%'
        ELSE 'N/A'
    END AS value
FROM growth_calc
UNION ALL
SELECT 
    'GROWTH INSIGHTS' AS section,
    '2025 YoY Order Growth' AS metric,
    CASE 
        WHEN orders_2024 > 0 THEN 
            ROUND(((orders_2025 - orders_2024) * 100.0 / orders_2024)::NUMERIC, 1)::TEXT || '%'
        ELSE 'N/A'
    END AS value
FROM growth_calc;

-- Clean up the temporary function
DROP FUNCTION IF EXISTS format_currency(NUMERIC);

-- Final summary message
SELECT 
    '✅ ANALYTICS COMPLETED' AS section,
    'Report Generated' AS metric,
    CURRENT_TIMESTAMP::TEXT AS value;
