-- ============================================================================
-- LAZADA & SHOPEE SALES ANALYTICS - PostgreSQL VERSION FOR SUPABASE
-- ============================================================================
-- This script generates comprehensive sales analytics for COMPLETED orders only
-- Run this in your Supabase SQL editor to get the same insights as the Python script
-- Results are displayed as temporary tables for better visibility
-- ============================================================================

-- Create temporary tables to store all results
DROP TABLE IF EXISTS temp_analytics_results;
CREATE TEMP TABLE temp_analytics_results (
    section TEXT,
    metric TEXT,
    value TEXT,
    sort_order INTEGER
);

-- ============================================================================
-- 1. EXECUTIVE SUMMARY - OVERALL KPIs
-- ============================================================================
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '🚀 EXECUTIVE SUMMARY - COMPLETED ORDERS ONLY' AS section,
    '' AS metric,
    '' AS value,
    100 AS sort_order;

-- Overall Business Metrics
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
    total_records::TEXT AS value,
    101 AS sort_order
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Date Range' AS metric,
    date_range_start::TEXT || ' to ' || date_range_end::TEXT AS value,
    102 AS sort_order
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Gross Revenue' AS metric,
    '₱ ' || TO_CHAR(total_gross_revenue, 'FM999,999,999.00') AS value,
    103 AS sort_order
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Discounts' AS metric,
    '₱ ' || TO_CHAR(total_discounts, 'FM999,999,999.00') AS value,
    104 AS sort_order
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Net Sales' AS metric,
    '₱ ' || TO_CHAR(total_net_sales, 'FM999,999,999.00') AS value,
    105 AS sort_order
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Total Items Sold' AS metric,
    total_items_sold::TEXT AS value,
    106 AS sort_order
FROM overall_stats
UNION ALL
SELECT 
    'OVERALL METRICS' AS section,
    'Average Order Value' AS metric,
    '₱ ' || TO_CHAR(avg_order_value, 'FM999,999.00') AS value,
    107 AS sort_order
FROM overall_stats;

-- ============================================================================
-- 2. PLATFORM-SPECIFIC KPIs
-- ============================================================================
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '🏪 PLATFORM BREAKDOWN' AS section,
    '' AS metric,
    '' AS value,
    200 AS sort_order;

INSERT INTO temp_analytics_results (section, metric, value, sort_order)
WITH platform_stats AS (
    SELECT 
        CASE 
            WHEN fo.platform_key = 1 THEN 'Lazada'
            WHEN fo.platform_key = 2 THEN 'Shopee'
            ELSE 'Platform ' || fo.platform_key::TEXT
        END AS platform,
        COUNT(DISTINCT fo.orders_key) AS total_orders,
        SUM(do_table.price_total) AS gross_revenue,
        SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) AS total_discounts,
        SUM(fo.paid_price) AS net_sales,
        SUM(fo.item_quantity) AS items_sold,
        ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS avg_order_value,
        ROUND((SUM(fo.paid_price) * 100.0 / SUM(SUM(fo.paid_price)) OVER())::NUMERIC, 1) AS revenue_percentage,
        fo.platform_key
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY fo.platform_key
    ORDER BY fo.platform_key
)
SELECT 
    platform || ' METRICS' AS section,
    'Orders' AS metric,
    total_orders::TEXT AS value,
    200 + (platform_key * 10) + 1 AS sort_order
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Gross Revenue' AS metric,
    '₱ ' || TO_CHAR(gross_revenue, 'FM999,999,999.00') AS value,
    200 + (platform_key * 10) + 2 AS sort_order
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Net Sales' AS metric,
    '₱ ' || TO_CHAR(net_sales, 'FM999,999,999.00') AS value,
    200 + (platform_key * 10) + 3 AS sort_order
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Items Sold' AS metric,
    items_sold::TEXT AS value,
    200 + (platform_key * 10) + 4 AS sort_order
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'AOV' AS metric,
    '₱ ' || TO_CHAR(avg_order_value, 'FM999,999.00') AS value,
    200 + (platform_key * 10) + 5 AS sort_order
FROM platform_stats
UNION ALL
SELECT 
    platform || ' METRICS' AS section,
    'Revenue Share' AS metric,
    revenue_percentage::TEXT || '%' AS value,
    200 + (platform_key * 10) + 6 AS sort_order
FROM platform_stats;

-- ============================================================================
-- 3. TOP SELLING PRODUCTS BY PLATFORM
-- ============================================================================
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '🏆 TOP SELLING PRODUCTS' AS section,
    '' AS metric,
    '' AS value,
    300 AS sort_order;

-- Overall Top 5 by Units
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
    total_units::TEXT || ' units' AS value,
    300 + rank AS sort_order
FROM overall_top_units;

-- Overall Top 5 by Revenue
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
    '₱ ' || TO_CHAR(total_revenue, 'FM999,999,999.00') AS value,
    310 + rank AS sort_order
FROM overall_top_revenue;

-- Platform-specific Top Products
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
        ROW_NUMBER() OVER (PARTITION BY fo.platform_key ORDER BY SUM(fo.paid_price) DESC) AS revenue_rank,
        fo.platform_key
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
    total_units::TEXT || ' units' AS value,
    320 + (platform_key * 20) + units_rank AS sort_order
FROM platform_top_products
WHERE units_rank <= 5
UNION ALL
-- Top by revenue per platform
SELECT 
    platform || ' TOP BY REVENUE' AS section,
    LEFT(product_name, 40) AS metric,
    '₱ ' || TO_CHAR(total_revenue, 'FM999,999,999.00') AS value,
    350 + (platform_key * 20) + revenue_rank AS sort_order
FROM platform_top_products
WHERE revenue_rank <= 5;

-- ============================================================================
-- 4. GEOGRAPHIC DISTRIBUTION
-- ============================================================================
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '🌍 GEOGRAPHIC DISTRIBUTION' AS section,
    '' AS metric,
    '' AS value,
    400 AS sort_order;

INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
    total_orders::TEXT || ' orders' AS value,
    400 + rank AS sort_order
FROM location_stats;

-- ============================================================================
-- 5. TIME SERIES ANALYSIS (CHRONOLOGICAL ORDER)
-- ============================================================================
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '📅 TIME SERIES ANALYSIS' AS section,
    '' AS metric,
    '' AS value,
    500 AS sort_order;

-- Monthly trends with proper chronological ordering
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
    '₱ ' || TO_CHAR(monthly_revenue, 'FM999,999,999.00') || ' (' || monthly_orders::TEXT || ' orders)' AS value,
    500 + (year - 2023) * 12 + month_num AS sort_order
FROM monthly_trends;

-- Platform-specific monthly trends for current year (2025)
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
        SUM(fo.paid_price) AS monthly_revenue,
        fo.platform_key
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
    '₱ ' || TO_CHAR(monthly_revenue, 'FM999,999,999.00') || ' (' || monthly_orders::TEXT || ' orders)' AS value,
    600 + (platform_key * 20) + month_num AS sort_order
FROM platform_monthly_2025;

-- ============================================================================
-- 6. DATA QUALITY METRICS
-- ============================================================================
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '📊 DATA QUALITY METRICS' AS section,
    '' AS metric,
    '' AS value,
    700 AS sort_order;

INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
    total_fact_records::TEXT AS value,
    701 AS sort_order
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Unique Orders' AS metric,
    unique_orders::TEXT AS value,
    702 AS sort_order
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Orders Key Coverage' AS metric,
    ROUND((orders_key_coverage * 100.0 / total_fact_records)::NUMERIC, 1)::TEXT || '%' AS value,
    703 AS sort_order
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Product Key Coverage' AS metric,
    ROUND((product_key_coverage * 100.0 / total_fact_records)::NUMERIC, 1)::TEXT || '%' AS value,
    704 AS sort_order
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Customer Key Coverage' AS metric,
    ROUND((customer_key_coverage * 100.0 / total_fact_records)::NUMERIC, 1)::TEXT || '%' AS value,
    705 AS sort_order
FROM data_quality
UNION ALL
SELECT 
    'DATA QUALITY' AS section,
    'Time Key Coverage' AS metric,
    ROUND((time_key_coverage * 100.0 / total_fact_records)::NUMERIC, 1)::TEXT || '%' AS value,
    706 AS sort_order
FROM data_quality;

-- ============================================================================
-- 7. EXECUTIVE INSIGHTS & RECOMMENDATIONS
-- ============================================================================
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '💡 EXECUTIVE INSIGHTS' AS section,
    '' AS metric,
    '' AS value,
    800 AS sort_order;

-- Growth rate calculation (comparing current vs previous year)
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
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
    END AS value,
    801 AS sort_order
FROM growth_calc
UNION ALL
SELECT 
    'GROWTH INSIGHTS' AS section,
    '2025 YoY Order Growth' AS metric,
    CASE 
        WHEN orders_2024 > 0 THEN 
            ROUND(((orders_2025 - orders_2024) * 100.0 / orders_2024)::NUMERIC, 1)::TEXT || '%'
        ELSE 'N/A'
    END AS value,
    802 AS sort_order
FROM growth_calc;

-- Final summary message
INSERT INTO temp_analytics_results (section, metric, value, sort_order)
SELECT 
    '✅ ANALYTICS COMPLETED' AS section,
    'Report Generated' AS metric,
    CURRENT_TIMESTAMP::TEXT AS value,
    900 AS sort_order;

-- ============================================================================
-- DISPLAY ALL RESULTS IN ORDER
-- ============================================================================
SELECT 
    section,
    metric,
    value
FROM temp_analytics_results 
ORDER BY sort_order, section, metric;
