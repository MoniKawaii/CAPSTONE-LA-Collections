-- ============================================================================
-- SALES ANALYTICS - SINGLE OUTPUT FOR SUPABASE
-- ============================================================================
-- This version combines all metrics into one result set for better visibility
-- in Supabase SQL editor
-- ============================================================================

WITH 
-- Core business metrics
core_metrics AS (
    SELECT 
        COUNT(DISTINCT fo.orders_key) AS total_records,
        MIN(dt.date) AS date_range_start,
        MAX(dt.date) AS date_range_end,
        SUM(do_table.price_total) AS total_gross_revenue,
        SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) AS total_discounts,
        SUM(fo.paid_price) AS total_net_sales,
        SUM(fo.item_quantity) AS total_items_sold,
        ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS avg_order_value,
        ROUND((SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) * 100.0 / 
              NULLIF(SUM(do_table.price_total), 0))::NUMERIC, 2) AS discount_rate_percentage
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
),

-- Platform breakdown
platform_metrics AS (
    SELECT 
        CASE 
            WHEN fo.platform_key = 1 THEN 'Lazada'
            WHEN fo.platform_key = 2 THEN 'Shopee'
            ELSE 'Platform ' || fo.platform_key::TEXT
        END AS platform,
        fo.platform_key,
        COUNT(DISTINCT fo.orders_key) AS platform_orders,
        SUM(do_table.price_total) AS platform_gross_revenue,
        SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) AS platform_discounts,
        SUM(fo.paid_price) AS platform_net_sales,
        SUM(fo.item_quantity) AS platform_items_sold,
        ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS platform_avg_order_value,
        ROUND((SUM(fo.paid_price) * 100.0 / 
              SUM(SUM(fo.paid_price)) OVER())::NUMERIC, 1) AS revenue_share_percent,
        ROUND((SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) * 100.0 / 
              NULLIF(SUM(do_table.price_total), 0))::NUMERIC, 2) AS platform_discount_rate
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY fo.platform_key
    ORDER BY fo.platform_key
),

-- Individual platform detailed metrics
platform_detailed AS (
    SELECT 
        CASE 
            WHEN fo.platform_key = 1 THEN 'Lazada'
            WHEN fo.platform_key = 2 THEN 'Shopee'
            ELSE 'Platform ' || fo.platform_key::TEXT
        END AS platform,
        fo.platform_key,
        
        -- Date range for this platform
        MIN(dt.date) AS platform_date_start,
        MAX(dt.date) AS platform_date_end,
        
        -- Financial metrics
        COUNT(DISTINCT fo.orders_key) AS total_orders,
        SUM(do_table.price_total) AS gross_revenue,
        SUM(COALESCE(fo.voucher_platform_amount, 0)) AS platform_vouchers,
        SUM(COALESCE(fo.voucher_seller_amount, 0)) AS seller_vouchers,
        SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) AS total_discounts,
        SUM(fo.paid_price) AS net_sales,
        
        -- Operational metrics
        SUM(fo.item_quantity) AS items_sold,
        ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS avg_order_value,
        ROUND(AVG(fo.item_quantity)::NUMERIC, 1) AS avg_items_per_order,
        
        -- Performance metrics
        ROUND((SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) * 100.0 / 
              NULLIF(SUM(do_table.price_total), 0))::NUMERIC, 2) AS discount_rate,
        ROUND((SUM(fo.paid_price) / COUNT(DISTINCT fo.orders_key))::NUMERIC, 2) AS revenue_per_order
        
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY fo.platform_key
),

-- Monthly trends (all months for readability)
monthly_summary AS (
    SELECT 
        dt.year || '-' || LPAD(EXTRACT(MONTH FROM dt.date)::TEXT, 2, '0') AS year_month,
        dt.month_name,
        COUNT(DISTINCT fo.orders_key) AS monthly_orders,
        SUM(fo.paid_price) AS monthly_net_sales,
        SUM(fo.item_quantity) AS monthly_items_sold,
        ROW_NUMBER() OVER (ORDER BY dt.year DESC, EXTRACT(MONTH FROM dt.date) DESC) AS rn
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY dt.year, dt.month_name, EXTRACT(MONTH FROM dt.date)
),

-- Monthly trends by platform
monthly_platform_summary AS (
    SELECT 
        CASE 
            WHEN fo.platform_key = 1 THEN 'Lazada'
            WHEN fo.platform_key = 2 THEN 'Shopee'
            ELSE 'Platform ' || fo.platform_key::TEXT
        END AS platform,
        fo.platform_key,
        dt.year || '-' || LPAD(EXTRACT(MONTH FROM dt.date)::TEXT, 2, '0') AS year_month,
        dt.month_name,
        COUNT(DISTINCT fo.orders_key) AS monthly_orders,
        SUM(do_table.price_total) AS monthly_gross_revenue,
        SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0)) AS monthly_discounts,
        SUM(fo.paid_price) AS monthly_net_sales,
        SUM(fo.item_quantity) AS monthly_items_sold,
        ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS monthly_avg_order_value,
        ROW_NUMBER() OVER (PARTITION BY fo.platform_key ORDER BY dt.year DESC, EXTRACT(MONTH FROM dt.date) DESC) AS platform_rn
    FROM la_collections.fact_orders fo
    JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
    JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
    WHERE do_table.order_status = 'COMPLETED'
    GROUP BY fo.platform_key, dt.year, dt.month_name, EXTRACT(MONTH FROM dt.date)
)

-- Final combined output
SELECT 
    section,
    category,
    metric,
    value,
    additional_info
FROM (
    SELECT 
        '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)' AS section,
        'Overview' AS category,
        'Total Orders' AS metric,
        TO_CHAR(total_records, 'FM999,999') AS value,
        NULL AS additional_info,
        1 AS section_order,
        1 AS category_order,
        1 AS metric_order
    FROM core_metrics

    UNION ALL
    SELECT '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)', 'Overview', 'Date Range',
        date_range_start::TEXT || ' to ' || date_range_end::TEXT, NULL, 1, 1, 2
    FROM core_metrics

    UNION ALL
    SELECT '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)', 'Financial', 'Gross Revenue',
        '₱ ' || TO_CHAR(total_gross_revenue, 'FM999,999,999.00'), NULL, 1, 2, 1
    FROM core_metrics

    UNION ALL
    SELECT '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)', 'Financial', 'Total Discounts',
        '₱ ' || TO_CHAR(total_discounts, 'FM999,999,999.00'), NULL, 1, 2, 2
    FROM core_metrics

    UNION ALL
    SELECT '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)', 'Financial', 'Net Sales',
        '₱ ' || TO_CHAR(total_net_sales, 'FM999,999,999.00'), NULL, 1, 2, 3
    FROM core_metrics

    UNION ALL
    SELECT '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)', 'Financial', 'Average Order Value',
        '₱ ' || TO_CHAR(avg_order_value, 'FM999,999.00'), NULL, 1, 2, 4
    FROM core_metrics

    UNION ALL
    SELECT '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)', 'Financial', 'Discount Rate',
        discount_rate_percentage::TEXT || '%', NULL, 1, 2, 5
    FROM core_metrics

    UNION ALL
    SELECT '🔸 EXECUTIVE SUMMARY (COMPLETED ORDERS)', 'Operational', 'Total Items Sold',
        TO_CHAR(total_items_sold, 'FM999,999') || ' units', NULL, 1, 3, 1
    FROM core_metrics

    -- Platform breakdown section
    UNION ALL
    SELECT 
        '🔸 PLATFORM BREAKDOWN', 
        platform, 
        'Orders',
        TO_CHAR(platform_orders, 'FM999,999'), 
        revenue_share_percent::TEXT || '% of total revenue',
        2,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        1
    FROM platform_metrics

    UNION ALL
    SELECT 
        '🔸 PLATFORM BREAKDOWN', 
        platform, 
        'Net Sales',
        '₱ ' || TO_CHAR(platform_net_sales, 'FM999,999,999.00'), 
        'Avg Order: ₱' || TO_CHAR(platform_avg_order_value, 'FM999,999.00'),
        2,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        2
    FROM platform_metrics

    UNION ALL
    SELECT 
        '🔸 PLATFORM BREAKDOWN', 
        platform, 
        'Items Sold',
        TO_CHAR(platform_items_sold, 'FM999,999') || ' units', 
        NULL,
        2,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        3
    FROM platform_metrics

    -- Individual Platform Detailed Analysis
    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Date Range',
        platform_date_start::TEXT || ' to ' || platform_date_end::TEXT, 
        NULL,
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        1
    FROM platform_detailed

    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Total Orders',
        TO_CHAR(total_orders, 'FM999,999'), 
        'Avg Items/Order: ' || avg_items_per_order::TEXT,
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        2
    FROM platform_detailed

    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Gross Revenue',
        '₱ ' || TO_CHAR(gross_revenue, 'FM999,999,999.00'), 
        'Revenue/Order: ₱' || TO_CHAR(revenue_per_order, 'FM999,999.00'),
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        3
    FROM platform_detailed

    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Platform Vouchers',
        '₱ ' || TO_CHAR(platform_vouchers, 'FM999,999,999.00'), 
        NULL,
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        4
    FROM platform_detailed

    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Seller Vouchers',
        '₱ ' || TO_CHAR(seller_vouchers, 'FM999,999,999.00'), 
        NULL,
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        5
    FROM platform_detailed

    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Total Discounts',
        '₱ ' || TO_CHAR(total_discounts, 'FM999,999,999.00'), 
        'Discount Rate: ' || discount_rate::TEXT || '%',
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        6
    FROM platform_detailed

    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Net Sales',
        '₱ ' || TO_CHAR(net_sales, 'FM999,999,999.00'), 
        'Avg Order Value: ₱' || TO_CHAR(avg_order_value, 'FM999,999.00'),
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        7
    FROM platform_detailed

    UNION ALL
    SELECT 
        '🔸 DETAILED PLATFORM ANALYSIS', 
        platform, 
        'Items Sold',
        TO_CHAR(items_sold, 'FM999,999') || ' units', 
        NULL,
        3,
        CASE platform WHEN 'Lazada' THEN 1 WHEN 'Shopee' THEN 2 ELSE 3 END,
        8
    FROM platform_detailed

    -- Monthly trends section (all months)
    UNION ALL
    SELECT 
        '🔸 MONTHLY TRENDS (ALL MONTHS)', 
        year_month, 
        month_name,
        '₱ ' || TO_CHAR(monthly_net_sales, 'FM999,999,999.00'), 
        TO_CHAR(monthly_orders, 'FM999,999') || ' orders, ' || TO_CHAR(monthly_items_sold, 'FM999,999') || ' items',
        4,
        rn,
        1
    FROM monthly_summary

    -- Monthly trends by platform section
    UNION ALL
    SELECT 
        '🔸 MONTHLY TRENDS BY PLATFORM', 
        platform || ' - ' || year_month, 
        month_name,
        '₱ ' || TO_CHAR(monthly_net_sales, 'FM999,999,999.00'), 
        TO_CHAR(monthly_orders, 'FM999,999') || ' orders, ' || 
        TO_CHAR(monthly_items_sold, 'FM999,999') || ' items, ' ||
        'AOV: ₱' || TO_CHAR(monthly_avg_order_value, 'FM999,999.00'),
        5,
        CASE platform WHEN 'Lazada' THEN platform_rn WHEN 'Shopee' THEN platform_rn + 1000 ELSE platform_rn + 2000 END,
        1
    FROM monthly_platform_summary

    -- Add completion timestamp
    UNION ALL
    SELECT 
        '🔸 REPORT STATUS', 
        'Completed', 
        'Generated At',
        CURRENT_TIMESTAMP::TEXT, 
        'All metrics based on COMPLETED orders only',
        6,
        1,
        1
) AS combined_results
ORDER BY section_order, category_order, metric_order;