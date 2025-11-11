-- ================================================================
-- DASHBOARD SQL QUERIES: Order Date Analysis
-- For LA Collections E-commerce Data
-- ================================================================

-- ================================================================
-- 1. OVERVIEW KPI QUERIES
-- ================================================================

-- Total Orders and Unique Dates by Platform
SELECT 
    CASE WHEN platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    COUNT(*) as total_orders,
    COUNT(DISTINCT order_date) as unique_order_dates,
    MIN(order_date) as first_order_date,
    MAX(order_date) as latest_order_date,
    ROUND(
        COUNT(DISTINCT order_date)::numeric / 
        (MAX(order_date) - MIN(order_date) + 1) * 100, 
        2
    ) as date_coverage_percentage
FROM dim_order 
WHERE order_status = 'COMPLETED'
GROUP BY platform_key
ORDER BY platform_key;

-- Current Streak (Consecutive Days with Orders)
WITH daily_orders AS (
    SELECT 
        platform_key,
        order_date,
        COUNT(*) as orders_count
    FROM dim_order 
    WHERE order_status = 'COMPLETED'
        AND order_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY platform_key, order_date
),
date_series AS (
    SELECT 
        platform_key,
        generate_series(
            CURRENT_DATE - INTERVAL '90 days',
            CURRENT_DATE,
            '1 day'::interval
        )::date as calendar_date
    FROM (SELECT DISTINCT platform_key FROM dim_order) p
),
daily_status AS (
    SELECT 
        ds.platform_key,
        ds.calendar_date,
        CASE WHEN do.orders_count IS NOT NULL THEN 1 ELSE 0 END as has_orders
    FROM date_series ds
    LEFT JOIN daily_orders do ON ds.platform_key = do.platform_key 
        AND ds.calendar_date = do.order_date
)
SELECT 
    CASE WHEN platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    COUNT(*) as current_consecutive_days
FROM daily_status 
WHERE has_orders = 1 
    AND calendar_date > (
        SELECT MAX(calendar_date) 
        FROM daily_status ds2 
        WHERE ds2.platform_key = daily_status.platform_key 
            AND has_orders = 0 
            AND calendar_date <= CURRENT_DATE
    )
    AND calendar_date <= CURRENT_DATE
GROUP BY platform_key;

-- ================================================================
-- 2. DAILY TRENDS FOR TIME SERIES CHARTS
-- ================================================================

-- Daily Order Counts with Missing Date Indicators
WITH date_ranges AS (
    SELECT 
        platform_key,
        MIN(order_date) as start_date,
        MAX(order_date) as end_date
    FROM dim_order 
    WHERE order_status = 'COMPLETED'
    GROUP BY platform_key
),
all_calendar_dates AS (
    SELECT 
        dr.platform_key,
        generate_series(dr.start_date, dr.end_date, '1 day'::interval)::date as calendar_date
    FROM date_ranges dr
),
daily_aggregates AS (
    SELECT 
        platform_key,
        order_date,
        COUNT(*) as daily_orders,
        COUNT(DISTINCT customer_key) as unique_customers,
        SUM(price_total) as daily_revenue
    FROM dim_order 
    WHERE order_status = 'COMPLETED'
    GROUP BY platform_key, order_date
)
SELECT 
    acd.platform_key,
    CASE WHEN acd.platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    acd.calendar_date as order_date,
    COALESCE(da.daily_orders, 0) as daily_orders,
    COALESCE(da.unique_customers, 0) as unique_customers,
    COALESCE(da.daily_revenue, 0) as daily_revenue,
    CASE WHEN da.daily_orders IS NULL THEN 1 ELSE 0 END as is_missing_date,
    EXTRACT(DOW FROM acd.calendar_date) as day_of_week,
    EXTRACT(MONTH FROM acd.calendar_date) as month,
    EXTRACT(YEAR FROM acd.calendar_date) as year
FROM all_calendar_dates acd
LEFT JOIN daily_aggregates da ON acd.platform_key = da.platform_key 
    AND acd.calendar_date = da.order_date
ORDER BY acd.platform_key, acd.calendar_date;

-- ================================================================
-- 3. MONTHLY/WEEKLY AGGREGATIONS
-- ================================================================

-- Monthly Summary with Coverage Metrics
SELECT 
    DATE_TRUNC('month', order_date) as month_year,
    CASE WHEN platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    COUNT(*) as total_orders,
    COUNT(DISTINCT order_date) as unique_dates,
    SUM(price_total) as total_revenue,
    ROUND(AVG(price_total), 2) as avg_order_value,
    EXTRACT(days FROM (DATE_TRUNC('month', order_date) + INTERVAL '1 month' - INTERVAL '1 day')) as days_in_month,
    ROUND(
        COUNT(DISTINCT order_date)::numeric / 
        EXTRACT(days FROM (DATE_TRUNC('month', order_date) + INTERVAL '1 month' - INTERVAL '1 day')) * 100,
        1
    ) as date_coverage_pct
FROM dim_order 
WHERE order_status = 'COMPLETED'
GROUP BY DATE_TRUNC('month', order_date), platform_key
ORDER BY month_year, platform_key;

-- Weekly Day-of-Week Pattern Analysis
SELECT 
    CASE WHEN platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    EXTRACT(DOW FROM order_date) as day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM order_date) = 0 THEN 'Sunday'
        WHEN EXTRACT(DOW FROM order_date) = 1 THEN 'Monday'
        WHEN EXTRACT(DOW FROM order_date) = 2 THEN 'Tuesday'
        WHEN EXTRACT(DOW FROM order_date) = 3 THEN 'Wednesday'
        WHEN EXTRACT(DOW FROM order_date) = 4 THEN 'Thursday'
        WHEN EXTRACT(DOW FROM order_date) = 5 THEN 'Friday'
        WHEN EXTRACT(DOW FROM order_date) = 6 THEN 'Saturday'
    END as day_name,
    COUNT(*) as total_orders,
    COUNT(DISTINCT order_date) as unique_dates,
    ROUND(AVG(price_total), 2) as avg_order_value,
    MIN(order_date) as first_occurrence,
    MAX(order_date) as last_occurrence
FROM dim_order 
WHERE order_status = 'COMPLETED'
GROUP BY platform_key, EXTRACT(DOW FROM order_date)
ORDER BY platform_key, day_of_week;

-- ================================================================
-- 4. MISSING DATES ANALYSIS
-- ================================================================

-- Top Missing Date Periods (for Gantt Chart)
WITH date_ranges AS (
    SELECT 
        platform_key,
        MIN(order_date) as start_date,
        MAX(order_date) as end_date
    FROM dim_order 
    WHERE order_status = 'COMPLETED'
    GROUP BY platform_key
),
all_dates AS (
    SELECT 
        dr.platform_key,
        generate_series(dr.start_date, dr.end_date, '1 day'::interval)::date as calendar_date
    FROM date_ranges dr
),
actual_order_dates AS (
    SELECT DISTINCT platform_key, order_date
    FROM dim_order 
    WHERE order_status = 'COMPLETED'
),
missing_dates AS (
    SELECT 
        ad.platform_key,
        ad.calendar_date
    FROM all_dates ad
    LEFT JOIN actual_order_dates aod ON ad.platform_key = aod.platform_key 
        AND ad.calendar_date = aod.order_date
    WHERE aod.order_date IS NULL
),
grouped_periods AS (
    SELECT 
        platform_key,
        calendar_date,
        calendar_date - ROW_NUMBER() OVER (PARTITION BY platform_key ORDER BY calendar_date)::int as group_date
    FROM missing_dates
),
consecutive_periods AS (
    SELECT 
        platform_key,
        MIN(calendar_date) as period_start,
        MAX(calendar_date) as period_end,
        COUNT(*) as days_missing
    FROM grouped_periods
    GROUP BY platform_key, group_date
)
SELECT 
    CASE WHEN platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    period_start,
    period_end,
    days_missing,
    CASE 
        WHEN days_missing = 1 THEN 'Single Day'
        WHEN days_missing <= 7 THEN 'Week or Less'
        WHEN days_missing <= 30 THEN 'Month or Less'
        ELSE 'More than Month'
    END as gap_category
FROM consecutive_periods
WHERE days_missing >= 1
ORDER BY days_missing DESC, platform_key
LIMIT 20;

-- ================================================================
-- 5. CALENDAR HEATMAP DATA
-- ================================================================

-- Calendar Heatmap Data (Day of Month vs Month)
SELECT 
    CASE WHEN platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    EXTRACT(YEAR FROM order_date) as year,
    EXTRACT(MONTH FROM order_date) as month,
    EXTRACT(DAY FROM order_date) as day_of_month,
    COUNT(*) as order_count,
    SUM(price_total) as daily_revenue,
    COUNT(DISTINCT customer_key) as unique_customers
FROM dim_order 
WHERE order_status = 'COMPLETED'
GROUP BY platform_key, EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date), EXTRACT(DAY FROM order_date)
ORDER BY platform_key, year, month, day_of_month;

-- ================================================================
-- 6. REAL-TIME MONITORING QUERIES
-- ================================================================

-- Recent Activity (Last 30 Days)
SELECT 
    CASE WHEN platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    order_date,
    COUNT(*) as daily_orders,
    SUM(price_total) as daily_revenue,
    COUNT(DISTINCT customer_key) as unique_customers,
    CASE 
        WHEN order_date = CURRENT_DATE THEN 'Today'
        WHEN order_date = CURRENT_DATE - 1 THEN 'Yesterday'
        WHEN order_date >= CURRENT_DATE - 7 THEN 'This Week'
        ELSE 'Earlier'
    END as time_category
FROM dim_order 
WHERE order_status = 'COMPLETED'
    AND order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY platform_key, order_date
ORDER BY order_date DESC, platform_key;

-- Alert Conditions (Days Without Orders)
WITH recent_dates AS (
    SELECT 
        platform_key,
        generate_series(
            CURRENT_DATE - INTERVAL '14 days',
            CURRENT_DATE,
            '1 day'::interval
        )::date as check_date
    FROM (SELECT DISTINCT platform_key FROM dim_order) p
),
order_activity AS (
    SELECT DISTINCT 
        platform_key,
        order_date
    FROM dim_order 
    WHERE order_status = 'COMPLETED'
        AND order_date >= CURRENT_DATE - INTERVAL '14 days'
)
SELECT 
    CASE WHEN rd.platform_key = 1 THEN 'Lazada' ELSE 'Shopee' END as platform_name,
    rd.check_date,
    CASE WHEN oa.order_date IS NULL THEN 'NO ORDERS' ELSE 'ORDERS PRESENT' END as status,
    CASE WHEN oa.order_date IS NULL THEN 1 ELSE 0 END as alert_flag
FROM recent_dates rd
LEFT JOIN order_activity oa ON rd.platform_key = oa.platform_key 
    AND rd.check_date = oa.order_date
WHERE rd.check_date <= CURRENT_DATE
ORDER BY rd.platform_key, rd.check_date DESC;

-- ================================================================
-- 7. COMPARATIVE ANALYSIS
-- ================================================================

-- Platform Comparison Summary
WITH platform_stats AS (
    SELECT 
        platform_key,
        COUNT(*) as total_orders,
        COUNT(DISTINCT order_date) as unique_dates,
        MIN(order_date) as start_date,
        MAX(order_date) as end_date,
        SUM(price_total) as total_revenue,
        AVG(price_total) as avg_order_value
    FROM dim_order 
    WHERE order_status = 'COMPLETED'
    GROUP BY platform_key
)
SELECT 
    'Lazada' as metric_category,
    COALESCE((SELECT total_orders FROM platform_stats WHERE platform_key = 1), 0) as lazada_value,
    COALESCE((SELECT total_orders FROM platform_stats WHERE platform_key = 2), 0) as shopee_value,
    'Total Orders' as metric_name
UNION ALL
SELECT 
    'Date Coverage',
    COALESCE((SELECT unique_dates FROM platform_stats WHERE platform_key = 1), 0),
    COALESCE((SELECT unique_dates FROM platform_stats WHERE platform_key = 2), 0),
    'Unique Order Dates'
UNION ALL
SELECT 
    'Revenue',
    COALESCE((SELECT ROUND(total_revenue) FROM platform_stats WHERE platform_key = 1), 0),
    COALESCE((SELECT ROUND(total_revenue) FROM platform_stats WHERE platform_key = 2), 0),
    'Total Revenue'
UNION ALL
SELECT 
    'Efficiency',
    COALESCE((SELECT ROUND(avg_order_value, 2) FROM platform_stats WHERE platform_key = 1), 0),
    COALESCE((SELECT ROUND(avg_order_value, 2) FROM platform_stats WHERE platform_key = 2), 0),
    'Average Order Value';