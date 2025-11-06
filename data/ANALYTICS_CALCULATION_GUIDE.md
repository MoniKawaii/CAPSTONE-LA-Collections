# Sales Analytics Calculation Guide

## Complete Documentation of All Metrics and Formulas

---

## 📊 EXECUTIVE SUMMARY CALCULATIONS

### Overview Metrics

#### **Total Orders**

```sql
COUNT(DISTINCT fo.orders_key)
```

- **What it counts**: Unique order identifiers from fact_orders table
- **Filter applied**: Only COMPLETED orders (WHERE do_table.order_status = 'COMPLETED')
- **Purpose**: Shows total number of successfully completed transactions

#### **Date Range**

```sql
MIN(dt.date) AS date_range_start,
MAX(dt.date) AS date_range_end
```

- **What it shows**: Earliest and latest order dates in the dataset
- **Display format**: "YYYY-MM-DD to YYYY-MM-DD"
- **Purpose**: Defines the time period covered by the analysis

### Financial Metrics

#### **Gross Revenue**

```sql
SUM(do_table.price_total)
```

- **Source**: dim_order.price_total column
- **What it represents**: Total order value before any discounts
- **Currency**: Philippine Peso (₱)
- **Purpose**: Shows total business volume before promotional impacts

#### **Total Discounts**

```sql
SUM(COALESCE(fo.voucher_platform_amount, 0) + COALESCE(fo.voucher_seller_amount, 0))
```

- **Components**:
  - `voucher_platform_amount`: Discounts provided by the platform (Lazada/Shopee)
  - `voucher_seller_amount`: Discounts provided by the seller
- **COALESCE function**: Treats NULL values as 0 to prevent calculation errors
- **Purpose**: Total promotional value given to customers

#### **Net Sales**

```sql
SUM(fo.paid_price)
```

- **Source**: fact_orders.paid_price column
- **What it represents**: Actual revenue received after all discounts
- **Relationship**: Gross Revenue - Total Discounts = Net Sales
- **Purpose**: Shows actual business income

#### **Average Order Value (AOV)**

```sql
ROUND(AVG(do_table.price_total)::NUMERIC, 2)
```

- **Calculation**: Average of all order totals (before discounts)
- **PostgreSQL casting**: `::NUMERIC` ensures proper decimal handling
- **Rounding**: 2 decimal places for currency precision
- **Purpose**: Measures typical customer spending per transaction

#### **Discount Rate**

```sql
ROUND((SUM(Total_Discounts) * 100.0 / NULLIF(SUM(Gross_Revenue), 0))::NUMERIC, 2)
```

- **Formula**: (Total Discounts ÷ Gross Revenue) × 100
- **NULLIF function**: Prevents division by zero errors
- **Result**: Percentage of gross revenue given as discounts
- **Purpose**: Measures promotional intensity

### Operational Metrics

#### **Total Items Sold**

```sql
SUM(fo.item_quantity)
```

- **Source**: fact_orders.item_quantity column
- **What it counts**: Physical units/items sold across all orders
- **Purpose**: Measures business volume in terms of product movement

---

## 🏪 PLATFORM BREAKDOWN CALCULATIONS

### **Platform Classification**

```sql
CASE
    WHEN fo.platform_key = 1 THEN 'Lazada'
    WHEN fo.platform_key = 2 THEN 'Shopee'
    ELSE 'Platform ' || fo.platform_key::TEXT
END
```

- **Mapping**: Converts numeric keys to readable platform names
- **Flexibility**: Handles additional platforms if platform_key > 2

### **Revenue Share Percentage**

```sql
ROUND((SUM(fo.paid_price) * 100.0 / SUM(SUM(fo.paid_price)) OVER())::NUMERIC, 1)
```

- **Window function**: `SUM(SUM(fo.paid_price)) OVER()` calculates total across all platforms
- **Formula**: (Platform Net Sales ÷ Total Net Sales) × 100
- **Purpose**: Shows each platform's contribution to total business

### **Platform-Specific Metrics**

All financial metrics (Gross Revenue, Net Sales, etc.) use the same formulas as Executive Summary but with `GROUP BY fo.platform_key` to separate by platform.

---

## 🔍 DETAILED PLATFORM ANALYSIS CALCULATIONS

### **Date Range per Platform**

```sql
MIN(dt.date) AS platform_date_start,
MAX(dt.date) AS platform_date_end
```

- **Scope**: Earliest and latest order dates for each specific platform
- **Purpose**: Shows operational period for each platform individually

### **Average Items per Order**

```sql
ROUND(AVG(fo.item_quantity)::NUMERIC, 1)
```

- **Calculation**: Average number of items across all orders for that platform
- **Rounding**: 1 decimal place for readability
- **Purpose**: Measures typical order size per platform

### **Revenue per Order**

```sql
ROUND((SUM(fo.paid_price) / COUNT(DISTINCT fo.orders_key))::NUMERIC, 2)
```

- **Formula**: Total Net Sales ÷ Number of Orders
- **Alternative calculation**: Could use AVG(fo.paid_price) but this method ensures accuracy
- **Purpose**: Shows average revenue generated per transaction

### **Platform vs Seller Vouchers**

- **Platform Vouchers**: `SUM(COALESCE(fo.voucher_platform_amount, 0))`
  - Discounts funded by Lazada/Shopee
  - Marketing/customer acquisition costs for platforms
- **Seller Vouchers**: `SUM(COALESCE(fo.voucher_seller_amount, 0))`
  - Discounts funded by individual sellers
  - Seller's promotional investment

---

## 📅 MONTHLY TRENDS CALCULATIONS

### **Year-Month Format**

```sql
dt.year || '-' || LPAD(EXTRACT(MONTH FROM dt.date)::TEXT, 2, '0')
```

- **LPAD function**: Ensures 2-digit month format (e.g., "01" not "1")
- **Result format**: "YYYY-MM" (e.g., "2024-11")
- **Purpose**: Standardized chronological sorting

### **Monthly Aggregations**

All monthly metrics use standard aggregation functions (`COUNT`, `SUM`, `AVG`) but with:

```sql
GROUP BY dt.year, dt.month_name, EXTRACT(MONTH FROM dt.date)
```

### **Row Numbering for Sorting**

```sql
ROW_NUMBER() OVER (ORDER BY dt.year DESC, EXTRACT(MONTH FROM dt.date) DESC)
```

- **Sorting**: Most recent months first
- **Purpose**: Consistent ordering across all monthly data

---

## 🏪 MONTHLY TRENDS BY PLATFORM

### **Platform-Specific Monthly Aggregation**

```sql
GROUP BY fo.platform_key, dt.year, dt.month_name, EXTRACT(MONTH FROM dt.date)
```

- **Multiple grouping**: Separates data by both platform and month
- **Result**: Individual monthly performance for each platform

### **Platform Row Numbering**

```sql
ROW_NUMBER() OVER (PARTITION BY fo.platform_key ORDER BY dt.year DESC, EXTRACT(MONTH FROM dt.date) DESC)
```

- **PARTITION BY**: Creates separate numbering sequences for each platform
- **Purpose**: Maintains chronological order within each platform's data

### **Monthly AOV per Platform**

```sql
ROUND(AVG(do_table.price_total)::NUMERIC, 2) AS monthly_avg_order_value
```

- **Scope**: Average order value for that specific platform in that specific month
- **Purpose**: Track AOV trends over time for each platform

---

## 🔧 TECHNICAL IMPLEMENTATION DETAILS

### **PostgreSQL Compatibility**

- **NUMERIC Casting**: `::NUMERIC` ensures proper decimal arithmetic
- **ROUND Function**: All ROUND functions use NUMERIC casting to prevent PostgreSQL errors
- **COALESCE**: Handles NULL values to prevent calculation failures

### **JOIN Strategy**

```sql
FROM la_collections.fact_orders fo
JOIN la_collections.dim_order do_table ON fo.orders_key = do_table.orders_key
JOIN la_collections.dim_time dt ON fo.time_key = dt.time_key
```

- **Star Schema**: Fact table joined to dimension tables via foreign keys
- **Performance**: Optimized for analytical queries

### **Data Quality Filters**

- **Status Filter**: `WHERE do_table.order_status = 'COMPLETED'`
- **NULL Handling**: COALESCE functions prevent NULL-related errors
- **NULLIF**: Prevents division by zero in percentage calculations

---

## 📋 COLUMN MAPPINGS

### **Discount Columns**

- **Database Column**: `voucher_platform_amount` + `voucher_seller_amount`
- **Business Meaning**: Total promotional value
- **Note**: These columns replaced earlier `discount_amount` column in schema updates

### **Key Relationships**

- **orders_key**: Links fact_orders to dim_order
- **time_key**: Links fact_orders to dim_time
- **platform_key**: 1 = Lazada, 2 = Shopee
- **customer_key**: Links to dim_customers (not used in current analytics)
- **product_key**: Links to dim_products (not used in current analytics)

---

## 🎯 BUSINESS INTERPRETATION

### **What Each Metric Tells You**

1. **Gross Revenue**: Market demand and pricing effectiveness
2. **Net Sales**: Actual business income after promotions
3. **Discount Rate**: Promotional intensity and margin impact
4. **AOV**: Customer spending behavior and upselling success
5. **Revenue per Order**: Platform efficiency in monetizing transactions
6. **Items per Order**: Customer buying patterns and cross-selling success
7. **Platform Share**: Channel performance and diversification
8. **Monthly Trends**: Seasonality, growth patterns, and business cycles

### **Key Performance Indicators (KPIs)**

- **Growth**: Month-over-month revenue trends
- **Efficiency**: AOV and revenue per order improvements
- **Channel Balance**: Platform revenue distribution
- **Promotion Effectiveness**: Discount rate vs. volume correlation
- **Customer Behavior**: Items per order and spending patterns

---

_This document explains all calculations used in the Sales Analytics report. All formulas are PostgreSQL-compatible and designed for the la_collections schema structure._
