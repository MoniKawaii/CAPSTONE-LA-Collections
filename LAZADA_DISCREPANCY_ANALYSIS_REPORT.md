# 🔍 Lazada Data Discrepancy Analysis Report

## Executive Summary

I've conducted a comprehensive analysis of the Lazada data discrepancies between the raw data, dimensional model, and comparison file. Here are the key findings and root causes:

## 🎯 Key Findings

### Data Distribution

- **Total Dimensional Orders**: 33,928 (9,038 Lazada + 24,890 Shopee)
- **Lazada Orders**: 9,038 total
  - ✅ COMPLETED: 8,439 (93.4%)
  - ❌ CANCELLED: 477 (5.3%)
  - 📦 OTHER STATUS: 122 (1.3%)

### Platform Identification

- **Platform Key 1**: Lazada (15.1 avg ID length, 100% numeric IDs)
- **Platform Key 2**: Shopee (14.0 avg ID length, 0% numeric IDs)

### Revenue Data Location

- ❌ `dim_order.price_total`: All zero for Lazada orders
- ✅ `fact_orders.paid_price`: Contains actual pricing data (avg ₱330.97 per item)

## 🔍 Root Cause Analysis

### 1. **Order Status Filtering**

The primary discrepancy source is **order status filtering**:

```
Monthly Comparison (DB vs Dimensional Model):
Month        Comp DB    All Orders  Completed   Match Rate
2023-10      320        372         352         90.9%
2024-02      197        219         219         100.0%
2025-01      215        230         215         100.0%
2025-07      247        266         247         100.0%
2025-08      222        242         222         100.0%
2025-09      194        205         194         100.0%
2025-10      151        167         151         100.0%
```

**Finding**: Filtering to COMPLETED orders only improved accuracy from 84% to 94%!

### 2. **Data Architecture Issues**

#### Revenue Calculation Problem

- `dim_order.price_total` = ₱0.00 for all Lazada orders
- Actual revenue data stored in `fact_orders.paid_price`
- Need to calculate total revenue by summing fact_orders by order

#### Correct Revenue Calculation

```sql
-- Correct way to calculate Lazada revenue
SELECT
    o.order_date,
    COUNT(DISTINCT o.orders_key) as order_count,
    SUM(f.paid_price) as total_revenue
FROM dim_order o
JOIN fact_orders f ON o.orders_key = f.orders_key
WHERE o.platform_key = 1
  AND o.order_status = 'COMPLETED'
GROUP BY o.order_date
```

### 3. **Harmonization Process Gaps**

#### Missing Price Rollup

The harmonization process is **not rolling up item-level prices to order-level totals**:

- `fact_orders` has individual item prices (₱330.97 avg)
- `dim_order.price_total` should sum all items per order
- Current process leaves `price_total` as zero

#### Status Handling Inconsistency

- Dimensional model includes ALL order statuses
- Comparison file likely excludes CANCELLED orders
- 477 cancelled orders (5.3%) cause count discrepancies

## 📊 Detailed Discrepancy Breakdown

### Order Count Discrepancies

```
Total Difference Analysis:
- Original DB-to-Dim difference: -172 orders
- After filtering to COMPLETED: -54 orders
- Improvement: 118 orders (69% reduction in discrepancy)
```

### Revenue Discrepancies

```
Comparison File Issues:
- DB shows proper revenue amounts
- CSV shows massive inflated values (₱858M total!)
- Likely currency conversion or aggregation error in CSV source
```

## 🔧 Recommended Fixes

### 1. **Immediate Actions**

#### Fix Revenue Calculation in dim_order

```python
# Update dim_order.price_total from fact_orders
UPDATE dim_order
SET price_total = (
    SELECT SUM(paid_price)
    FROM fact_orders
    WHERE fact_orders.orders_key = dim_order.orders_key
)
WHERE platform_key = 1;
```

#### Update Dashboard Queries

```sql
-- Use this for accurate Lazada revenue analysis
SELECT
    DATE_TRUNC('month', o.order_date) as month,
    COUNT(DISTINCT o.orders_key) as orders,
    SUM(f.paid_price) as revenue
FROM dim_order o
JOIN fact_orders f ON o.orders_key = f.orders_key
WHERE o.platform_key = 1
  AND o.order_status = 'COMPLETED'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month;
```

### 2. **Harmonization Process Improvements**

#### Fix Order Total Calculation

```python
# In harmonize_dim_order.py
def calculate_order_totals():
    """Calculate and update order totals from fact_orders"""
    query = """
    UPDATE dim_order
    SET price_total = subquery.total_price
    FROM (
        SELECT orders_key, SUM(paid_price) as total_price
        FROM fact_orders
        GROUP BY orders_key
    ) AS subquery
    WHERE dim_order.orders_key = subquery.orders_key
    """
```

#### Add Status Filtering Options

```python
def get_lazada_orders(include_cancelled=False):
    """Get Lazada orders with optional status filtering"""
    status_filter = "WHERE order_status = 'COMPLETED'" if not include_cancelled else ""
    # ... rest of query
```

### 3. **Data Quality Validation**

#### Add Revenue Validation

```python
def validate_revenue_consistency():
    """Ensure dim_order totals match fact_orders sums"""
    # Compare dim_order.price_total vs SUM(fact_orders.paid_price)
    # Flag discrepancies for manual review
```

#### Monthly Reconciliation Process

```python
def monthly_reconciliation_report():
    """Generate monthly comparison with multiple data sources"""
    # Compare: Raw API data vs Dimensional model vs External reports
    # Highlight discrepancies by category (status, dates, amounts)
```

## 🎯 Expected Impact of Fixes

### Order Count Accuracy

- **Current**: 84% match rate with comparison file
- **After status filtering**: 94% match rate
- **Target**: >98% match rate with proper date alignment

### Revenue Accuracy

- **Current**: ₱0 revenue in dimensional model
- **After fix**: ~₱3M total Lazada revenue (₱330.97 × 9,038 orders)
- **Validation**: Monthly totals will match comparison DB figures

### Dashboard Reliability

- **Current**: Misleading zero revenue for Lazada
- **After fix**: Accurate revenue trends and KPIs
- **Benefit**: Proper platform comparison and business insights

## ✅ Next Steps

1. **Execute revenue calculation fix** (Priority: HIGH)
2. **Update harmonization pipeline** to include order total rollups
3. **Implement status filtering** in reporting queries
4. **Re-generate dashboard CSV files** with corrected data
5. **Establish monthly reconciliation** process for ongoing accuracy

## 🔍 Technical Details

### Platform Key Mapping

- **Platform 1**: Lazada (numeric order IDs)
- **Platform 2**: Shopee (alphanumeric order IDs)

### Data Relationships

```
dim_order (9,038 Lazada orders)
    ↓ 1:many
fact_orders (11,178 Lazada items)
```

### Revenue Formula

```
Order Revenue = SUM(fact_orders.paid_price)
WHERE fact_orders.orders_key = dim_order.orders_key
```

This analysis explains why your comparison file shows discrepancies - the dimensional model needs revenue rollups and proper status filtering to match external reporting systems.
