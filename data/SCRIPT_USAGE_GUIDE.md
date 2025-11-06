# Sales Analytics Scripts Usage Guide

## Overview

This folder contains three versions of sales analytics:

1. **descriptive_summary.py** - Python script for local analysis
2. **sales_analytics_simple.sql** - Core metrics for Supabase (PostgreSQL)
3. **sales_analytics_postgresql.sql** - Comprehensive analytics for Supabase (PostgreSQL)

## Database Schema Requirements

All SQL scripts assume the following schema structure in Supabase:

- Schema: `la_collections`
- Tables: `fact_orders`, `dim_time`, `dim_orders`, `dim_products`, `dim_customers`

## Column Mappings

The scripts use these discount column mappings:

- `voucher_platform_amount` - Platform discounts
- `voucher_seller_amount` - Seller discounts
- Total discount = `voucher_platform_amount + voucher_seller_amount`

## Usage Instructions

### Python Script (descriptive_summary.py)

```bash
cd "c:\Users\alyss\Desktop\CAPSTONE-LA-Collections"
python data/descriptive_summary.py
```

- Processes CSV files from `app/Transformed/`
- Filters for COMPLETED orders only
- Separates analysis by platform_key
- Outputs results to timestamped text file

### Simple SQL Script (sales_analytics_simple.sql)

Execute in Supabase SQL Editor for core business metrics:

- Total records and date range
- Gross revenue, discounts, and net sales
- Average order value
- Total items sold
- Platform breakdown

### Comprehensive SQL Script (sales_analytics_postgresql.sql)

Execute in Supabase SQL Editor for detailed analytics:

- Executive Summary
- Platform Performance Breakdown
- Top Products Analysis
- Geographic Analysis
- Time Series Analysis
- Data Quality Metrics

## PostgreSQL Compatibility Notes

All ROUND functions use `::NUMERIC` casting for PostgreSQL compatibility:

```sql
-- Correct format
ROUND(AVG(price_total)::NUMERIC, 2)

-- Instead of (causes error in PostgreSQL)
ROUND(AVG(price_total), 2)
```

## Filter Settings

All scripts are configured to:

- Process only COMPLETED orders (`order_status = 'COMPLETED'`)
- Separate analysis by platform (Lazada vs Shopee)
- Sort time series chronologically

## Output Files

- Python script: `sales_analytics_report_YYYYMMDD_HHMMSS.txt`
- SQL scripts: Display results in Supabase query results panel

## Last Updated

Scripts updated with PostgreSQL compatibility fixes and schema alignment.
