# Enhanced Lazada Sales Analytics - Dimensional Model Implementation

## 🎯 **Project Overview**

This project implements a comprehensive dimensional data warehouse model for Lazada sales analytics, featuring proper price calculations, voucher tracking, and time-based analysis capabilities.

## 📊 **Dimensional Model Architecture**

### **Fact Tables (Measures & Transactions)**

1. **`Fact_Sales`** - Central analytical table

   - Additive measures: quantity_sold, gross_sales_amount, net_sales_amount
   - Semi-additive: unit_price, unit_cost
   - Flags: is_cancelled, is_returned, is_voucher_used

2. **`Orders`** - Order transactions with proper price calculations
3. **`Order_Items`** - Individual item sales details
4. **`Sales_Summary`** - Pre-aggregated daily metrics for performance

### **Dimension Tables (Master Data)**

1. **`Dim_Time`** - Complete time hierarchy for OLAP cubes

   - Daily granularity from 2024-2025 (731 records)
   - Includes: year, quarter, month, week, day attributes
   - Season, weekend, holiday flags
   - Fiscal year support

2. **`Dim_Customers`** - Customer master with segmentation

   - Customer lifecycle metrics
   - Segmentation: New, Regular, VIP, Inactive
   - Lifetime value calculations

3. **`Dim_Products`** - Product catalog with performance metrics

   - SKU management with variations (color, size)
   - Inventory status tracking
   - Performance metrics integration

4. **`Dim_Vouchers`** - Promotion management
   - Voucher types and discount structures
   - Date ranges and usage limits
   - Target audience tracking

### **Voucher Management Tables**

1. **`Voucher_Products`** - Product-specific voucher mappings
2. **`Voucher_Usage`** - Effectiveness tracking and analysis

## 💰 **Price Calculation Implementation**

### **Lazada Price Formula**

```sql
-- Buyer Paid Price = price - voucher + shipping_fee
buyer_paid_price = order_total_price - voucher_total + shipping_fee

-- Net Revenue Calculation
net_revenue = SUM(buyer_paid_price)

-- Voucher Effectiveness
effectiveness = (Total_Order_Value_Generated - Total_Discount_Given) / Total_Discount_Given
```

### **Price Fields in Schema**

- `order_total_price` - Original price (excluding discounts)
- `voucher_seller` - Seller voucher discount
- `voucher_platform` - Platform voucher discount
- `voucher_total` - Combined voucher discounts
- `shipping_fee` - Shipping charges
- `buyer_paid_price` - **Final amount paid by customer**

## 🎫 **Voucher Analytics Capabilities**

### **Voucher Tracking Features**

- **Time-based Analysis**: Track voucher usage patterns over time
- **Effectiveness Metrics**: ROI calculation for each voucher campaign
- **Customer Acquisition**: New customer acquisition through vouchers
- **Product Mapping**: Which products are included in voucher campaigns
- **Usage Patterns**: Peak usage times and customer segments

### **Voucher APIs Integration**

- `/promotion/vouchers/get` - Master voucher data
- `/promotion/voucher/products/get` - Product-specific voucher rules

## 📈 **Analytical Views & Reports**

### **Pre-built Analytical Views**

1. **`v_product_performance`** - Product sales analysis
2. **`v_voucher_effectiveness`** - Voucher ROI and effectiveness
3. **`v_customer_segments`** - Customer behavior analysis
4. **`v_sales_trends`** - Time-based sales patterns

### **Key Performance Metrics**

- **Sales Metrics**: Gross revenue, net revenue, discount rates
- **Customer Metrics**: AOV, CLV, segmentation, retention
- **Product Metrics**: Best sellers, margin analysis, inventory turnover
- **Voucher Metrics**: Usage rates, effectiveness, customer acquisition cost

## 🔍 **Current Data Sample**

### **Extracted Data Summary**

- **📅 Time Dimension**: 731 records (2024-2025)
- **👥 Customers**: 2 active customers
- **🛍️ Products**: 12 unique products
- **📈 Sales Facts**: 32 transaction records
- **💰 Total Revenue**: ₱63,175.00

### **Customer Insights**

- **Segment Distribution**: Regular customers with high AOV
- **Average Order Value**: ₱2,537.00
- **Customer Lifetime Value**: ₱12,685.00 per customer

### **Product Portfolio**

- Women's Long Pants (various colors/sizes)
- Fluffy Comfortable Slippers
- Sweet Night Perfume Body Mist
- Born Pretty Gel Nail Polish
- Test items for validation

## 🚀 **Implementation Benefits**

### **Business Intelligence Capabilities**

1. **OLAP Cube Analysis** - Multi-dimensional data slicing
2. **Time Series Analysis** - Trend identification and forecasting
3. **Customer Segmentation** - Targeted marketing campaigns
4. **Voucher Optimization** - ROI-driven promotion strategies
5. **Inventory Management** - Performance-based stock decisions

### **Scalability Features**

- **Dimensional Modeling** - Efficient query performance
- **Star Schema Design** - Optimized for analytics
- **Indexing Strategy** - Fast query execution
- **Aggregation Tables** - Dashboard performance optimization

## 📋 **Next Steps & Recommendations**

### **Phase 1: Enhanced Data Collection**

1. Implement voucher APIs integration
2. Add product category enrichment
3. Collect cost data for margin analysis
4. Integrate customer demographics

### **Phase 2: Advanced Analytics**

1. Build OLAP cubes for multi-dimensional analysis
2. Implement predictive analytics for demand forecasting
3. Create customer lifetime value models
4. Develop voucher optimization algorithms

### **Phase 3: Automation & Monitoring**

1. Automated ETL pipelines
2. Real-time dashboard updates
3. Alert systems for key metrics
4. Performance monitoring and optimization

## 🛠 **Technical Implementation**

### **Files Created**

1. `Enhanced_Lazada_Sales_Schema_Dimensional.sql` - Complete database schema
2. `lazada_dimensional_etl.py` - ETL extraction and transformation
3. `lazada_dimensional_data.json` - Sample extracted data

### **Key Features**

- **Proper Price Calculations** - Implements Lazada's buyer_paid_price formula
- **Voucher Integration** - Tracks promotion effectiveness over time
- **Time Dimension** - Enables time-based OLAP analysis
- **Customer Segmentation** - Automated customer lifecycle management
- **Product Performance** - Sales analytics with inventory insights

## 📊 **Schema Diagram Overview**

```
Dim_Time ────┐
             │
Dim_Customers ─── Fact_Sales ─── Dim_Products
             │         │
Dim_Vouchers ─────────┘
             │
        Voucher_Usage
             │
     Voucher_Products
```

This dimensional model provides a robust foundation for comprehensive sales analytics, enabling data-driven decision making for LA Collections' Lazada operations.
