# PRICE_TOTAL VALIDATION FINDINGS - DIM_ORDER ANALYSIS

**Generated:** November 11, 2025  
**Analysis Scope:** price_total field validation for Lazada orders (platform_key = 1) in dim_order

## 🎯 CRITICAL FINDINGS

### ⚠️ **MAJOR ISSUE IDENTIFIED: Price Data Loss in Transformation**

**Price Quality Overview:**

- **Total Lazada Orders:** 9,038
- **Valid Prices:** 7,846 orders (86.8%)
- **NaN/Missing Prices:** 1,192 orders (13.2%)
- **Zero Prices:** 1 order (0.0%)

### 🔍 **ROOT CAUSE ANALYSIS**

**Key Discovery:** All orders with NaN prices in dim_order have **VALID PRICES in raw data**!

```
Price Data Flow Issue:
Raw Data → Transformation → dim_order
✅ Valid    →     ❌      →    NaN

Sample Evidence:
├── Order 549438864248073: Raw = ₱660.00 → dim_order = NaN
├── Order 553040277083957: Raw = ₱310.00 → dim_order = NaN
├── Order 553686656925244: Raw = ₱310.00 → dim_order = NaN
├── Order 553828644933619: Raw = ₱310.00 → dim_order = NaN
└── Order 554237037820426: Raw = ₱310.00 → dim_order = NaN
```

**Price Loss Pattern by Status:**

- **COMPLETED:** 593/8,439 orders (7.0%) lose prices → **93.0% price retention**
- **CANCELLED:** 477/477 orders (100%) lose prices → **0.0% price retention**
- **SHIPPED_BACK_SUCCESS:** 106/106 orders (100%) lose prices → **0.0% price retention**
- **Other Statuses:** 16/16 orders (100%) lose prices → **0.0% price retention**

### 📊 **TRANSFORMATION LOGIC ISSUE**

**Problem:** The transformation logic appears to:

1. ✅ Successfully extract prices from raw data (100% have valid prices)
2. ❌ Fail to map prices for non-COMPLETED orders during transformation
3. ❌ Fail to map prices for 7% of COMPLETED orders

**Evidence:**

- **Raw Data:** 100% of orders have valid price fields
- **dim_order:** Only 86.8% retain price values
- **fact_orders:** Only orders with valid dim_order prices are included

### 📅 **TEMPORAL PATTERN ANALYSIS**

**Price Quality Improvement Over Time:**

- **2023-2024:** 80-90% price retention rate
- **2025 onwards:** 90-96% price retention rate
- **Recent months:** Significant improvement in price mapping

**Monthly Price Retention Rates (2025):**

- 2025-01: 93.5% ✅ Good improvement
- 2025-02: 94.6% ✅ Better
- 2025-03: 95.9% ✅ Excellent
- 2025-04: 94.6% ✅ Maintained
- 2025-05: 91.2% ✅ Good
- 2025-06: 95.6% ✅ Excellent

## 🎯 **BUSINESS IMPACT**

### Financial Impact

- **Lost Revenue Tracking:** 1,192 orders with missing price data
- **Analytical Gaps:** 13.2% of orders excluded from revenue analysis
- **Historical Data:** Price issues more prevalent in older data (2022-2024)

### Data Quality Impact

- **fact_orders Accuracy:** Correctly excludes orders without valid prices
- **Analytical Integrity:** Maintained (only valid-price orders in analysis)
- **Reporting Completeness:** 86.8% coverage for revenue metrics

## 🔧 **ROOT CAUSE & SOLUTION**

### Identified Issues

1. **Status-Based Price Mapping:** Non-COMPLETED orders completely lose prices during transformation
2. **Partial Price Loss:** 7% of COMPLETED orders also lose prices
3. **Transformation Logic:** Price mapping appears to be conditional on order status
4. **No Fallback:** No price preservation for non-revenue orders

### Immediate Actions Required

1. **✅ Fix Transformation Logic:** Preserve raw prices for all orders regardless of status
2. **✅ Status-Agnostic Pricing:** Ensure price mapping works for all order statuses
3. **✅ Data Recovery:** Implement price recovery from raw data for 1,192 orders
4. **✅ Validation Rules:** Add price mapping validation in transformation pipeline

### Technical Investigation Needed

1. **Review transformation scripts** in `app/Transformation/`
2. **Check price mapping logic** for status-based filtering
3. **Validate price field mapping** from raw JSON to dimensional model
4. **Test price preservation** for all order statuses

## 💡 **RECOMMENDATIONS**

### High Priority Fixes

1. **🔧 FIX TRANSFORMATION:** Update price mapping to preserve all raw prices
2. **🔄 BACKFILL DATA:** Recover missing prices from raw data for 1,192 orders
3. **✅ VALIDATE MAPPING:** Ensure 100% price preservation from raw to dim_order

### Medium Priority Improvements

1. **📊 MONITORING:** Add price mapping validation to transformation pipeline
2. **🔍 TESTING:** Implement price preservation tests for all order statuses
3. **📋 DOCUMENTATION:** Document price mapping business rules

### Quality Assurance

1. **🎯 TARGET:** Achieve 98%+ price mapping success rate
2. **📈 TRACKING:** Monitor monthly price retention rates
3. **⚠️ ALERTS:** Set up alerts for price mapping failures

## 🎉 **POSITIVE FINDINGS**

1. **✅ Raw Data Integrity:** 100% of orders have valid prices in source data
2. **✅ Recent Improvements:** Price mapping quality improving over time (90-96% in 2025)
3. **✅ fact_orders Logic:** Correctly designed to exclude invalid price data
4. **✅ Data Flow:** Clear path from raw data through transformation identified

## 🎯 **CONCLUSION**

The price_total validation reveals a **transformation issue** rather than a data source problem:

- **Source Data:** ✅ Perfect (100% valid prices)
- **Transformation:** ❌ Needs Fix (13.2% price loss)
- **Destination:** ✅ Correct (valid prices only)

**Priority:** **HIGH** - Fix transformation logic to preserve all price data from raw sources.

**Impact:** Resolving this will increase analytical coverage from 86.8% to 100% and provide complete revenue tracking.

---

_This analysis confirms the transformation layer as the point of price data loss, with a clear path to resolution through transformation script updates._
