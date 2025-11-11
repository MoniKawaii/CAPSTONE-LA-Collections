# Tests and Validation Suite

This folder contains all validation scripts and tests for the LA Collections data harmonization pipeline.

## Quick Start

### Before Running Harmonization

```bash
cd tests
python quick_readiness_check.py
```

### After Running Harmonization

```bash
cd tests
python post_harmonization_validation.py
```

### Run All Tests

```bash
cd tests
python master_validation.py run
```

## Available Tests

### 🚀 Essential Tests (Run These)

| Script                             | Purpose                         | When to Use                      |
| ---------------------------------- | ------------------------------- | -------------------------------- |
| `quick_readiness_check.py`         | Check if harmonization is ready | **Before** running harmonization |
| `post_harmonization_validation.py` | Validate harmonization success  | **After** running harmonization  |
| `master_validation.py`             | Central test coordinator        | **Anytime** for overview         |

### 🔧 Diagnostic Tests

| Script                            | Purpose                          | When to Use            |
| --------------------------------- | -------------------------------- | ---------------------- |
| `validate_dim_order_fix.py`       | Check price mapping fix status   | After price fixes      |
| `validate_price_fix_impact.py`    | Analyze business impact of fixes | After major data fixes |
| `test_harmonization_integrity.py` | Comprehensive system test        | When troubleshooting   |

### 🔍 Analysis Scripts

| Script                                   | Purpose                       | When to Use                       |
| ---------------------------------------- | ----------------------------- | --------------------------------- |
| `analyze_harmonization_discrepancies.py` | Find data discrepancies       | When data doesn't match           |
| `check_price_validity.py`                | Validate price data patterns  | When price issues suspected       |
| `analyze_missing_completed_orders.py`    | Find missing completed orders | When fact_orders seems incomplete |

### 🆘 Emergency Fixes

| Script                        | Purpose                  | When to Use                                |
| ----------------------------- | ------------------------ | ------------------------------------------ |
| `fix_lazada_price_mapping.py` | Fix price mapping issues | **EMERGENCY ONLY** when prices are missing |

## Test Results Interpretation

### ✅ Success Indicators

- **100% price completeness** in dim_order
- **≥95% fact_orders coverage** of completed orders
- **All dimension files** present and populated
- **No missing critical mappings** in configuration

### ❌ Failure Indicators

- **<95% price completeness** in dim_order
- **Missing dimension files** (customer, product, order)
- **Import errors** in configuration
- **Large discrepancies** between raw and transformed data

## Common Issues and Solutions

### Issue: Price Mapping Failures

**Symptoms**: Missing price_total values in dim_order
**Solution**: Run `fix_lazada_price_mapping.py`
**Prevention**: Enhanced price mapping is now built into harmonization

### Issue: Missing Completed Orders in Fact Orders

**Symptoms**: fact_orders count < completed orders in dim_order
**Solution**: Check that all completed orders have valid prices
**Test**: Use `analyze_missing_completed_orders.py`

### Issue: Configuration Import Errors

**Symptoms**: "Import config could not be resolved"
**Solution**: Ensure you're running from the correct directory
**Fix**: Always run tests from the `tests/` directory

### Issue: Raw Data Not Found

**Symptoms**: "File not found" errors for JSON files
**Solution**: Ensure raw data files are in app/Staging/
**Files needed**:

- `lazada_orders_raw.json`
- `lazada_multiple_order_items_raw.json`
- `shopee_orders_raw.json`
- `shopee_paymentdetail_raw.json`
- `shopee_paymentdetail_2_raw.json`

## Data Quality Thresholds

### Price Completeness

- **Excellent**: ≥98% price completeness
- **Good**: 95-98% price completeness
- **Needs Work**: <95% price completeness

### Fact Orders Coverage

- **Excellent**: ≥98% of completed orders in fact_orders
- **Good**: 95-98% coverage
- **Needs Work**: <95% coverage

### Overall System Health

- **Healthy**: All critical tests pass, ≥98% data quality
- **Warning**: 1-2 minor test failures, 95-98% data quality
- **Critical**: Major test failures, <95% data quality

## Workflow Integration

### Standard Development Workflow

1. **Before Changes**: `python quick_readiness_check.py`
2. **Make Changes**: Modify harmonization files
3. **Run Harmonization**: Execute harmonize\_\*.py scripts
4. **Validate Results**: `python post_harmonization_validation.py`
5. **Deploy**: Only if all tests pass

### Emergency Fix Workflow

1. **Identify Issue**: Use diagnostic tests
2. **Apply Fix**: Run appropriate fix script
3. **Validate Fix**: Run all validation tests
4. **Document**: Update this README with lessons learned

## Maintenance

### Weekly

- Run `master_validation.py run` to check system health
- Review any failing tests and address root causes

### Monthly

- Update test thresholds based on data growth
- Review and clean up old backup files
- Document any new edge cases discovered

### Before Major Changes

- Run full test suite and document baseline
- Create additional backups of critical files
- Plan rollback strategy if tests fail

## Support

If tests are failing and you're not sure why:

1. **Check the basics**: Raw data files, directory structure, permissions
2. **Run diagnostics**: Use `test_harmonization_integrity.py` for detailed analysis
3. **Review logs**: Check harmonization script output for errors
4. **Emergency mode**: If critical, use emergency fix scripts with caution
5. **Documentation**: Update this README with new findings

---

_Last updated: November 11, 2024_  
_Maintained by: Data Engineering Team_
