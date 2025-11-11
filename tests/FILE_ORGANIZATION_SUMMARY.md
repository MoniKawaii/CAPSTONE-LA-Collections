# Test Files Organization Summary

## Files Moved to tests/ Folder

### From Root Directory:

- `test_config.py` → `tests/test_config.py`
- `test_upload.py` → `tests/test_upload.py`
- `test_lazada_sku_config.py` → `tests/test_lazada_sku_config.py`
- `test_enhanced_extraction.py` → `tests/test_enhanced_extraction.py`
- `check_tokens.py` → `tests/check_tokens.py`

### From app/Transformation/ Directory:

- `test_payment_details.py` → `tests/test_payment_details.py`
- `check_voucher_fields.py` → `tests/check_voucher_fields.py`
- `check_payment_structure.py` → `tests/check_payment_structure.py`

### Already in tests/ Directory:

- `analyze_fallback_usage.py`
- `analyze_missing_lazada.py`
- `analyze_sku_mapping.py`
- `check_fallback_voucher_fields.py`
- `check_voucher_amounts.py`
- `investigate_missing_skus.py`
- `lazada_test.py`
- `shopee_test.py`
- `test_etl.py`
- `test_fact_orders_integrity.py`
- `test_fact_orders_realistic.py`
- `test_route.py`
- `FACT_ORDERS_TEST_REPORT.md`
- `SKU_ALIGNMENT_CONFIGURATION_ANALYSIS.md`

## Total Files in tests/ Folder: 20 Python files + 2 Markdown reports

All test and check scripts are now organized in the tests/ directory for better project structure and easier maintenance.
