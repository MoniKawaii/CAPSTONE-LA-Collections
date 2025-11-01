#!/bin/bash
#
# Run All Transformation Scripts
# This script executes all transformation scripts in the correct order
#

echo "============================================================"
echo "🚀 Starting All Transformation Scripts"
echo "============================================================"
echo "📅 Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Change to the Transformation directory
cd "$(dirname "$0")" || exit 1

# Counter for tracking
TOTAL=7
CURRENT=0
FAILED=0

# Function to run a script
run_script() {
    CURRENT=$((CURRENT + 1))
    echo ""
    echo "[$CURRENT/$TOTAL] Running: $1"
    echo "------------------------------------------------------------"
    
    if python3 "$1"; then
        echo "✅ $1 completed successfully"
    else
        echo "❌ $1 failed"
        FAILED=$((FAILED + 1))
    fi
}

# Run all transformation scripts in order
run_script "generate_dim_time.py"
run_script "harmonize_dim_product.py"
run_script "harmonize_dim_customer.py"
run_script "harmonize_dim_order.py"
run_script "harmonize_fact_orders.py"
run_script "harmonize_fact_traffic.py"
run_script "harmonize_sales_aggregate_new.py"

# Summary
echo ""
echo "============================================================"
echo "📊 TRANSFORMATION SUMMARY"
echo "============================================================"
echo "📅 Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "✅ Successful: $((TOTAL - FAILED))/$TOTAL"
echo "❌ Failed: $FAILED/$TOTAL"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "🎉 All transformations completed successfully!"
    echo "📁 Output location: ../Transformed/"
    exit 0
else
    echo "⚠️  Some transformations failed. Please check the output above."
    exit 1
fi
