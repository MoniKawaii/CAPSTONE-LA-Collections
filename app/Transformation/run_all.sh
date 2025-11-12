#!/bin/bash
#
# Run All Transformation Scripts
# This script executes all transformation scripts in the correct order
# Works on both Windows and Unix systems
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

# Detect Python command (works on Windows and Unix)
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    echo "❌ Python not found. Please install Python."
    exit 1
fi

echo "🐍 Using Python command: $PYTHON_CMD"
echo ""

# Function to run a script
run_script() {
    CURRENT=$((CURRENT + 1))
    echo ""
    echo "[$CURRENT/$TOTAL] Running: $1"
    echo "------------------------------------------------------------"
    
    # Change to project root directory for proper imports
    cd ../.. || exit 1
    
    if $PYTHON_CMD "app/Transformation/$1"; then
        echo "✅ $1 completed successfully"
    else
        echo "❌ $1 failed"
        FAILED=$((FAILED + 1))
        echo "🔍 Check error output above for details"
    fi
    
    # Return to Transformation directory
    cd app/Transformation || exit 1
}

# Run all transformation scripts in order
echo "🔄 Starting transformation pipeline..."
echo ""

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
    echo ""
    echo "📊 Generated files:"
    echo "   - dim_time.csv"
    echo "   - dim_product.csv" 
    echo "   - dim_customer.csv"
    echo "   - dim_order.csv"
    echo "   - fact_orders.csv"
    echo "   - fact_traffic.csv"
    echo "   - fact_sales_aggregate.csv"
    echo ""
    echo "🚀 Ready for analysis and reporting!"
    exit 0
else
    echo "⚠️  Some transformations failed. Please check the output above."
    echo "💡 Try running individual scripts to debug specific issues."
    echo ""
    echo "🔧 Individual script commands:"
    echo "   python app/Transformation/generate_dim_time.py"
    echo "   python app/Transformation/harmonize_dim_product.py"
    echo "   python app/Transformation/harmonize_dim_customer.py"
    echo "   python app/Transformation/harmonize_dim_order.py"
    echo "   python app/Transformation/harmonize_fact_orders.py"
    echo "   python app/Transformation/harmonize_fact_traffic.py"
    echo "   python app/Transformation/harmonize_sales_aggregate_new.py"
    exit 1
fi
