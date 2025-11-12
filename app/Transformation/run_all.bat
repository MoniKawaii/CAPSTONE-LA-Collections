@echo off
REM
REM Run All Transformation Scripts (Windows Batch Version)
REM This script executes all transformation scripts in the correct order
REM

echo ============================================================
echo 🚀 Starting All Transformation Scripts
echo ============================================================
echo 📅 Started: %date% %time%
echo.

REM Change to project root directory
cd /d "%~dp0"
cd ..\..

REM Counter for tracking
set TOTAL=7
set CURRENT=0
set FAILED=0

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python not found. Please install Python.
    pause
    exit /b 1
)

echo 🐍 Using Python command: python
echo.

REM Function to run a script (using subroutine)
call :run_script "app\Transformation\generate_dim_time.py"
call :run_script "app\Transformation\harmonize_dim_product.py"
call :run_script "app\Transformation\harmonize_dim_customer.py"
call :run_script "app\Transformation\harmonize_dim_order.py"
call :run_script "app\Transformation\harmonize_fact_orders.py"
call :run_script "app\Transformation\harmonize_fact_traffic.py"
call :run_script "app\Transformation\harmonize_sales_aggregate_new.py"

REM Summary
echo.
echo ============================================================
echo 📊 TRANSFORMATION SUMMARY
echo ============================================================
echo 📅 Finished: %date% %time%
set /a SUCCESSFUL=%TOTAL%-%FAILED%
echo ✅ Successful: %SUCCESSFUL%/%TOTAL%
echo ❌ Failed: %FAILED%/%TOTAL%
echo.

if %FAILED%==0 (
    echo 🎉 All transformations completed successfully!
    echo 📁 Output location: app\Transformed\
    echo.
    echo 📊 Generated files:
    echo    - dim_time.csv
    echo    - dim_product.csv
    echo    - dim_customer.csv
    echo    - dim_order.csv
    echo    - fact_orders.csv
    echo    - fact_traffic.csv
    echo    - fact_sales_aggregate.csv
    echo.
    echo 🚀 Ready for analysis and reporting!
    pause
    exit /b 0
) else (
    echo ⚠️  Some transformations failed. Please check the output above.
    echo 💡 Try running individual scripts to debug specific issues.
    echo.
    echo 🔧 Individual script commands:
    echo    python app\Transformation\generate_dim_time.py
    echo    python app\Transformation\harmonize_dim_product.py
    echo    python app\Transformation\harmonize_dim_customer.py
    echo    python app\Transformation\harmonize_dim_order.py
    echo    python app\Transformation\harmonize_fact_orders.py
    echo    python app\Transformation\harmonize_fact_traffic.py
    echo    python app\Transformation\harmonize_sales_aggregate_new.py
    pause
    exit /b 1
)

:run_script
set /a CURRENT=%CURRENT%+1
echo.
echo [%CURRENT%/%TOTAL%] Running: %~1
echo ------------------------------------------------------------

python %~1
if %errorlevel%==0 (
    echo ✅ %~1 completed successfully
) else (
    echo ❌ %~1 failed
    set /a FAILED=%FAILED%+1
    echo 🔍 Check error output above for details
)
goto :eof