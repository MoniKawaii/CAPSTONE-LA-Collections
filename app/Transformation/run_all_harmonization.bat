@echo off
REM ============================================================
REM Run All Harmonization Scripts - Windows Batch Version
REM This batch file executes all harmonization scripts in order
REM ============================================================

echo ============================================================
echo 🚀 Starting All Harmonization Scripts
echo ============================================================
echo 📅 Started: %date% %time%
echo.

REM Change to the script directory
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python not found. Please ensure Python is installed and in PATH.
    pause
    exit /b 1
)

echo ✅ Python found
echo.

REM Run the Python version of the script
echo 🚀 Executing Python harmonization runner...
echo.
python run_all_harmonization.py

REM Check the result
if errorlevel 1 (
    echo.
    echo ❌ Harmonization process failed
    echo Press any key to exit...
    pause >nul
    exit /b 1
) else (
    echo.
    echo ✅ Harmonization process completed
    echo Press any key to exit...
    pause >nul
    exit /b 0
)