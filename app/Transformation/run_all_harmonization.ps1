# ============================================================
# Run All Harmonization Scripts - PowerShell Version
# This script executes all harmonization scripts in the correct order
# ============================================================

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "🚀 Starting All Harmonization Scripts" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "📅 Started: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Yellow
Write-Host ""

# Change to the Transformation directory
Set-Location -Path $PSScriptRoot

# Counter for tracking
$TOTAL = 7
$CURRENT = 0
$FAILED = 0
$SUCCEEDED = @()
$FAILED_SCRIPTS = @()

# Function to run a script
function Run-Script {
    param($ScriptName)
    
    $script:CURRENT++
    Write-Host ""
    Write-Host "[$script:CURRENT/$script:TOTAL] Running: $ScriptName" -ForegroundColor Magenta
    Write-Host "------------------------------------------------------------" -ForegroundColor Gray
    
    $startTime = Get-Date
    
    try {
        $result = python $ScriptName
        if ($LASTEXITCODE -eq 0) {
            $endTime = Get-Date
            $duration = $endTime - $startTime
            Write-Host "✅ $ScriptName completed successfully" -ForegroundColor Green
            Write-Host "⏱️  Duration: $($duration.ToString('mm\:ss'))" -ForegroundColor Gray
            $script:SUCCEEDED += $ScriptName
        } else {
            throw "Python script returned exit code $LASTEXITCODE"
        }
    }
    catch {
        $endTime = Get-Date
        $duration = $endTime - $startTime
        Write-Host "❌ $ScriptName failed" -ForegroundColor Red
        Write-Host "⏱️  Duration: $($duration.ToString('mm\:ss'))" -ForegroundColor Gray
        Write-Host "💬 Error: $($_.Exception.Message)" -ForegroundColor Red
        $script:FAILED++
        $script:FAILED_SCRIPTS += $ScriptName
    }
}

# Validate Python installation
Write-Host "🔍 Checking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✅ Python found: $pythonVersion" -ForegroundColor Green
}
catch {
    Write-Host "❌ Python not found. Please ensure Python is installed and in PATH." -ForegroundColor Red
    exit 1
}

# Check if required input files exist
Write-Host ""
Write-Host "🔍 Checking input files..." -ForegroundColor Yellow
$requiredFiles = @(
    "..\Staging\lazada_orders_raw.json",
    "..\Staging\lazada_products_raw.json", 
    "..\Staging\lazada_multiple_order_items_raw.json",
    "..\Staging\shopee_orders_raw.json",
    "..\Staging\shopee_products_raw.json"
)

$missingFiles = @()
foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        $missingFiles += $file
    }
}

if ($missingFiles.Count -gt 0) {
    Write-Host "⚠️  Warning: Missing input files:" -ForegroundColor Yellow
    foreach ($file in $missingFiles) {
        Write-Host "   - $file" -ForegroundColor Yellow
    }
    Write-Host ""
    $continue = Read-Host "Do you want to continue anyway? (y/N)"
    if ($continue -ne 'y' -and $continue -ne 'Y') {
        Write-Host "❌ Aborted by user." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "✅ All required input files found." -ForegroundColor Green
}

Write-Host ""
Write-Host "🚀 Starting harmonization process..." -ForegroundColor Green

# Run all harmonization scripts in the correct order
Run-Script "generate_dim_time.py"
Run-Script "harmonize_dim_product.py"
Run-Script "harmonize_dim_customer.py"
Run-Script "harmonize_dim_order.py"
Run-Script "harmonize_fact_orders.py"
Run-Script "harmonize_fact_traffic.py"
Run-Script "harmonize_sales_aggregate_new.py"

# Summary
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "📊 HARMONIZATION SUMMARY" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "📅 Finished: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Yellow
Write-Host "✅ Successful: $(($TOTAL - $FAILED))/$TOTAL" -ForegroundColor Green
Write-Host "❌ Failed: $FAILED/$TOTAL" -ForegroundColor Red
Write-Host ""

if ($SUCCEEDED.Count -gt 0) {
    Write-Host "✅ Successful Scripts:" -ForegroundColor Green
    foreach ($script in $SUCCEEDED) {
        Write-Host "   - $script" -ForegroundColor Green
    }
    Write-Host ""
}

if ($FAILED_SCRIPTS.Count -gt 0) {
    Write-Host "❌ Failed Scripts:" -ForegroundColor Red
    foreach ($script in $FAILED_SCRIPTS) {
        Write-Host "   - $script" -ForegroundColor Red
    }
    Write-Host ""
}

if ($FAILED -eq 0) {
    Write-Host "🎉 All harmonization scripts completed successfully!" -ForegroundColor Green
    Write-Host "📁 Output location: ..\Transformed\" -ForegroundColor Cyan
    
    # Show output files
    Write-Host ""
    Write-Host "📋 Generated Files:" -ForegroundColor Cyan
    $outputPath = "..\Transformed"
    if (Test-Path $outputPath) {
        $outputFiles = Get-ChildItem -Path $outputPath -Filter "*.csv" | Sort-Object Name
        foreach ($file in $outputFiles) {
            $size = [math]::Round($file.Length / 1MB, 2)
            Write-Host "   📄 $($file.Name) ($size MB)" -ForegroundColor White
        }
    }
    
    exit 0
} else {
    Write-Host "⚠️  Some harmonization scripts failed. Please check the output above." -ForegroundColor Yellow
    Write-Host "💡 Tip: Run individual scripts to debug specific failures." -ForegroundColor Gray
    exit 1
}