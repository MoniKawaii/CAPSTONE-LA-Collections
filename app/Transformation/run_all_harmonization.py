#!/usr/bin/env python3
"""
Run All Harmonization Scripts - Python Version
This script executes all harmonization scripts in the correct order
"""

import os
import sys
import subprocess
import time
from datetime import datetime
import json

def print_banner(text, char="=", color=None):
    """Print a banner with text"""
    banner = char * 60
    print(f"\n{banner}")
    print(f"{text}")
    print(f"{banner}")

def print_step(current, total, script_name):
    """Print current step"""
    print(f"\n[{current}/{total}] Running: {script_name}")
    print("-" * 60)

def run_script(script_name):
    """Run a single harmonization script"""
    start_time = time.time()
    try:
        # Run the script
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, 
                              text=True, 
                              check=True)
        
        end_time = time.time()
        duration = end_time - start_time
        print(f"✅ {script_name} completed successfully")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        return True, duration
        
    except subprocess.CalledProcessError as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"❌ {script_name} failed")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"💬 Error: Return code {e.returncode}")
        return False, duration
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"❌ {script_name} failed")
        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"💬 Error: {str(e)}")
        return False, duration

def check_input_files():
    """Check if required input files exist"""
    required_files = [
        "../Staging/lazada_orders_raw.json",
        "../Staging/lazada_products_raw.json", 
        "../Staging/lazada_multiple_order_items_raw.json",
        "../Staging/shopee_orders_raw.json",
        "../Staging/shopee_products_raw.json"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    return missing_files

def get_output_files():
    """Get list of generated output files"""
    output_path = "../Transformed"
    output_files = []
    
    if os.path.exists(output_path):
        for file in os.listdir(output_path):
            if file.endswith('.csv'):
                file_path = os.path.join(output_path, file)
                if os.path.isfile(file_path):
                    size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    output_files.append((file, size_mb))
    
    return sorted(output_files)

def main():
    """Main function to run all harmonization scripts"""
    
    print_banner("🚀 Starting All Harmonization Scripts", "=")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Change to the Transformation directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Check Python installation
    print(f"\n🔍 Checking Python installation...")
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"✅ Python found: {python_version}")
    
    # Check input files
    print(f"\n🔍 Checking input files...")
    missing_files = check_input_files()
    
    if missing_files:
        print(f"⚠️  Warning: Missing input files:")
        for file in missing_files:
            print(f"   - {file}")
        
        response = input("\nDo you want to continue anyway? (y/N): ").strip().lower()
        if response != 'y':
            print("❌ Aborted by user.")
            return 1
    else:
        print("✅ All required input files found.")
    
    # Define scripts to run in order
    scripts = [
        "generate_dim_time.py",
        "harmonize_dim_product.py", 
        "harmonize_dim_customer.py",
        "harmonize_dim_order.py",
        "harmonize_fact_orders.py",
        "harmonize_fact_traffic.py",
        "harmonize_sales_aggregate_new.py"
    ]
    
    # Track execution
    total_scripts = len(scripts)
    succeeded = []
    failed = []
    total_duration = 0
    
    print(f"\n🚀 Starting harmonization process...")
    
    # Run each script
    for i, script in enumerate(scripts, 1):
        print_step(i, total_scripts, script)
        
        if not os.path.exists(script):
            print(f"❌ Script not found: {script}")
            failed.append(script)
            continue
        
        success, duration = run_script(script)
        total_duration += duration
        
        if success:
            succeeded.append(script)
        else:
            failed.append(script)
    
    # Summary
    print_banner("📊 HARMONIZATION SUMMARY")
    print(f"📅 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Total Duration: {total_duration:.2f} seconds")
    print(f"✅ Successful: {len(succeeded)}/{total_scripts}")
    print(f"❌ Failed: {len(failed)}/{total_scripts}")
    
    if succeeded:
        print(f"\n✅ Successful Scripts:")
        for script in succeeded:
            print(f"   - {script}")
    
    if failed:
        print(f"\n❌ Failed Scripts:")
        for script in failed:
            print(f"   - {script}")
    
    if len(failed) == 0:
        print(f"\n🎉 All harmonization scripts completed successfully!")
        print(f"📁 Output location: ../Transformed/")
        
        # Show output files
        output_files = get_output_files()
        if output_files:
            print(f"\n📋 Generated Files:")
            for filename, size_mb in output_files:
                print(f"   📄 {filename} ({size_mb:.2f} MB)")
        
        return 0
    else:
        print(f"\n⚠️  Some harmonization scripts failed. Please check the output above.")
        print(f"💡 Tip: Run individual scripts to debug specific failures.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)