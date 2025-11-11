#!/usr/bin/env python3
"""
Master Validation Test Suite
============================

Central script to run all validation tests for the harmonization pipeline.
"""

import os
import sys

def list_available_tests():
    """List all available validation tests"""
    print("🧪 AVAILABLE VALIDATION TESTS")
    print("=" * 50)
    
    test_descriptions = {
        "quick_readiness_check.py": "Check if harmonization is ready to run",
        "post_harmonization_validation.py": "Validate harmonization completed successfully",
        "test_harmonization_integrity.py": "Comprehensive harmonization system test",
        "validate_dim_order_fix.py": "Validate dim_order price mapping fix",
        "validate_price_fix_impact.py": "Analyze price fix impact on business metrics",
        "fix_lazada_price_mapping.py": "Emergency price mapping fix script",
        "analyze_harmonization_discrepancies.py": "Analyze data discrepancies between stages",
        "check_price_validity.py": "Check price data validity and patterns",
        "analyze_missing_completed_orders.py": "Analyze missing completed orders issue"
    }
    
    print("📋 VALIDATION TESTS:")
    for script, description in test_descriptions.items():
        if os.path.exists(script):
            print(f"  ✅ {script}")
            print(f"      {description}")
        else:
            print(f"  ⚠️  {script} (not found)")
    
    print(f"\n📁 Total tests available: {sum(1 for script in test_descriptions.keys() if os.path.exists(script))}")

def run_test_suite():
    """Run the recommended test sequence"""
    print("\n🚀 RUNNING RECOMMENDED TEST SEQUENCE")
    print("=" * 50)
    
    recommended_sequence = [
        ("quick_readiness_check.py", "Pre-harmonization readiness check"),
        ("post_harmonization_validation.py", "Post-harmonization validation")
    ]
    
    for script, description in recommended_sequence:
        if os.path.exists(script):
            print(f"\n▶️  Running: {script}")
            print(f"   {description}")
            print("-" * 40)
            
            # Run the script
            exit_code = os.system(f"python {script}")
            
            if exit_code == 0:
                print(f"✅ {script} completed successfully")
            else:
                print(f"❌ {script} failed with exit code {exit_code}")
                print("   Check the output above for details")
                return False
        else:
            print(f"⚠️  {script} not found, skipping...")
    
    print(f"\n🎉 Test sequence completed successfully!")
    return True

def show_usage():
    """Show usage instructions"""
    print("📖 USAGE INSTRUCTIONS")
    print("=" * 50)
    print()
    print("Available commands:")
    print("  python master_validation.py list     - List all available tests")
    print("  python master_validation.py run      - Run recommended test sequence")
    print("  python master_validation.py help     - Show this help")
    print()
    print("Individual test usage:")
    print("  python quick_readiness_check.py      - Before harmonization")
    print("  python post_harmonization_validation.py - After harmonization")
    print("  python validate_dim_order_fix.py     - Check price fix status")
    print()
    print("Emergency fix:")
    print("  python fix_lazada_price_mapping.py   - Fix price mapping issues")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "list":
            list_available_tests()
        elif command == "run":
            success = run_test_suite()
            exit(0 if success else 1)
        elif command == "help":
            show_usage()
        else:
            print(f"❌ Unknown command: {command}")
            show_usage()
            exit(1)
    else:
        print("🧪 MASTER VALIDATION TEST SUITE")
        print("=" * 50)
        list_available_tests()
        print("\n" + "=" * 50)
        show_usage()
        print("\n💡 TIP: Run 'python master_validation.py run' for automated testing")