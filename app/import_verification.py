"""
Supabase Client Import Verification & Fix Script

This script verifies that all imports are working correctly and provides
solutions for any environment issues.
"""

import os
import sys

def check_python_environment():
    """Check current Python environment details"""
    print("=== Python Environment Check ===")
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python path: {sys.path[:3]}...")  # Show first 3 paths
    

def test_all_imports():
    """Test all required imports"""
    print("\n=== Import Testing ===")
    
    # Add app directory to path
    sys.path.append('./app')
    sys.path.append('.')
    
    imports_to_test = [
        ("Standard Libraries", [
            ('os', 'import os'),
            ('sys', 'import sys'),
            ('time', 'import time'),
            ('hmac', 'import hmac'),
            ('hashlib', 'import hashlib'),
        ]),
        ("Third-party Packages", [
            ('requests', 'import requests'),
            ('pandas', 'import pandas as pd'),
            ('dotenv', 'from dotenv import load_dotenv'),
            ('supabase', 'from supabase import create_client'),
        ]),
        ("Project Modules", [
            ('supabase_client', 'from supabase_client import supabase'),
            ('lazada_api_client', 'import lazada_api_client'),
        ])
    ]
    
    for category, imports in imports_to_test:
        print(f"\n{category}:")
        for name, import_statement in imports:
            try:
                exec(import_statement)
                print(f"  ✓ {name}: SUCCESS")
            except Exception as e:
                print(f"  ✗ {name}: ERROR - {e}")

def check_environment_variables():
    """Check required environment variables"""
    print("\n=== Environment Variables Check ===")
    
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✓ .env file loaded successfully")
    except Exception as e:
        print(f"✗ Failed to load .env: {e}")
        return
    
    required_vars = [
        'SUPABASE_URL',
        'SUPABASE_ANON_KEY',
        'SUPABASE_SERVICE_ROLE_KEY',
        'LAZADA_APP_KEY',
        'LAZADA_APP_SECRET',
        'LAZADA_ACCESS_TOKEN',
        'LAZADA_REFRESH_TOKEN'
    ]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Show first 10 chars for security
            display_value = value[:10] + "..." if len(value) > 10 else value
            print(f"  ✓ {var}: {display_value}")
        else:
            print(f"  ✗ {var}: MISSING")

def test_supabase_connection():
    """Test Supabase connection"""
    print("\n=== Supabase Connection Test ===")
    
    try:
        sys.path.append('./app')
        from supabase_client import supabase
        
        # Test connection with a simple query
        result = supabase.table('Dim_Platform').select('*', count='exact').limit(1).execute()
        print(f"✓ Supabase connection successful")
        print(f"  Platform count: {result.count}")
        
    except Exception as e:
        print(f"✗ Supabase connection failed: {e}")

def provide_solutions():
    """Provide solutions for common issues"""
    print("\n=== Solutions for Common Issues ===")
    
    solutions = [
        "1. Use the correct Python environment:",
        "   - Current working env: .venv\\Scripts\\python.exe",
        "   - Run scripts with: python your_script.py",
        "",
        "2. If imports fail in other terminals:",
        "   - Activate virtual environment first:",
        "   - .venv\\Scripts\\activate.bat",
        "",
        "3. For conda environment use:",
        "   - C:\\Users\\alyss\\anaconda3\\python.exe your_script.py",
        "",
        "4. All packages are installed in .venv environment:",
        "   - supabase (2.20.0)",
        "   - pandas (2.3.3)", 
        "   - python-dotenv (1.1.1)",
        "   - psycopg2-binary (2.9.10)",
        "   - fastapi (0.118.0)",
        "",
        "5. Environment variables are properly configured in .env"
    ]
    
    for solution in solutions:
        print(solution)

if __name__ == "__main__":
    print("🔧 Supabase Client Import Verification & Fix")
    print("=" * 50)
    
    check_python_environment()
    test_all_imports()
    check_environment_variables()
    test_supabase_connection()
    provide_solutions()
    
    print("\n" + "=" * 50)
    print("✅ Import verification complete!")
    print("If all tests show SUCCESS, your environment is properly configured.")