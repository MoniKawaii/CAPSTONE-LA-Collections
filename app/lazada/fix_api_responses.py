"""
Fix API response parsing in lazada_api_functions.py
Handle both dict and string response bodies from lazop SDK
"""

import os

def fix_api_functions():
    """Update all API functions to handle both dict and string response bodies"""
    
    file_path = os.path.join(os.path.dirname(__file__), 'lazada_api_functions.py')
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix response body parsing across all functions
    old_pattern = """        # Parse response body
        response_data = json.loads(response.body) if response.body else {}"""
    
    new_pattern = """        # Parse response body  
        if isinstance(response.body, dict):
            response_data = response.body
        else:
            response_data = json.loads(response.body) if response.body else {}"""
    
    # Replace all occurrences
    content = content.replace(old_pattern, new_pattern)
    
    # Write back to file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ Fixed API response parsing for all functions")

if __name__ == "__main__":
    fix_api_functions()