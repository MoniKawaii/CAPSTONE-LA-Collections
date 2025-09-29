"""
Lazada API Integration Test

This file tests the Lazada API integration for the LA Collections project.
"""
import time
import hashlib
import hmac
import requests
from urllib.parse import urlencode
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Lazada API credentials from environment
APP_KEY = os.getenv('LAZADA_APP_KEY')
APP_SECRET = os.getenv('LAZADA_APP_SECRET')

if not APP_KEY or not APP_SECRET:
    raise ValueError("LAZADA_APP_KEY and LAZADA_APP_SECRET must be set in .env file")

# Lazada API URLs
LAZADA_API_BASE = "https://api.lazada.com/rest"
LAZADA_AUTH_URL = "https://auth.lazada.com/rest/auth/token/create"

def generate_signature(secret, api_path, parameters):
    """
    Generate signature for Lazada API request
    
    Args:
        secret (str): App secret
        api_path (str): API endpoint path
        parameters (dict): Request parameters
    
    Returns:
        str: Generated signature
    """
    # Sort parameters alphabetically by key
    sorted_params = sorted(parameters.items())
    
    # Create the string to sign
    parameters_str = api_path + ''.join(f'{key}{value}' for key, value in sorted_params)
    
    # Generate HMAC-SHA256 signature
    signature = hmac.new(
        secret.encode('utf-8'), 
        parameters_str.encode('utf-8'), 
        hashlib.sha256
    ).hexdigest().upper()
    
    return signature

def test_lazada_api_connection():
    """
    Test connection to Lazada API with access token
    """
    # This test requires a valid access token
    # For now, we'll test the API structure without a token
    api_path = "/seller/get"
    
    # Required parameters
    params = {
        "app_key": APP_KEY,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "sha256",
        # access_token would be required here for real requests
    }
    
    # Generate signature
    signature = generate_signature(APP_SECRET, api_path, params)
    params["sign"] = signature
    
    # Create full URL
    url = LAZADA_API_BASE + api_path
    
    try:
        # Make the request
        response = requests.get(url, params=params, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        # Try to parse JSON response
        try:
            json_response = response.json()
            print(f"JSON Response: {json.dumps(json_response, indent=2)}")
        except json.JSONDecodeError:
            print(f"Non-JSON Response: {response.text[:500]}...")
            
        return response.status_code == 200
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return False
    """
    Get access token and refresh token using authorization code
    
    Args:
        auth_code (str): Authorization code obtained from Lazada authorization flow
    
    Returns:
        dict: Token response containing access_token, refresh_token, etc.
    """
    # API endpoint for token creation
    api_path = "/auth/token/create"
    
    # Required parameters for token creation
    params = {
        "app_key": APP_KEY,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "sha256",
        "code": auth_code
    }
    
    # Generate signature
    signature = generate_signature(APP_SECRET, api_path, params)
    params["sign"] = signature
    
    # Full URL for token endpoint
    url = LAZADA_AUTH_URL
    
    try:
        # Make POST request to get tokens
        response = requests.post(url, data=params, timeout=10)
        
        print(f"Token Request Status Code: {response.status_code}")
        print(f"Token Request Headers: {dict(response.headers)}")
        
        # Parse JSON response
        try:
            json_response = response.json()
            print(f"Token Response: {json.dumps(json_response, indent=2)}")
            
            if response.status_code == 200 and 'access_token' in json_response:
                return {
                    'success': True,
                    'access_token': json_response.get('access_token'),
                    'refresh_token': json_response.get('refresh_token'),
                    'expires_in': json_response.get('expires_in'),
                    'refresh_expires_in': json_response.get('refresh_expires_in'),
                    'account_platform': json_response.get('account_platform'),
                    'country_user_info': json_response.get('country_user_info')
                }
            else:
                return {
                    'success': False,
                    'error': json_response.get('message', 'Unknown error'),
                    'code': json_response.get('code', 'Unknown'),
                    'response': json_response
                }
                
        except json.JSONDecodeError:
            print(f"Non-JSON Response: {response.text}")
            return {
                'success': False,
                'error': 'Invalid JSON response',
                'response_text': response.text
            }
            
    except requests.exceptions.RequestException as e:
        print(f"Token request failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def refresh_access_token(refresh_token):
    """
    Refresh access token using refresh token
    
    Args:
        refresh_token (str): Refresh token
    
    Returns:
        dict: New token response
    """
    # API endpoint for token refresh
    api_path = "/auth/token/refresh"
    
    # Required parameters for token refresh
    params = {
        "app_key": APP_KEY,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "sha256",
        "refresh_token": refresh_token
    }
    
    # Generate signature
    signature = generate_signature(APP_SECRET, api_path, params)
    params["sign"] = signature
    
    # Full URL for refresh endpoint
    url = LAZADA_AUTH_URL.replace("/create", "/refresh")
    
    try:
        # Make POST request to refresh token
        response = requests.post(url, data=params, timeout=10)
        
        print(f"Refresh Token Status Code: {response.status_code}")
        
        # Parse JSON response
        try:
            json_response = response.json()
            print(f"Refresh Response: {json.dumps(json_response, indent=2)}")
            
            if response.status_code == 200 and 'access_token' in json_response:
                return {
                    'success': True,
                    'access_token': json_response.get('access_token'),
                    'refresh_token': json_response.get('refresh_token'),
                    'expires_in': json_response.get('expires_in'),
                    'refresh_expires_in': json_response.get('refresh_expires_in')
                }
            else:
                return {
                    'success': False,
                    'error': json_response.get('message', 'Unknown error'),
                    'code': json_response.get('code', 'Unknown'),
                    'response': json_response
                }
                
        except json.JSONDecodeError:
            return {
                'success': False,
                'error': 'Invalid JSON response',
                'response_text': response.text
            }
            
    except requests.exceptions.RequestException as e:
        print(f"Refresh token request failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def get_authorization_url():
    """
    Generate authorization URL for getting auth code
    
    Returns:
        str: Authorization URL
    """
    # Lazada authorization URL
    auth_base_url = "https://auth.lazada.com/oauth/authorize"
    
    # Parameters for authorization
    auth_params = {
        "response_type": "code",
        "force_auth": "true",
        "redirect_uri": "https://your-app.com/callback",  # Replace with your actual callback URL
        "client_id": APP_KEY
    }
    
    # Create authorization URL
    auth_url = auth_base_url + "?" + urlencode(auth_params)
    return auth_url

def test_token_generation():
    """
    Test token generation workflow (demonstration only)
    """
    print("=== Token Generation Workflow ===")
    print("Step 1: Get Authorization URL")
    auth_url = get_authorization_url()
    print(f"Authorization URL: {auth_url}")
    print("\nInstructions:")
    print("1. Visit the authorization URL above")
    print("2. Log in to your Lazada seller account")
    print("3. Accept the permissions")
    print("4. Copy the 'code' parameter from the callback URL")
    print("5. Use that code with get_access_token() function")
    print("\nExample usage:")
    print("# After getting auth_code from callback")
    print("# token_result = get_access_token('your_auth_code_here')")
    print("# if token_result['success']:")
    print("#     access_token = token_result['access_token']")
    print("#     refresh_token = token_result['refresh_token']")
    print("\nStep 2: Test with dummy code (will fail, but shows the flow)")
    
    # Test with dummy code (will fail but shows the request format)
    dummy_result = get_access_token("dummy_code_for_testing")
    print(f"Dummy test result: {dummy_result['success']}")
    if not dummy_result['success']:
        print(f"Expected error: {dummy_result['error']}")
    
    return dummy_result['success']  # Will be False, but that's expected

def test_lazada_api_connection():
    """
    Test connection to Lazada API with access token
    """
    # This test requires a valid access token
    # For now, we'll test the API structure without a token
    api_path = "/seller/get"
    
    # Required parameters
    params = {
        "app_key": APP_KEY,
        "timestamp": str(int(time.time() * 1000)),  # timestamp in milliseconds
        "sign_method": "sha256",
        # Add other required parameters as needed
    }
    
    # Generate signature
    signature = generate_signature(APP_SECRET, api_path, params)
    params["sign"] = signature
    
    # Create full URL
    url = LAZADA_API_BASE + api_path
    
    try:
        # Make the request
        response = requests.get(url, params=params, timeout=10)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        # Try to parse JSON response
        try:
            json_response = response.json()
            print(f"JSON Response: {json.dumps(json_response, indent=2)}")
        except json.JSONDecodeError:
            print(f"Non-JSON Response: {response.text[:500]}...")  # First 500 chars
            
        return response.status_code == 200
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return False

def get_access_token(auth_code):
    """
    Get access token and refresh token using authorization code
    
    Args:
        auth_code (str): Authorization code obtained from Lazada authorization flow
    
    Returns:
        dict: Token response containing access_token, refresh_token, etc.
    """
    # API endpoint for token creation
    api_path = "/auth/token/create"
    
    # Required parameters for token creation
    params = {
        "app_key": APP_KEY,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "sha256",
        "code": auth_code
    }
    
    # Generate signature
    signature = generate_signature(APP_SECRET, api_path, params)
    params["sign"] = signature
    
    # Full URL for token endpoint
    url = LAZADA_AUTH_URL
    
    try:
        # Make POST request to get tokens
        response = requests.post(url, data=params, timeout=10)
        
        print(f"Token Request Status Code: {response.status_code}")
        print(f"Token Request Headers: {dict(response.headers)}")
        
        # Parse JSON response
        try:
            json_response = response.json()
            print(f"Token Response: {json.dumps(json_response, indent=2)}")
            
            if response.status_code == 200 and 'access_token' in json_response:
                return {
                    'success': True,
                    'access_token': json_response.get('access_token'),
                    'refresh_token': json_response.get('refresh_token'),
                    'expires_in': json_response.get('expires_in'),
                    'refresh_expires_in': json_response.get('refresh_expires_in'),
                    'account_platform': json_response.get('account_platform'),
                    'country_user_info': json_response.get('country_user_info')
                }
            else:
                return {
                    'success': False,
                    'error': json_response.get('message', 'Unknown error'),
                    'code': json_response.get('code', 'Unknown'),
                    'response': json_response
                }
                
        except json.JSONDecodeError:
            print(f"Non-JSON Response: {response.text}")
            return {
                'success': False,
                'error': 'Invalid JSON response',
                'response_text': response.text
            }
            
    except requests.exceptions.RequestException as e:
        print(f"Token request failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def refresh_access_token(refresh_token):
    """
    Refresh access token using refresh token
    
    Args:
        refresh_token (str): Refresh token
    
    Returns:
        dict: New token response
    """
    # API endpoint for token refresh
    api_path = "/auth/token/refresh"
    
    # Required parameters for token refresh
    params = {
        "app_key": APP_KEY,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "sha256",
        "refresh_token": refresh_token
    }
    
    # Generate signature
    signature = generate_signature(APP_SECRET, api_path, params)
    params["sign"] = signature
    
    # Full URL for refresh endpoint
    url = LAZADA_AUTH_URL.replace("/create", "/refresh")
    
    try:
        # Make POST request to refresh token
        response = requests.post(url, data=params, timeout=10)
        
        print(f"Refresh Token Status Code: {response.status_code}")
        
        # Parse JSON response
        try:
            json_response = response.json()
            print(f"Refresh Response: {json.dumps(json_response, indent=2)}")
            
            if response.status_code == 200 and 'access_token' in json_response:
                return {
                    'success': True,
                    'access_token': json_response.get('access_token'),
                    'refresh_token': json_response.get('refresh_token'),
                    'expires_in': json_response.get('expires_in'),
                    'refresh_expires_in': json_response.get('refresh_expires_in')
                }
            else:
                return {
                    'success': False,
                    'error': json_response.get('message', 'Unknown error'),
                    'code': json_response.get('code', 'Unknown'),
                    'response': json_response
                }
                
        except json.JSONDecodeError:
            return {
                'success': False,
                'error': 'Invalid JSON response',
                'response_text': response.text
            }
            
    except requests.exceptions.RequestException as e:
        print(f"Refresh token request failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def get_authorization_url():
    """
    Generate authorization URL for getting auth code
    
    Returns:
        str: Authorization URL
    """
    # Lazada authorization URL
    auth_base_url = "https://auth.lazada.com/oauth/authorize"
    
    # Parameters for authorization
    auth_params = {
        "response_type": "code",
        "force_auth": "true",
        "redirect_uri": "https://your-app.com/callback",  # Replace with your actual callback URL
        "client_id": APP_KEY
    }
    
    # Create authorization URL
    auth_url = auth_base_url + "?" + urlencode(auth_params)
    return auth_url

def test_token_generation():
    """
    Test token generation workflow (demonstration only)
    """
    print("=== Token Generation Workflow ===")
    print("Step 1: Get Authorization URL")
    auth_url = get_authorization_url()
    print(f"Authorization URL: {auth_url}")
    print("\nInstructions:")
    print("1. Visit the authorization URL above")
    print("2. Log in to your Lazada seller account")
    print("3. Accept the permissions")
    print("4. Copy the 'code' parameter from the callback URL")
    print("5. Use that code with get_access_token() function")
    print("\nExample usage:")
    print("# After getting auth_code from callback")
    print("# token_result = get_access_token('your_auth_code_here')")
    print("# if token_result['success']:")
    print("#     access_token = token_result['access_token']")
    print("#     refresh_token = token_result['refresh_token']")
    print("\nStep 2: Test with dummy code (will fail, but shows the flow)")
    
    # Test with dummy code (will fail but shows the request format)
    dummy_result = get_access_token("dummy_code_for_testing")
    print(f"Dummy test result: {dummy_result['success']}")
    if not dummy_result['success']:
        print(f"Expected error: {dummy_result['error']}")
    
    return dummy_result['success']  # Will be False, but that's expected

def save_tokens_to_file(token_data, filename="lazada_tokens.json"):
    """
    Save tokens to a JSON file
    
    Args:
        token_data (dict): Token data from get_access_token()
        filename (str): Filename to save tokens
    """
    if token_data['success']:
        token_info = {
            'access_token': token_data['access_token'],
            'refresh_token': token_data['refresh_token'],
            'expires_in': token_data['expires_in'],
            'refresh_expires_in': token_data['refresh_expires_in'],
            'created_at': int(time.time()),
            'account_platform': token_data.get('account_platform'),
            'country_user_info': token_data.get('country_user_info')
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(token_info, f, indent=2)
            print(f"Tokens saved to {filename}")
            return True
        except Exception as e:
            print(f"Error saving tokens: {e}")
            return False
    else:
        print("Cannot save tokens - token generation failed")
        return False

def load_tokens_from_file(filename="lazada_tokens.json"):
    """
    Load tokens from a JSON file
    
    Args:
        filename (str): Filename to load tokens from
    
    Returns:
        dict: Token data or None if file doesn't exist
    """
    try:
        with open(filename, 'r') as f:
            token_data = json.load(f)
        print(f"Tokens loaded from {filename}")
        return token_data
    except FileNotFoundError:
        print(f"Token file {filename} not found")
        return None
    except Exception as e:
        print(f"Error loading tokens: {e}")
        return None

def is_token_expired(token_data):
    """
    Check if access token is expired
    
    Args:
        token_data (dict): Token data from load_tokens_from_file()
    
    Returns:
        bool: True if token is expired
    """
    if not token_data or 'created_at' not in token_data or 'expires_in' not in token_data:
        return True
    
    created_at = token_data['created_at']
    expires_in = token_data['expires_in']
    current_time = int(time.time())
    
    # Add 5 minute buffer to account for network delays
    expiry_time = created_at + expires_in - 300
    
    return current_time >= expiry_time

def test_csv_processing():
    """
    Test CSV processing functionality with sample Lazada data
    """
    try:
        # Import local modules
        from app.etl import process_csv_file
        import io
        
        # Sample Lazada CSV data
        sample_csv = """Date,Revenue,Visitors,Buyers,Orders,Pageviews,Units Sold,Conversion Rate,Revenue per Buyer,Visitor Value,Add to Cart Users,Add to Cart Units,Wishlists,Wishlist Users,Average Order Value,Average Basket Size,Cancelled Amount,Return/Refund Amount
2024-05-01~2024-05-31,149140.9,6879,249,261,14826,397,3.62%,598.96,21.68,1710,2125,285,257,571.42,1.52,9783.56,0
01/05/2024,7595.81,95,9,9,202,20,9.47%,843.98,79.96,19,29,9,7,843.98,2.22,2235,0
02/05/2024,5444.28,72,11,11,186,16,15.28%,494.93,75.62,17,23,11,10,494.93,1.45,1098.6,0"""
        
        # Process the CSV
        file_like = io.StringIO(sample_csv)
        result = process_csv_file(file_like, "Lazada", save_to_db=False)
        
        print(f"CSV Processing Status: {result['status']}")
        if result['status'] == 'success':
            df = result['dataframe']
            print(f"Processed {len(df)} rows")
            print(f"Columns: {list(df.columns)}")
            print("Sample data:")
            print(df[['date', 'total_sales_value', 'total_orders']].head())
            return True
        else:
            print(f"CSV Processing Error: {result['detail']}")
            return False
            
    except ImportError as e:
        print(f"Import error: {e}")
        return False
    except Exception as e:
        print(f"CSV processing failed: {e}")
        return False

if __name__ == "__main__":
    print("=== Lazada Integration Tests ===\n")
    
    print("1. Testing CSV Processing...")
    csv_test_passed = test_csv_processing()
    print(f"CSV Test: {'PASSED' if csv_test_passed else 'FAILED'}\n")
    
    print("2. Testing Token Generation Workflow...")
    token_test_passed = test_token_generation()
    print(f"Token Test: {'PASSED' if token_test_passed else 'EXPECTED FAILURE (no real auth code)'}\n")
    
    print("3. Testing Lazada API Connection...")
    api_test_passed = test_lazada_api_connection()
    print(f"API Test: {'PASSED' if api_test_passed else 'FAILED'}\n")
    
    print("=== Test Summary ===")
    print(f"CSV Processing: {'✓' if csv_test_passed else '✗'}")
    print(f"Token Generation: {'✓' if token_test_passed else '○ (Expected - needs real auth code)'}")
    print(f"API Connection: {'✓' if api_test_passed else '✗'}")
    
    print("\n=== Next Steps for Token Generation ===")
    print("1. Visit the authorization URL shown above")
    print("2. Complete the OAuth flow to get an auth code")
    print("3. Replace 'dummy_code_for_testing' with your actual auth code")
    print("4. Re-run the test to get real access and refresh tokens")
