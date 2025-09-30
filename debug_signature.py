"""
Lazada Signature Debug - Finding the correct signature format
"""

import os
import time
import hmac
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()

def test_different_signature_formats():
    """Test different signature generation formats"""
    
    print("🔍 TESTING DIFFERENT SIGNATURE FORMATS")
    print("=" * 60)
    
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    app_key = os.getenv('LAZADA_APP_KEY')
    app_secret = os.getenv('LAZADA_APP_SECRET')
    
    api_path = '/auth/token/refresh'
    timestamp = str(int(time.time() * 1000))
    
    parameters = {
        'app_key': app_key,
        'refresh_token': refresh_token,
        'sign_method': 'sha256',
        'timestamp': timestamp
    }
    
    # Test different signature formats
    formats = [
        ("Format 1: path + query (no separator)", lambda p, q: p + q),
        ("Format 2: path + ? + query", lambda p, q: p + "?" + q),
        ("Format 3: path + & + query", lambda p, q: p + "&" + q),
        ("Format 4: just query string", lambda p, q: q),
        ("Format 5: path + query with URL encoding", None)  # Special case
    ]
    
    sorted_params = sorted(parameters.items())
    query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
    
    print(f"🔑 App Key: {app_key}")
    print(f"📋 Query String: {query_string[:50]}...")
    print()
    
    for i, (format_name, format_func) in enumerate(formats, 1):
        print(f"{i}. {format_name}")
        
        if format_func:
            string_to_sign = format_func(api_path, query_string)
        else:
            # Special case for URL encoding
            import urllib.parse
            encoded_params = []
            for k, v in sorted_params:
                encoded_value = urllib.parse.quote_plus(str(v))
                encoded_params.append(f"{k}={encoded_value}")
            encoded_query = '&'.join(encoded_params)
            string_to_sign = api_path + encoded_query
        
        print(f"   String to sign: {string_to_sign[:80]}...")
        
        signature = hmac.new(
            app_secret.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()
        
        print(f"   Signature: {signature}")
        
        # Test this signature
        test_params = parameters.copy()
        test_params['sign'] = signature
        
        url = 'https://auth.lazada.com/rest' + api_path
        
        try:
            response = requests.post(url, data=test_params, timeout=10)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                if 'access_token' in result:
                    print(f"   ✅ SUCCESS! This format works!")
                    return i, string_to_sign, signature, result
                else:
                    error_code = result.get('code', 'Unknown')
                    print(f"   ❌ Error: {error_code}")
            else:
                print(f"   ❌ HTTP Error: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Request Error: {str(e)[:50]}")
        
        print()
    
    print("❌ None of the tested formats worked")
    return None, None, None, None

def analyze_lazada_docs():
    """Analyze what we know about Lazada signature format"""
    
    print("\n📚 LAZADA SIGNATURE ANALYSIS")
    print("=" * 50)
    
    print("📖 From Lazada API documentation:")
    print("1. Sort parameters alphabetically")
    print("2. Create query string: key1=value1&key2=value2")
    print("3. Concatenate: API_PATH + QUERY_STRING")
    print("4. Sign with HMAC-SHA256")
    print("5. Convert to uppercase")
    print()
    
    print("🤔 Possible issues:")
    print("• URL encoding of parameters")
    print("• Special character handling")
    print("• Timestamp format")
    print("• Parameter naming")
    print("• Environment (sandbox vs production)")
    print()
    
    print("🎯 Let's check official examples...")

def check_token_validity():
    """Check if the tokens themselves are valid"""
    
    print("\n🔍 TOKEN VALIDITY CHECK")
    print("=" * 50)
    
    access_token = os.getenv('LAZADA_ACCESS_TOKEN')
    refresh_token = os.getenv('LAZADA_REFRESH_TOKEN')
    
    print(f"Access Token Length: {len(access_token)}")
    print(f"Refresh Token Length: {len(refresh_token)}")
    
    # Check token format patterns
    if access_token.startswith('50000'):
        print("✅ Access token appears to be production format")
    else:
        print("⚠️ Access token format unclear")
    
    if refresh_token.startswith('50001'):
        print("✅ Refresh token appears to be production format")
    else:
        print("⚠️ Refresh token format unclear")
    
    # Check if tokens are expired (rough estimate)
    import time
    current_timestamp = int(time.time())
    print(f"Current timestamp: {current_timestamp}")
    
    # Try to extract timestamp from token (if possible)
    try:
        # Some tokens contain timestamp info
        if len(access_token) > 20:
            print("📅 Tokens appear to be in expected format")
    except:
        pass

def main():
    """Run comprehensive signature debugging"""
    
    print("🐛 LAZADA SIGNATURE DEBUG")
    print("=" * 60)
    print("🔍 Finding the correct signature format")
    print()
    
    # Check token validity first
    check_token_validity()
    
    # Test different signature formats
    working_format, string_to_sign, signature, result = test_different_signature_formats()
    
    if working_format:
        print(f"\n🎉 FOUND WORKING FORMAT: {working_format}")
        print(f"📋 String to sign: {string_to_sign}")
        print(f"🔐 Signature: {signature}")
        print(f"🎯 Result: {result}")
        
        # Update .env with new tokens
        if 'access_token' in result:
            print("\n💾 Updating .env with new tokens...")
            # Here you'd update the .env file
    else:
        print("\n❌ NO WORKING SIGNATURE FORMAT FOUND")
        print("🔧 POSSIBLE SOLUTIONS:")
        print("1. Check Lazada Developer Console for app status")
        print("2. Verify app is approved for API access")
        print("3. Try fresh OAuth authorization")
        print("4. Contact Lazada support")
        print("5. Check if using correct environment (sandbox/production)")
    
    # Show documentation analysis
    analyze_lazada_docs()

if __name__ == "__main__":
    main()