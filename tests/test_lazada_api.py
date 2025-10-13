import lazop
from app.config import LAZADA_TOKENS, LAZADA_API_URL

# Test the Lazada API connection
print("🧪 Testing Lazada API Connection")
print("=" * 50)

# Initialize client
client = lazop.LazopClient(LAZADA_API_URL, LAZADA_TOKENS['app_key'], LAZADA_TOKENS['app_secret'])
print(f"✅ Client initialized")
print(f"🌐 API URL: {LAZADA_API_URL}")
print(f"🔑 App Key: {LAZADA_TOKENS['app_key']}")
print(f"🎫 Access Token: {LAZADA_TOKENS['access_token'][:20]}...")

# Test products API
print("\n📦 Testing Products API...")
request = lazop.LazopRequest('/products/get', 'GET')
request.add_api_param('filter', 'all')
request.add_api_param('limit', '5')
request.add_api_param('offset', '0')

response = client.execute(request, LAZADA_TOKENS['access_token'])

print(f"Response Type: {response.type}")
print(f"Response Code: {getattr(response, 'code', 'no code')}")
print(f"Response Message: {getattr(response, 'message', 'no message')}")
print(f"Response Body Type: {type(response.body)}")

if hasattr(response, 'body') and response.body:
    print(f"Response Body Preview: {str(response.body)[:200]}...")
    
    if response.type == "ISP":
        print("\n❌ ISP Error - This usually means:")
        print("   - Invalid access token")
        print("   - Token expired") 
        print("   - Insufficient permissions")
        print("   - API endpoint not accessible")

# Test orders API
print("\n🛒 Testing Orders API...")
request2 = lazop.LazopRequest('/orders/get', 'GET')
request2.add_api_param('limit', '5')
request2.add_api_param('created_after', '2024-01-01T00:00:00+08:00')

response2 = client.execute(request2, LAZADA_TOKENS['access_token'])
print(f"Orders Response Type: {response2.type}")
print(f"Orders Response Code: {getattr(response2, 'code', 'no code')}")

print("\n🔍 Debugging Complete!")