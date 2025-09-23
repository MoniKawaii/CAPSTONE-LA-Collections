import requests
import io

# Test the upload endpoint directly
def test_upload():
    url = "http://localhost:8000/upload"
    
    # Sample CSV content (minimal test)
    csv_content = """Date,Revenue,Visitors,Buyers,Orders,Pageviews,Units Sold,Conversion Rate,Revenue per Buyer,Visitor Value,Add to Cart Users,Add to Cart Units,Wishlists,Wishlist Users,Average Order Value,Average Basket Size,Cancelled Amount,Return/Refund Amount
2024-05-01,7595.81,95,9,9,202,20,9.47%,843.98,79.96,19,29,9,7,843.98,2.22,2235,0"""
    
    # Create file-like object
    files = {'file': ('test.csv', csv_content, 'text/csv')}
    data = {'platform': 'Lazada'}
    
    try:
        response = requests.post(url, files=files, data=data)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        if response.status_code == 200:
            json_response = response.json()
            print(f"Inserted rows: {json_response.get('inserted', 'Unknown')}")
        else:
            print(f"Error: {response.text}")
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to backend server. Is it running on port 8000?")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_upload()