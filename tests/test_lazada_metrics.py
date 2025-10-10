"""
Test Lazada Metrics and Sponsor Solu            if not self.access_token:
                raise Exception("Failed to get valid access token")
            logger.info("Access token obtained successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            return Falseeport API
Tests the /sponsor/solutions/report/getReportOverview endpoint for advertising metrics
"""

import sys
import os
import json
from datetime import datetime, timedelta
import logging

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lazop_sdk import LazopClient, LazopRequest
from app.lazada.get_lazada_tokens import get_valid_token
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LazadaMetricsTest:
    """Test class for Lazada advertising and metrics APIs"""
    
    def __init__(self):
        self.app_key = os.getenv('LAZADA_APP_KEY')
        self.app_secret = os.getenv('LAZADA_APP_SECRET')
        self.api_base = "https://api.lazada.com.ph/rest"
        self.access_token = None
        
    def get_access_token(self):
        """Get valid access token"""
        try:
            self.access_token = get_valid_token()
            if not self.access_token:
                raise Exception("Failed to get valid access token")
            logger.info(" Access token obtained successfully")
            return True
        except Exception as e:
            logger.error(f" Failed to get access token: {e}")
            return False
    
    def test_sponsor_report_overview(self, start_date=None, end_date=None):
        """
        Test the sponsor solutions report overview API
        
        Args:
            start_date: Start date for report (YYYY-MM-DD format)
            end_date: End date for report (YYYY-MM-DD format)
        """
        logger.info("Testing Sponsor Solutions Report Overview API...")
        
        # Default to last 7 days if no dates provided
        if not start_date or not end_date:
            end_date_dt = datetime.now()
            start_date_dt = end_date_dt - timedelta(days=7)
            start_date = start_date_dt.strftime('%Y-%m-%d')
            end_date = end_date_dt.strftime('%Y-%m-%d')
            
            # Last period for comparison (7 days before)
            last_end_date_dt = start_date_dt - timedelta(days=1)
            last_start_date_dt = last_end_date_dt - timedelta(days=6)
            last_start_date = last_start_date_dt.strftime('%Y-%m-%d')
            last_end_date = last_end_date_dt.strftime('%Y-%m-%d')
        else:
            # Calculate last period for comparison
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            period_days = (end_dt - start_dt).days + 1
            
            last_end_date_dt = start_dt - timedelta(days=1)
            last_start_date_dt = last_end_date_dt - timedelta(days=period_days-1)
            last_start_date = last_start_date_dt.strftime('%Y-%m-%d')
            last_end_date = last_end_date_dt.strftime('%Y-%m-%d')
        
        try:
            # Create client and request
            client = LazopClient(self.api_base, self.app_key, self.app_secret)
            request = LazopRequest('/sponsor/solutions/report/getReportOverview')
            
            # Add API parameters
            request.add_api_param('startDate', start_date)
            request.add_api_param('endDate', end_date)
            request.add_api_param('lastStartDate', last_start_date)
            request.add_api_param('lastEndDate', last_end_date)
            request.add_api_param('useRtTable', 'false')
            request.add_api_param('bizCode', 'sponsoredSearch')
            
            # Execute request
            response = client.execute(request, self.access_token)
            
            # Print response details
            print(f"\nSPONSOR SOLUTIONS REPORT OVERVIEW")
            print("=" * 60)
            print(f"Report Period: {start_date} to {end_date}")
            print(f"Comparison Period: {last_start_date} to {last_end_date}")
            print(f"Response Type: {response.type}")
            print(f"Raw Response Body: {response.body}")
            
            # Parse response if it's JSON
            try:
                if isinstance(response.body, str):
                    response_data = json.loads(response.body)
                else:
                    response_data = response.body
                
                print(f"\nParsed Response:")
                print(json.dumps(response_data, indent=2))
                
                # Check if response is successful
                if response_data.get('code') == '0':
                    print("API call successful!")
                    
                    # Extract metrics if available
                    data = response_data.get('data', {})
                    if data:
                        print(f"\nADVERTISING METRICS:")
                        print("-" * 40)
                        
                        # Common metrics that might be in the response
                        metrics_to_check = [
                            'impressions', 'clicks', 'ctr', 'cost', 'cpc',
                            'conversions', 'conversionRate', 'roas', 'revenue',
                            'orders', 'acos', 'totalSpend', 'totalSales'
                        ]
                        
                        for metric in metrics_to_check:
                            if metric in data:
                                print(f"  • {metric}: {data[metric]}")
                        
                        # Print all available data fields
                        print(f"\nAll Available Fields:")
                        for key, value in data.items():
                            print(f"  • {key}: {value}")
                    
                    return {
                        'success': True,
                        'data': response_data.get('data', {}),
                        'raw_response': response.body
                    }
                else:
                    error_msg = response_data.get('message', 'Unknown error')
                    print(f"API Error: {response_data.get('code')} - {error_msg}")
                    return {
                        'success': False,
                        'error': error_msg,
                        'code': response_data.get('code'),
                        'raw_response': response.body
                    }
                    
            except json.JSONDecodeError:
                print(f"Response is not valid JSON: {response.body}")
                return {
                    'success': False,
                    'error': 'Invalid JSON response',
                    'raw_response': response.body
                }
                
        except Exception as e:
            logger.error(f"Error testing sponsor report: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def test_multiple_date_ranges(self):
        """Test the API with multiple date ranges"""
        logger.info("Testing multiple date ranges...")
        
        date_ranges = [
            # Last 7 days
            {
                'name': 'Last 7 Days',
                'start_date': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                'end_date': datetime.now().strftime('%Y-%m-%d')
            },
            # Last 30 days
            {
                'name': 'Last 30 Days',
                'start_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                'end_date': datetime.now().strftime('%Y-%m-%d')
            },
            # Current month
            {
                'name': 'Current Month',
                'start_date': datetime.now().replace(day=1).strftime('%Y-%m-%d'),
                'end_date': datetime.now().strftime('%Y-%m-%d')
            }
        ]
        
        results = {}
        
        for date_range in date_ranges:
            print(f"\nTesting: {date_range['name']}")
            result = self.test_sponsor_report_overview(
                start_date=date_range['start_date'],
                end_date=date_range['end_date']
            )
            results[date_range['name']] = result
        
        return results
    
    def run_all_tests(self):
        """Run all metric tests"""
        print("\nSTARTING LAZADA METRICS TESTS")
        print("=" * 60)
        
        # Get access token
        if not self.get_access_token():
            print("Cannot proceed without access token")
            return False
        
        # Test basic sponsor report
        print(f"\nTEST 1: Basic Sponsor Report Overview")
        basic_result = self.test_sponsor_report_overview()
        
        # Test multiple date ranges
        print(f"\nTEST 2: Multiple Date Ranges")
        multi_results = self.test_multiple_date_ranges()
        
        # Summary
        print(f"\nTEST SUMMARY")
        print("=" * 60)
        print(f"Basic Test: {'PASSED' if basic_result.get('success') else 'FAILED'}")
        
        for range_name, result in multi_results.items():
            status = 'PASSED' if result.get('success') else 'FAILED'
            print(f"{range_name}: {status}")
        
        return True


def main():
    """Main test function"""
    tester = LazadaMetricsTest()
    tester.run_all_tests()


if __name__ == "__main__":
    main()