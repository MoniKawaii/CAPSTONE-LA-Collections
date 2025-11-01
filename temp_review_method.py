    def extract_review_history_list(self, start_fresh=False, limit_products=None):
        """
        Step 1: Extract review IDs using /review/seller/history/list API
        This API gets historical reviews with time-based pagination (7-day chunks)
        Limitations:
        - Cannot query reviews older than 3 months
        - Response time is within 1 month
        - Time range between start_time and end_time must be 7 days maximum
        
        Args:
            start_fresh (bool): Whether to start fresh or append to existing data
            limit_products (int): Not used in this API, kept for compatibility
        
        Returns:
            list: List of review IDs from historical data
        """
        filename = 'lazada_reviewhistorylist_raw.json'
        
        if not start_fresh:
            existing_ids = self._load_from_json(filename)
            if existing_ids:
                print(f"📋 Found {len(existing_ids)} existing review IDs. Use start_fresh=True to overwrite.")
                return existing_ids
        
        print(f"🔍 Starting historical review ID extraction using /review/seller/history/list...")
        
        all_review_ids = []
        
        # Calculate date ranges - API limitation: 3 months back maximum
        end_date = datetime.now()
        # Go back 3 months maximum (API limitation)  
        start_date = end_date - timedelta(days=90)  # 3 months
        
        print(f"📅 Extracting review IDs from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"⚠️ API Limitations: 7-day chunks, 3-month historical limit, 1-month response time")
        
        # Process in 7-day chunks (API requirement)
        current_date = start_date
        chunk_count = 0
        
        while current_date < end_date:
            if self.api_calls_made >= self.max_daily_calls:
                print("⚠️ Daily API limit reached")
                break
            
            # Calculate 7-day chunk end date
            chunk_end = min(current_date + timedelta(days=7), end_date)
            chunk_count += 1
            
            # Format dates for API (ISO format)
            start_time = current_date.strftime('%Y-%m-%dT%H:%M:%S+08:00')
            end_time = chunk_end.strftime('%Y-%m-%dT%H:%M:%S+08:00')
            
            print(f"\n📦 Chunk {chunk_count}: {current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            # Add rate limiting between chunks
            if chunk_count > 1:
                import time
                print(f"   ⏳ Rate limiting: waiting 10 seconds...")
                time.sleep(10)
            
            try:
                # API call to get historical review IDs
                request = lazop.LazopRequest('/review/seller/history/list', 'GET')
                request.add_api_param('start_time', start_time)
                request.add_api_param('end_time', end_time)
                request.add_api_param('current', '1')  # Page number
                request.add_api_param('limit', '100')  # Max IDs per page
                
                print(f"   📡 API Call: /review/seller/history/list from {start_time} to {end_time}")
                
                response_data = self._make_api_call(request, f"review-history-{chunk_count}")
                
                if response_data and response_data.get('data'):
                    data = response_data['data']
                    
                    # Check for review ID list
                    if 'id_list' in data and data['id_list']:
                        id_list = data['id_list']
                        print(f"   ✅ Found {len(id_list)} review IDs in this chunk")
                        
                        # Save each review ID with metadata
                        for review_id in id_list:
                            review_entry = {
                                'review_id': review_id,
                                'extraction_period_start': start_time,
                                'extraction_period_end': end_time,
                                'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'type': 'historical_review_id',
                                'chunk_number': chunk_count
                            }
                            all_review_ids.append(review_entry)
                    
                    # Handle pagination if there are more pages
                    total_count = data.get('total_count', 0)
                    if total_count > len(data.get('id_list', [])):
                        print(f"   📄 More pages available: {total_count} total IDs")
                        
                        # Calculate additional pages
                        pages_needed = (total_count // 100) + (1 if total_count % 100 > 0 else 0)
                        
                        for page in range(2, min(pages_needed + 1, 6)):  # Limit to 5 pages max per chunk
                            request = lazop.LazopRequest('/review/seller/history/list', 'GET')
                            request.add_api_param('start_time', start_time)
                            request.add_api_param('end_time', end_time)
                            request.add_api_param('current', str(page))
                            request.add_api_param('limit', '100')
                            
                            page_data = self._make_api_call(request, f"review-history-{chunk_count}-p{page}")
                            
                            if page_data and page_data.get('data', {}).get('id_list'):
                                page_ids = page_data['data']['id_list']
                                print(f"   📄 Page {page}: Found {len(page_ids)} more review IDs")
                                
                                for review_id in page_ids:
                                    review_entry = {
                                        'review_id': review_id,
                                        'extraction_period_start': start_time,
                                        'extraction_period_end': end_time,
                                        'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'type': 'historical_review_id',
                                        'chunk_number': chunk_count,
                                        'page_number': page
                                    }
                                    all_review_ids.append(review_entry)
                            
                            # Rate limiting between pages
                            time.sleep(2)
                    
                    if not data.get('id_list'):
                        print(f"   ℹ️ No review IDs found for this time period")
                else:
                    print(f"   ⚠️ No data returned for this time period")
                    
            except Exception as e:
                print(f"   ❌ Error processing time chunk {chunk_count}: {e}")
                continue
            
            # Move to next 7-day chunk
            current_date = chunk_end
        
        # Save all collected review IDs
        self._save_to_json(all_review_ids, filename)
        
        print(f"\n🎉 Historical review ID extraction complete!")
        print(f"   Time chunks processed: {chunk_count}")
        print(f"   Total review IDs collected: {len(all_review_ids)}")
        print(f"   Saved to: {filename}")
        print(f"   ⚠️ Note: These are IDs only. Use extract_review_details() to get content.")
        
        return all_review_ids