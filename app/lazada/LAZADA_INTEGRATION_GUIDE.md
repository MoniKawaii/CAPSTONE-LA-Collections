# Lazada Integration with ngrok - Complete Guide

This integration provides automatic Lazada API access with token management and ngrok callback handling.

## Features

✅ **Automatic Token Management**: Tokens refresh automatically before expiration  
✅ **ngrok Integration**: No need for manual Postman callbacks  
✅ **Lazada SDK**: Official SDK for reliable API communication  
✅ **ETL Pipeline Integration**: Direct integration with your existing data pipeline  
✅ **Background Sync**: Automated data synchronization

## Quick Start

### 1. Setup ngrok and Get Authorization

```bash
# Start the setup script
python setup_lazada.py
```

### 2. Start FastAPI Server

```bash
# Start the server with auto-reload
uvicorn main:app --reload --port 8000
```

### 3. Complete OAuth Flow

Visit the authorization URL from step 1, complete the OAuth flow, and tokens will be saved automatically.

## API Endpoints

### OAuth & Setup

- `GET /lazada/setup-ngrok` - Setup ngrok tunnel and get auth URL
- `GET /lazada/auth-url` - Get authorization URL
- `GET /lazada/callback` - OAuth callback handler (automatic)
- `POST /lazada/refresh-token` - Manual token refresh
- `GET /lazada/status` - Check integration status

### Data Retrieval

- `GET /lazada/seller` - Get seller information
- `GET /lazada/products` - Get products with pagination
- `GET /lazada/orders` - Get orders with date filtering

### Data Synchronization

- `POST /lazada/sync/products` - Sync products to database
- `POST /lazada/sync/orders` - Sync orders to database
- `POST /lazada/sync/all` - Complete data synchronization
- `GET /lazada/preview/products` - Preview products without saving
- `GET /lazada/preview/orders` - Preview orders without saving

## Usage Examples

### 1. Complete Setup and First Sync

```bash
# 1. Setup ngrok and authorization
curl -X GET "http://localhost:8000/lazada/setup-ngrok"

# 2. Check status
curl -X GET "http://localhost:8000/lazada/status"

# 3. Sync all data
curl -X POST "http://localhost:8000/lazada/sync/all?products_limit=100&orders_days_back=7"
```

### 2. Regular Data Sync

```bash
# Sync just products
curl -X POST "http://localhost:8000/lazada/sync/products?limit=50"

# Sync recent orders
curl -X POST "http://localhost:8000/lazada/sync/orders?days_back=3"
```

### 3. Preview Data (Without Saving)

```bash
# Preview 10 products
curl -X GET "http://localhost:8000/lazada/preview/products?limit=10"

# Preview last 7 days orders
curl -X GET "http://localhost:8000/lazada/preview/orders?days_back=7"
```

## Configuration

### Environment Variables (.env)

```env
# Lazada API Credentials
LAZADA_APP_KEY=your_app_key
LAZADA_APP_SECRET=your_app_secret

# Tokens (auto-updated)
LAZADA_ACCESS_TOKEN=auto_updated
LAZADA_REFRESH_TOKEN=auto_updated

# Database
SUPABASE_DB_URL=your_supabase_url
SUPABASE_DB_PASSWORD=your_password
```

## Automatic Features

### Token Refresh

- Tokens are checked every 5 minutes
- Auto-refresh when expiring in 10 minutes
- Tokens saved to both `.env` and `lazada_tokens.json`

### Background Services

- Token refresh scheduler starts with the FastAPI app
- ngrok tunnel management
- Error handling and logging

## Data Flow

1. **OAuth Setup**: ngrok tunnel → authorization URL → callback handling
2. **Token Management**: Auto-refresh → save to file → update environment
3. **Data Sync**: Fetch from API → transform to DataFrame → ETL pipeline → database
4. **Continuous Operation**: Background scheduler ensures tokens stay valid

## File Structure

```
app/
├── lazada_service.py         # Main Lazada OAuth service
├── lazada_data_service.py    # Data retrieval and ETL integration
├── token_scheduler.py        # Background token refresh
├── routes.py                 # FastAPI endpoints
└── ...

setup_lazada.py              # Quick setup script
lazada_tokens.json           # Token backup file
.env                         # Configuration
```

## Troubleshooting

### Common Issues

1. **"ngrok not found"**

   ```bash
   pip install pyngrok
   ```

2. **"Invalid app credentials"**

   - Check your `.env` file has correct `LAZADA_APP_KEY` and `LAZADA_APP_SECRET`
   - Verify credentials in Lazada Developer Console

3. **"Token expired"**

   - The system should auto-refresh, but you can manually refresh:

   ```bash
   curl -X POST "http://localhost:8000/lazada/refresh-token"
   ```

4. **"API rate limit"**
   - The system includes delays between requests
   - Reduce batch sizes if needed

### Debug Commands

```bash
# Check current status
curl -X GET "http://localhost:8000/lazada/status"

# Test seller API
curl -X GET "http://localhost:8000/lazada/seller"

# Preview small dataset
curl -X GET "http://localhost:8000/lazada/preview/products?limit=5"
```

## Integration Benefits

- **No Manual Intervention**: Once setup, everything runs automatically
- **Reliable**: Official SDK with proper error handling
- **Scalable**: Background processing and efficient batch operations
- **Secure**: Token management with automatic refresh
- **Integrated**: Works with your existing ETL pipeline and database

## Production Deployment

For production, consider:

- Using a proper reverse proxy instead of ngrok
- Setting up proper logging
- Implementing monitoring for token refresh failures
- Adding rate limiting
- Securing the callback endpoint
