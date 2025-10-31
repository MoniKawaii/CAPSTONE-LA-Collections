# Lazada Tokens Directory

This directory stores Lazada API authentication tokens.

## File Structure

- `lazada_tokens.json` - Contains Lazada OAuth tokens (auto-generated)

## Security Notice

🔒 **IMPORTANT**: This directory contains sensitive authentication tokens. The `lazada_tokens.json` file is automatically ignored by git (see `.gitignore`).

## How to Get Tokens

Run the token generation script from the project root:

```bash
python3 get_lazada_tokens.py
```

Choose option 1 to generate new tokens through OAuth flow.

## Token File Format

```json
{
  "access_token": "your_access_token_here",
  "refresh_token": "your_refresh_token_here",
  "expires_in": 2592000,
  "refresh_expires_in": 15552000,
  "created_at": 1234567890,
  "account_platform": "seller_center",
  "country_user_info": []
}
```

## Token Lifecycle

- **Access Token**: Valid for 30 days (2,592,000 seconds)
- **Refresh Token**: Valid for 180 days (15,552,000 seconds)
- Tokens are automatically refreshed by the system when expired

## Troubleshooting

If you see "Token file not found":
1. Run `python3 get_lazada_tokens.py` 
2. Choose option 1 to generate new tokens
3. Follow the OAuth authorization flow
4. Tokens will be saved to this directory automatically
