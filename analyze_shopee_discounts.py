import pandas as pd

def analyze_shopee_discounts():
    """Analyze Shopee discount calculations in fact_orders"""
    df = pd.read_csv('app/Transformed/fact_orders.csv')
    
    # Focus on Shopee records
    shopee_df = df[df['platform_key'] == 2].copy()
    print('=== SHOPEE DISCOUNT ANALYSIS ===')
    print(f'Total Shopee records: {len(shopee_df)}')
    print()
    
    # Check for discounts
    shopee_df['has_discount'] = shopee_df['paid_price'] < shopee_df['original_unit_price']
    print(f'Records with paid_price < original_unit_price: {shopee_df["has_discount"].sum()}')
    print(f'Records with paid_price = original_unit_price: {(shopee_df["paid_price"] == shopee_df["original_unit_price"]).sum()}')
    print()
    
    # Show sample records with potential discounts
    print('Sample records where paid_price != original_unit_price:')
    different_prices = shopee_df[shopee_df['paid_price'] != shopee_df['original_unit_price']]
    if len(different_prices) > 0:
        print(different_prices[['order_item_key', 'paid_price', 'original_unit_price', 'voucher_platform_amount', 'voucher_seller_amount']].head(10))
    else:
        print('No records found with different prices!')
        
    print()
    print('Records with vouchers but paid_price = original_unit_price:')
    voucher_but_same_price = shopee_df[
        ((shopee_df['voucher_platform_amount'] > 0) | (shopee_df['voucher_seller_amount'] > 0)) &
        (shopee_df['paid_price'] == shopee_df['original_unit_price'])
    ]
    print(f'Count: {len(voucher_but_same_price)}')
    if len(voucher_but_same_price) > 0:
        print(voucher_but_same_price[['order_item_key', 'paid_price', 'original_unit_price', 'voucher_platform_amount', 'voucher_seller_amount']].head(5))
    
    print()
    print('Sample of first 10 Shopee records:')
    print(shopee_df[['order_item_key', 'paid_price', 'original_unit_price', 'voucher_platform_amount', 'voucher_seller_amount']].head(10))
    
    print()
    print('Summary of voucher amounts:')
    print(f'Records with platform voucher > 0: {(shopee_df["voucher_platform_amount"] > 0).sum()}')
    print(f'Records with seller voucher > 0: {(shopee_df["voucher_seller_amount"] > 0).sum()}')
    print(f'Max platform voucher: {shopee_df["voucher_platform_amount"].max()}')
    print(f'Max seller voucher: {shopee_df["voucher_seller_amount"].max()}')

if __name__ == "__main__":
    analyze_shopee_discounts()