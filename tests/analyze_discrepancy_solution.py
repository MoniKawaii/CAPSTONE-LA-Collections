import pandas as pd

# Let's analyze the discrepant records in more detail
fact_orders_file = r"c:\Users\alyss\Desktop\CAPSTONE-LA-Collections\app\Transformed\fact_orders.csv"

try:
    fact_orders_df = pd.read_csv(fact_orders_file)
    print(f"📊 Loaded {len(fact_orders_df)} fact order records")
    
    # Calculate pricing discrepancies
    fact_orders_df['calculated_total'] = (
        fact_orders_df['paid_price'] + 
        fact_orders_df['voucher_platform_amount'] + 
        fact_orders_df['voucher_seller_amount']
    )
    
    fact_orders_df['price_difference'] = (
        fact_orders_df['calculated_total'] - fact_orders_df['original_unit_price']
    )
    
    tolerance = 0.01
    discrepant_records = fact_orders_df[
        abs(fact_orders_df['price_difference']) > tolerance
    ]
    
    print(f"\n🔍 DISCREPANCY ANALYSIS:")
    print(f"Total discrepant records: {len(discrepant_records):,}")
    print(f"Total discrepancy amount: ₱{discrepant_records['price_difference'].sum():,.2f}")
    
    # Check if these are records without payment details (voucher amounts are zero)
    no_voucher_records = discrepant_records[
        (discrepant_records['voucher_platform_amount'] == 0) & 
        (discrepant_records['voucher_seller_amount'] == 0)
    ]
    
    print(f"\nRecords without voucher data (fallback to basic order): {len(no_voucher_records):,}")
    print(f"Discrepancy from no-voucher records: ₱{no_voucher_records['price_difference'].sum():,.2f}")
    
    # Analyze the fallback data pattern
    print(f"\n📋 FALLBACK RECORDS ANALYSIS:")
    print(f"Date range: {no_voucher_records['time_key'].min()} to {no_voucher_records['time_key'].max()}")
    print(f"Average discrepancy per record: ₱{no_voucher_records['price_difference'].mean():,.2f}")
    
    # Calculate what percentage this represents
    total_shopee_revenue = fact_orders_df[
        fact_orders_df['order_item_key'].str.startswith('S')
    ]['paid_price'].sum()
    
    discrepancy_percentage = abs(discrepant_records['price_difference'].sum()) / total_shopee_revenue * 100
    
    print(f"\n📈 IMPACT ASSESSMENT:")
    print(f"Total Shopee revenue: ₱{total_shopee_revenue:,.2f}")
    print(f"Discrepancy as % of Shopee revenue: {discrepancy_percentage:.3f}%")
    
    # Check if we can calculate the missing voucher amount
    if len(no_voucher_records) > 0:
        print(f"\n🔧 POTENTIAL SOLUTION:")
        print(f"For {len(no_voucher_records):,} records without payment details:")
        print(f"- We could add missing discount amount to voucher_platform_amount")
        print(f"- This would balance the pricing formula automatically")
        
        # Show impact on voucher statistics
        current_shopee_platform_vouchers = fact_orders_df[
            fact_orders_df['order_item_key'].str.startswith('S')
        ]['voucher_platform_amount'].sum()
        
        missing_amount = abs(no_voucher_records['price_difference'].sum())
        new_platform_vouchers = current_shopee_platform_vouchers + missing_amount
        
        print(f"- Current Shopee platform vouchers: ₱{current_shopee_platform_vouchers:,.2f}")
        print(f"- After adding missing discounts: ₱{new_platform_vouchers:,.2f}")
        print(f"- Increase: ₱{missing_amount:,.2f} ({(missing_amount/current_shopee_platform_vouchers)*100:.2f}%)")

except Exception as e:
    print(f"❌ Error: {e}")