import pandas as pd
import numpy as np

# Load the harmonized fact orders data
fact_orders_file = r"c:\Users\alyss\Desktop\CAPSTONE-LA-Collections\app\Transformed\fact_orders.csv"

try:
    fact_orders_df = pd.read_csv(fact_orders_file)
    print(f"📊 Loaded {len(fact_orders_df)} fact order records")
    
    # Check if the pricing formula now balances
    print("\n🔍 PRICING FORMULA VALIDATION:")
    print("Formula: paid_price + voucher_platform_amount + voucher_seller_amount = original_unit_price")
    
    # Calculate the left side of the equation
    fact_orders_df['calculated_total'] = (
        fact_orders_df['paid_price'] + 
        fact_orders_df['voucher_platform_amount'] + 
        fact_orders_df['voucher_seller_amount']
    )
    
    # Check for discrepancies
    fact_orders_df['price_difference'] = (
        fact_orders_df['calculated_total'] - fact_orders_df['original_unit_price']
    )
    
    # Find records with pricing discrepancies (allowing for small floating point errors)
    tolerance = 0.01
    discrepant_records = fact_orders_df[
        abs(fact_orders_df['price_difference']) > tolerance
    ]
    
    print(f"\n📈 RESULTS:")
    print(f"Total records: {len(fact_orders_df):,}")
    print(f"Records with balanced pricing: {len(fact_orders_df) - len(discrepant_records):,}")
    print(f"Records with pricing discrepancies: {len(discrepant_records):,}")
    print(f"Percentage balanced: {((len(fact_orders_df) - len(discrepant_records)) / len(fact_orders_df)) * 100:.2f}%")
    
    if len(discrepant_records) > 0:
        print(f"\n❌ PRICING DISCREPANCIES FOUND:")
        print(f"Total discrepancy amount: ₱{discrepant_records['price_difference'].sum():,.2f}")
        print(f"Average discrepancy: ₱{discrepant_records['price_difference'].mean():,.2f}")
        print(f"Median discrepancy: ₱{discrepant_records['price_difference'].median():,.2f}")
        print(f"Max discrepancy: ₱{discrepant_records['price_difference'].max():,.2f}")
        print(f"Min discrepancy: ₱{discrepant_records['price_difference'].min():,.2f}")
        
        # Breakdown by platform
        shopee_discrepant = discrepant_records[discrepant_records['order_item_key'].str.startswith('S')]
        lazada_discrepant = discrepant_records[discrepant_records['order_item_key'].str.startswith('L')]
        
        print(f"\nPlatform breakdown of discrepancies:")
        print(f"  Shopee: {len(shopee_discrepant):,} records, ₱{shopee_discrepant['price_difference'].sum():,.2f}")
        print(f"  Lazada: {len(lazada_discrepant):,} records, ₱{lazada_discrepant['price_difference'].sum():,.2f}")
        
        # Show sample discrepant records
        print(f"\nSample discrepant records:")
        sample_cols = ['order_item_key', 'paid_price', 'original_unit_price', 
                      'voucher_platform_amount', 'voucher_seller_amount', 'price_difference']
        print(discrepant_records[sample_cols].head(10).to_string(index=False))
        
    else:
        print(f"\n✅ ALL PRICING FORMULAS BALANCED!")
        print("The updated harmonization successfully fixed the pricing discrepancies.")
        
    # Show overall statistics
    print(f"\n📊 DISCOUNT STATISTICS:")
    print(f"Total voucher_platform_amount: ₱{fact_orders_df['voucher_platform_amount'].sum():,.2f}")
    print(f"Total voucher_seller_amount: ₱{fact_orders_df['voucher_seller_amount'].sum():,.2f}")
    print(f"Total discounts: ₱{(fact_orders_df['voucher_platform_amount'] + fact_orders_df['voucher_seller_amount']).sum():,.2f}")
    
    # Platform breakdown
    shopee_df = fact_orders_df[fact_orders_df['order_item_key'].str.startswith('S')]
    lazada_df = fact_orders_df[fact_orders_df['order_item_key'].str.startswith('L')]
    
    print(f"\nPlatform discount breakdown:")
    print(f"  Shopee platform vouchers: ₱{shopee_df['voucher_platform_amount'].sum():,.2f}")
    print(f"  Shopee seller vouchers: ₱{shopee_df['voucher_seller_amount'].sum():,.2f}")
    print(f"  Lazada platform vouchers: ₱{lazada_df['voucher_platform_amount'].sum():,.2f}")
    print(f"  Lazada seller vouchers: ₱{lazada_df['voucher_seller_amount'].sum():,.2f}")
    
except FileNotFoundError:
    print(f"❌ Could not find fact_orders.csv file")
except Exception as e:
    print(f"❌ Error: {e}")