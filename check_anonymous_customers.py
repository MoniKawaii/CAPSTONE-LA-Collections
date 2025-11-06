#!/usr/bin/env python3

import sys
sys.path.append('C:/Users/alyss/Desktop/CAPSTONE-LA-Collections')
import pandas as pd

print("🔍 Checking for anonymous customers in dim_customer...")

# Load customer dimension
dim_customer_df = pd.read_csv('app/Transformed/dim_customer.csv')

# Check for anonymous customers
anonymous_customers = dim_customer_df[dim_customer_df['buyer_segment'] == 'Anonymous']
print(f"📊 Anonymous customers found: {len(anonymous_customers)}")

if len(anonymous_customers) > 0:
    print(f"\n🔍 Anonymous customer details:")
    for _, customer in anonymous_customers.iterrows():
        platform_name = "Lazada" if customer['platform_key'] == 1 else "Shopee"
        print(f"   - {platform_name}: customer_key={customer['customer_key']}, platform_customer_id='{customer['platform_customer_id']}'")

# Check Lazada customers specifically
lazada_customers = dim_customer_df[dim_customer_df['platform_key'] == 1]
anonymous_lazada = lazada_customers[lazada_customers['buyer_segment'] == 'Anonymous']

print(f"\n📊 Lazada customer analysis:")
print(f"   - Total Lazada customers: {len(lazada_customers)}")
print(f"   - Anonymous Lazada customers: {len(anonymous_lazada)}")

if len(anonymous_lazada) > 0:
    lazada_anon = anonymous_lazada.iloc[0]
    print(f"   ✅ Anonymous Lazada customer exists:")
    print(f"     - customer_key: {lazada_anon['customer_key']}")
    print(f"     - platform_customer_id: '{lazada_anon['platform_customer_id']}'")
else:
    print(f"   ❌ No anonymous Lazada customer found!")

# Check Shopee anonymous customer for comparison
shopee_customers = dim_customer_df[dim_customer_df['platform_key'] == 2]
anonymous_shopee = shopee_customers[shopee_customers['buyer_segment'] == 'Anonymous']

print(f"\n📊 Shopee customer analysis:")
print(f"   - Total Shopee customers: {len(shopee_customers)}")
print(f"   - Anonymous Shopee customers: {len(anonymous_shopee)}")

if len(anonymous_shopee) > 0:
    shopee_anon = anonymous_shopee.iloc[0]
    print(f"   ✅ Anonymous Shopee customer exists:")
    print(f"     - customer_key: {shopee_anon['customer_key']}")
    print(f"     - platform_customer_id: '{shopee_anon['platform_customer_id']}'")

print(f"\n💡 Analysis:")
if len(anonymous_lazada) > 0 and len(anonymous_shopee) > 0:
    print(f"   ✅ Both platforms have anonymous customers - harmonization should work!")
elif len(anonymous_lazada) == 0:
    print(f"   ❌ Missing anonymous Lazada customer - need to add one!")
elif len(anonymous_shopee) == 0:
    print(f"   ❌ Missing anonymous Shopee customer - need to add one!")
else:
    print(f"   ❌ Missing anonymous customers for both platforms!")