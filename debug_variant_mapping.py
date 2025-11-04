import pandas as pd

# Check current fact_orders variant coverage
print("=== FACT ORDERS VARIANT ANALYSIS ===")
fact_df = pd.read_csv('app/Transformed/fact_orders.csv')
print(f"Total fact_orders records: {len(fact_df)}")

# Check variant key distribution
print(f"\nVariant key distribution:")
print(fact_df['product_variant_key'].value_counts().head(10))
print(f"Zero variant keys: {(fact_df['product_variant_key'] == 0.0).sum()}")
print(f"Non-zero variant keys: {(fact_df['product_variant_key'] != 0.0).sum()}")

# Check product coverage
print(f"\nProduct key distribution:")
print(fact_df['product_key'].value_counts().head(10))

# Load variant dimension to check what's available
print(f"\n=== VARIANT DIMENSION ANALYSIS ===")
variant_df = pd.read_csv('app/Transformed/dim_product_variant.csv')
print(f"Total variants available: {len(variant_df)}")

# Check DEFAULT variants
default_variants = variant_df[variant_df['platform_sku_id'].str.startswith('DEFAULT_', na=False)]
print(f"DEFAULT variants: {len(default_variants)}")

# Check which products have DEFAULT variants
print(f"\nProducts with DEFAULT variants:")
print(default_variants[['product_variant_key', 'product_key', 'platform_sku_id']].head(10))

# Cross-reference: Which products in fact_orders have 0.0 variants?
zero_variant_records = fact_df[fact_df['product_variant_key'] == 0.0]
print(f"\n=== ZERO VARIANT ANALYSIS ===")
print(f"Records with 0.0 variant key: {len(zero_variant_records)}")

if len(zero_variant_records) > 0:
    print("Sample records with 0.0 variant keys:")
    print(zero_variant_records[['order_item_key', 'product_key', 'product_variant_key', 'platform_key']].head(10))
    
    # Check which product_keys are affected
    affected_products = zero_variant_records['product_key'].unique()
    print(f"\nAffected product_keys: {affected_products[:10]}")
    
    # Check if these products have DEFAULT variants available
    print(f"\nChecking if DEFAULT variants exist for affected products:")
    for prod_key in affected_products[:5]:
        matching_defaults = default_variants[default_variants['product_key'] == prod_key]
        print(f"Product {prod_key}: {len(matching_defaults)} DEFAULT variants available")
        if len(matching_defaults) > 0:
            print(f"  Available: {matching_defaults['product_variant_key'].tolist()}")