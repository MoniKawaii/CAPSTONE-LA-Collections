#!/usr/bin/env python3

import sys
sys.path.append('C:/Users/alyss/Desktop/CAPSTONE-LA-Collections')
import pandas as pd
import json
from collections import Counter

print("🔍 Analyzing Lazada order statuses array...")

# Load raw Lazada order data
with open('app/Staging/lazada_orders_raw.json', 'r', encoding='utf-8') as f:
    lazada_orders = json.load(f)

print(f"📊 Total raw Lazada orders: {len(lazada_orders)}")

# Check the statuses array structure
statuses_sample = []
for i, order in enumerate(lazada_orders[:10]):
    statuses = order.get('statuses', [])
    print(f"\n🔍 Order {i+1}:")
    print(f"   - order_id: {order.get('order_id')}")
    print(f"   - order_number: {order.get('order_number')}")
    print(f"   - statuses: {statuses}")
    
    if statuses and len(statuses) > 0:
        first_status = statuses[0]
        print(f"   - first_status type: {type(first_status)}")
        print(f"   - first_status content: {first_status}")
        
        if isinstance(first_status, dict):
            status_value = first_status.get('status', '')
            print(f"   - extracted status: '{status_value}'")
        else:
            print(f"   - extracted status: '{first_status}'")
        statuses_sample.append(first_status)

# Count all unique status patterns in the entire dataset
print(f"\n📊 Analyzing all statuses patterns...")
all_status_types = []
all_extracted_statuses = []

for order in lazada_orders:
    statuses = order.get('statuses', [])
    if statuses and len(statuses) > 0:
        first_status = statuses[0]
        all_status_types.append(type(first_status).__name__)
        
        if isinstance(first_status, dict):
            status_value = first_status.get('status', '')
            all_extracted_statuses.append(status_value)
        else:
            all_extracted_statuses.append(str(first_status))
    else:
        all_extracted_statuses.append('')

print(f"\n📊 Status type distribution:")
type_counts = Counter(all_status_types)
for status_type, count in type_counts.most_common():
    print(f"   - {status_type}: {count}")

print(f"\n📊 Extracted status distribution:")
status_counts = Counter(all_extracted_statuses)
for status, count in status_counts.most_common(10):
    print(f"   - '{status}': {count}")

# Check what should be considered "completed" orders
print(f"\n🔍 Checking for orders that might be considered completed...")

# Let's check if any orders have completed-like statuses
completed_like = ['COMPLETED', 'DELIVERED', 'FINISHED', 'SUCCESS', 'DONE', 'delivered', 'completed']
found_completed = False
for status in completed_like:
    if status in status_counts:
        print(f"   ✅ Found '{status}': {status_counts[status]} orders")
        found_completed = True

if not found_completed:
    print("   ❌ No standard 'completed' statuses found!")
    print("   📊 Top statuses to investigate:")
    for status, count in list(status_counts.most_common(5)):
        if status:  # Skip empty statuses
            print(f"      - '{status}': {count} orders")

print(f"\n🔍 This explains why no orders are being processed for fact_orders!")
print(f"   - Fact orders processing only includes 'COMPLETED' status orders")
print(f"   - But Lazada orders have different status values")
print(f"   - Need to update status mapping to include valid Lazada statuses")