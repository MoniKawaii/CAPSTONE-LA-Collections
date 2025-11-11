#!/usr/bin/env python3
"""
Check Current Missing Orders Status
Shows the current state of missing orders after harmonization fixes
"""

import psycopg2
import os
from dotenv import load_dotenv

def check_missing_orders():
    load_dotenv()
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        print('=== CURRENT FACT_ORDERS STATUS ===')
        cursor.execute('SELECT COUNT(*) FROM fact_orders')
        fact_count = cursor.fetchone()[0]
        print(f'Total fact_orders records: {fact_count:,}')
        
        print('\n=== ORDERS BY PLATFORM ===')
        cursor.execute('''
            SELECT p.platform_name, COUNT(*) as order_count
            FROM dim_order o
            JOIN dim_platform p ON o.platform_key = p.platform_key
            WHERE o.order_status = 'COMPLETED'
            GROUP BY p.platform_name
            ORDER BY p.platform_name
        ''')
        
        for platform, count in cursor.fetchall():
            print(f'{platform}: {count:,} COMPLETED orders')
        
        print('\n=== FACT_ORDERS BY PLATFORM ===')
        cursor.execute('''
            SELECT p.platform_name, COUNT(*) as fact_count
            FROM fact_orders f
            JOIN dim_platform p ON f.platform_key = p.platform_key
            GROUP BY p.platform_name
            ORDER BY p.platform_name
        ''')
        
        for platform, count in cursor.fetchall():
            print(f'{platform}: {count:,} fact_orders records')
        
        print('\n=== MISSING ORDERS ANALYSIS ===')
        cursor.execute('''
            WITH completed_orders AS (
                SELECT platform_key, COUNT(*) as total_completed
                FROM dim_order 
                WHERE order_status = 'COMPLETED'
                GROUP BY platform_key
            ),
            fact_orders_count AS (
                SELECT platform_key, COUNT(*) as total_fact
                FROM fact_orders
                GROUP BY platform_key
            )
            SELECT 
                p.platform_name,
                co.total_completed,
                COALESCE(fo.total_fact, 0) as total_fact,
                (co.total_completed - COALESCE(fo.total_fact, 0)) as missing_orders
            FROM completed_orders co
            JOIN dim_platform p ON co.platform_key = p.platform_key
            LEFT JOIN fact_orders_count fo ON co.platform_key = fo.platform_key
            ORDER BY p.platform_name
        ''')
        
        total_missing = 0
        for platform, completed, fact, missing in cursor.fetchall():
            coverage = (fact / completed * 100) if completed > 0 else 0
            print(f'{platform}: {missing:,} missing orders ({coverage:.1f}% coverage)')
            total_missing += missing
        
        print(f'\nTOTAL MISSING ORDERS: {total_missing:,}')
        
        if total_missing > 0:
            print('\n=== SAMPLE MISSING ORDERS ===')
            cursor.execute('''
                SELECT DISTINCT
                    o.platform_order_id,
                    p.platform_name,
                    o.order_status,
                    o.order_date,
                    o.price_total
                FROM dim_order o
                JOIN dim_platform p ON o.platform_key = p.platform_key
                LEFT JOIN fact_orders f ON o.orders_key = f.orders_key
                WHERE o.order_status = 'COMPLETED' 
                AND f.orders_key IS NULL
                ORDER BY o.order_date DESC
                LIMIT 10
            ''')
            
            print('Sample missing orders:')
            for order_id, platform, status, date, total in cursor.fetchall():
                print(f'  {platform}: {order_id} | {status} | {date} | ${total:.2f}')
        
        conn.close()
        
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    check_missing_orders()