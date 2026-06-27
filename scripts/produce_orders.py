"""
Order event producer — 3-broker cluster
Produces structured mix to orders.raw:
  - Low-value orders (amount <= 500) → expect FILTERED in Pipeline 1
  - High-value orders (amount > 500) → expect ENRICHED in Pipeline 1
  - Bulk orders (multiple items)     → expect EXPANDED in Pipeline 2

Run from kafka-3 (monitoring node):
  python3 scripts/produce_orders.py
"""

import json
import uuid
import random
import time
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = '192.168.100.21:9092,192.168.100.22:9092,192.168.100.23:9092'
TOPIC = 'orders.raw'

producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'enable.idempotence': 'true',
    'acks': 'all',
})

def delivery_callback(err, msg):
    if err:
        print(f"  Delivery failed: {err}")
    else:
        print(f"  Delivered: partition={msg.partition()} offset={msg.offset()}")

PRODUCTS = [
    {'sku': 'ELEC-001', 'name': 'MacBook Pro 16"',
     'unit_price': 2499.99, 'category': 'electronics'},
    {'sku': 'ELEC-002', 'name': 'Dell Monitor 27"',
     'unit_price': 649.99, 'category': 'electronics'},
    {'sku': 'ELEC-003', 'name': 'iPad Pro 12.9"',
     'unit_price': 1099.99, 'category': 'electronics'},
    {'sku': 'ELEC-004', 'name': 'USB-C Cable 2m',
     'unit_price': 19.99, 'category': 'electronics'},
    {'sku': 'ELEC-005', 'name': 'Wireless Mouse',
     'unit_price': 49.99, 'category': 'electronics'},
    {'sku': 'FURN-001', 'name': 'Standing Desk 160cm',
     'unit_price': 899.99, 'category': 'furniture'},
    {'sku': 'FURN-002', 'name': 'Ergonomic Chair',
     'unit_price': 1299.99, 'category': 'furniture'},
    {'sku': 'FURN-003', 'name': 'Monitor Arm',
     'unit_price': 149.99, 'category': 'furniture'},
    {'sku': 'APRL-001', 'name': 'Company Hoodie',
     'unit_price': 65.00, 'category': 'apparel'},
    {'sku': 'APRL-002', 'name': 'Running Shoes',
     'unit_price': 129.99, 'category': 'apparel'},
]

REGIONS = ['us-east', 'us-west', 'eu']

def make_low_value_order():
    cheap = [p for p in PRODUCTS if p['unit_price'] < 150]
    product = random.choice(cheap)
    qty = random.randint(1, 3)
    return {
        'order_id': f'ORD-{uuid.uuid4().hex[:8].upper()}',
        'order_type': 'SINGLE',
        'customer_id': f'CUST-{random.randint(1000,9999)}',
        'category': product['category'],
        'region': random.choice(REGIONS),
        'amount': round(product['unit_price'] * qty, 2),
        'currency': 'USD',
        'status': 'PLACED',
        'placed_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'items': [{'sku': product['sku'], 'product_name': product['name'],
                   'category': product['category'],
                   'quantity': qty, 'unit_price': product['unit_price']}],
    }

def make_high_value_order():
    expensive = [p for p in PRODUCTS if p['unit_price'] > 500]
    product = random.choice(expensive)
    qty = random.randint(1, 2)
    return {
        'order_id': f'ORD-{uuid.uuid4().hex[:8].upper()}',
        'order_type': 'SINGLE',
        'customer_id': f'CUST-{random.randint(1000,9999)}',
        'category': product['category'],
        'region': random.choice(REGIONS),
        'amount': round(product['unit_price'] * qty, 2),
        'currency': 'USD',
        'status': 'PLACED',
        'placed_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'items': [{'sku': product['sku'], 'product_name': product['name'],
                   'category': product['category'],
                   'quantity': qty, 'unit_price': product['unit_price']}],
    }

def make_bulk_order():
    num_items = random.randint(3, 5)
    selected = random.sample(PRODUCTS, num_items)
    items = []
    total = 0.0
    for p in selected:
        qty = random.randint(1, 3)
        items.append({'sku': p['sku'], 'product_name': p['name'],
                      'category': p['category'],
                      'quantity': qty, 'unit_price': p['unit_price']})
        total += p['unit_price'] * qty
    return {
        'order_id': f'BULK-{uuid.uuid4().hex[:8].upper()}',
        'order_type': 'BULK',
        'customer_id': f'CORP-{random.randint(100,999)}',
        'category': 'mixed',
        'region': random.choice(REGIONS),
        'amount': round(total, 2),
        'currency': 'USD',
        'status': 'PLACED',
        'placed_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'items': items,
    }

# Production plan — deliberate sequence for lab observation
plan = [
    ('LOW',  'Low value — expect FILTER OUT in Pipeline 1'),
    ('LOW',  'Low value — expect FILTER OUT in Pipeline 1'),
    ('LOW',  'Low value — expect FILTER OUT in Pipeline 1'),
    ('HIGH', 'High value — expect PASS filter + ENRICH in Pipeline 1'),
    ('HIGH', 'High value — expect PASS filter + ENRICH in Pipeline 1'),
    ('HIGH', 'High value — expect PASS filter + ENRICH in Pipeline 1'),
    ('BULK', 'Bulk — expect FLAT MAP expansion in Pipeline 2'),
    ('BULK', 'Bulk — expect FLAT MAP expansion in Pipeline 2'),
    ('LOW',  'Mixed — low value'),
    ('HIGH', 'Mixed — high value'),
    ('BULK', 'Mixed — bulk'),
    ('LOW',  'Mixed — low value'),
    ('HIGH', 'Mixed — high value'),
]

print("=" * 65)
print(f"ORDERS PRODUCER — 3-broker cluster")
print(f"Bootstrap: {BOOTSTRAP_SERVERS}")
print(f"Topic: {TOPIC}")
print(f"Producing {len(plan)} orders with 1.5s delay between each")
print("Watch worker terminals on kafka-1 and kafka-2")
print("=" * 65)

counts = {'LOW': 0, 'HIGH': 0, 'BULK': 0}

for order_type, description in plan:
    if order_type == 'LOW':
        order = make_low_value_order()
    elif order_type == 'HIGH':
        order = make_high_value_order()
    else:
        order = make_bulk_order()

    counts[order_type] += 1
    items_count = len(order['items'])

    print(f"\n[{order_type}] {order['order_id']}")
    print(f"  amount=${order['amount']:.2f} | "
          f"category={order['category']} | "
          f"region={order['region']} | "
          f"items={items_count}")
    print(f"  expect: {description}")

    producer.produce(
        topic=TOPIC,
        key=order['order_id'].encode('utf-8'),
        value=json.dumps(order).encode('utf-8'),
        callback=delivery_callback,
    )
    producer.poll(0)
    time.sleep(1.5)

producer.flush()
print(f"\n{'='*65}")
print(f"DONE: {len(plan)} orders produced")
print(f"  Low-value : {counts['LOW']} (expect all filtered in Pipeline 1)")
print(f"  High-value: {counts['HIGH']} (expect all enriched in Pipeline 1)")
print(f"  Bulk      : {counts['BULK']} (each expands to N items in Pipeline 2)")
