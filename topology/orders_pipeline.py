"""
Orders Stream Processing — Stateless Operations
confluent-kafka implementation — Kafka 3.7 + Python 3.12 compatible

Three stateless operations:

PIPELINE 1 — Filter + Enrich:
  orders.raw → [FILTER amount > 500] → [MAP/ENRICH] → orders.high-value

PIPELINE 2 — Flat Map:
  orders.raw → [FLAT_MAP 1 order → N line items] → orders.line-items

Stateless: no Tables, no state stores, no changelog topics.
Each record evaluated in complete isolation.
"""

import json
import time

FILTER_THRESHOLD = 500.00

PROCESSING_TIERS = [
    (5000.0, 'PLATINUM'),
    (2000.0, 'GOLD'),
    (1000.0, 'SILVER'),
    (500.0,  'STANDARD'),
]

FULFILLMENT_ROUTING = {
    ('electronics', 'us-east'): 'FC-NJ-01',
    ('electronics', 'us-west'): 'FC-CA-01',
    ('electronics', 'eu'):      'FC-DE-01',
    ('furniture',   'us-east'): 'FC-PA-01',
    ('furniture',   'us-west'): 'FC-WA-01',
    ('apparel',     'us-east'): 'FC-NY-01',
    ('apparel',     'us-west'): 'FC-CA-02',
}

def determine_tier(amount: float) -> str:
    """Stateless: same amount always returns same tier."""
    for threshold, tier in PROCESSING_TIERS:
        if amount > threshold:
            return tier
    return 'STANDARD'

def determine_fc(category: str, region: str) -> str:
    """Stateless: same category+region always returns same FC."""
    return FULFILLMENT_ROUTING.get(
        (category.lower(), region.lower()),
        'FC-DEFAULT-01'
    )

def apply_filter(order: dict) -> bool:
    """
    FILTER operation — stateless.
    Returns True if order should pass downstream.
    Returns False if order should be dropped.
    No state: each order evaluated in isolation.
    """
    return float(order.get('amount', 0)) > FILTER_THRESHOLD

def apply_enrich(order: dict) -> dict:
    """
    MAP/ENRICH operation — stateless.
    Takes one order dict, returns one enriched order dict.
    Same input always produces same output.
    Added fields: priority_flag, processing_tier,
                  fulfillment_center, high_value_since, pipeline
    """
    amount = float(order.get('amount', 0))
    category = order.get('category', 'unknown')
    region = order.get('region', 'us-east')

    return {
        **order,
        'priority_flag': 'HIGH',
        'processing_tier': determine_tier(amount),
        'fulfillment_center': determine_fc(category, region),
        'high_value_since': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'original_amount': amount,
        'pipeline': 'filter-enrich-v1',
    }

def apply_flat_map(order: dict) -> list:
    """
    FLAT_MAP operation — stateless.
    Takes one order dict, returns list of line item dicts.
    One input → zero or more outputs.
    Empty items array → empty list (zero outputs) — valid case.
    """
    order_id = order.get('order_id', 'UNKNOWN')
    customer_id = order.get('customer_id', 'UNKNOWN')
    region = order.get('region', 'us-east')
    items = order.get('items', [])

    line_items = []
    for position, item in enumerate(items, start=1):
        sku = item.get('sku', 'UNKNOWN-SKU')
        quantity = item.get('quantity', 1)
        unit_price = float(item.get('unit_price', 0))
        category = item.get('category', order.get('category', 'unknown'))

        line_items.append({
            'parent_order_id': order_id,
            'customer_id': customer_id,
            'order_region': region,
            'order_total': order.get('amount', 0),
            'line_item_id': f'{order_id}-ITEM-{position:03d}',
            'position': position,
            'sku': sku,
            'product_name': item.get('product_name', ''),
            'category': category,
            'quantity': quantity,
            'unit_price': unit_price,
            'line_total': round(quantity * unit_price, 2),
            'assigned_warehouse': determine_fc(category, region),
            'pick_pack_status': 'PENDING',
            'expanded_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'pipeline': 'flat-map-expand-v1',
        })

    return line_items
