"""
Verification consumer — reads both output topics, validates results.
Run on kafka-3 (monitoring node).
Bootstrap hits all 3 brokers — works if any single broker is down.
"""

import json
import time
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = '192.168.100.21:9092,192.168.100.22:9092,192.168.100.23:9092'

def make_consumer(group_id):
    return Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'true',
    })

def verify_high_value(timeout=8.0):
    consumer = make_consumer('lab14-verify-hv')
    consumer.subscribe(['orders.high-value'])

    print("\n" + "=" * 65)
    print("VERIFYING: orders.high-value")
    print("Expected: amount > $500, priority_flag=HIGH, enrichment fields present")
    print("=" * 65)

    records = []
    violations = []
    empty_polls = 0

    try:
        while empty_polls < 3:
            msg = consumer.poll(timeout=timeout)
            if msg is None:
                if records:
                    empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {msg.error()}")
                continue

            empty_polls = 0
            try:
                order = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                print(f"Parse error: {e}")
                continue

            records.append(order)
            order_id = order.get('order_id', 'UNKNOWN')
            amount = float(order.get('amount', 0))
            priority = order.get('priority_flag')
            tier = order.get('processing_tier')
            fc = order.get('fulfillment_center')
            pipeline = order.get('pipeline')

            issues = []
            if amount <= 500:
                issues.append(f"amount ${amount:.2f} <= $500 — filter failed")
            if priority != 'HIGH':
                issues.append(f"priority_flag={priority} expected HIGH")
            if tier not in ['STANDARD', 'SILVER', 'GOLD', 'PLATINUM']:
                issues.append(f"processing_tier={tier} unexpected")
            if not fc:
                issues.append("fulfillment_center missing")
            if pipeline != 'filter-enrich-v1':
                issues.append(f"pipeline={pipeline} unexpected")

            status = "OK" if not issues else "FAIL"
            print(f"\n  [{status}] {order_id} "
                  f"partition={msg.partition()} offset={msg.offset()}")
            print(f"    amount=${amount:.2f} | tier={tier} | "
                  f"fc={fc} | priority={priority}")
            if issues:
                violations.append({'order_id': order_id, 'issues': issues})
                for i in issues:
                    print(f"    ISSUE: {i}")

    finally:
        consumer.close()

    print(f"\n{'─'*65}")
    print(f"Records received : {len(records)}")
    print(f"Violations       : {len(violations)}")
    if not violations:
        print("RESULT: PASS — filter and enrichment correct")
    else:
        print("RESULT: FAIL — see issues above")
    return len(records), len(violations)

def verify_line_items(timeout=8.0):
    consumer = make_consumer('lab14-verify-li')
    consumer.subscribe(['orders.line-items'])

    print("\n" + "=" * 65)
    print("VERIFYING: orders.line-items")
    print("Expected: one event per line item, parent_order_id present")
    print("=" * 65)

    records = []
    violations = []
    by_parent = defaultdict(list)
    empty_polls = 0

    try:
        while empty_polls < 3:
            msg = consumer.poll(timeout=timeout)
            if msg is None:
                if records:
                    empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {msg.error()}")
                continue

            empty_polls = 0
            try:
                item = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                print(f"Parse error: {e}")
                continue

            records.append(item)
            item_id = item.get('line_item_id', 'UNKNOWN')
            parent_id = item.get('parent_order_id', 'UNKNOWN')
            sku = item.get('sku', 'UNKNOWN')
            qty = item.get('quantity', 0)
            line_total = item.get('line_total', 0)
            warehouse = item.get('assigned_warehouse', '')
            position = item.get('position')
            by_parent[parent_id].append(item_id)

            issues = []
            if not parent_id or parent_id == 'UNKNOWN':
                issues.append("parent_order_id missing")
            if not sku or sku == 'UNKNOWN-SKU':
                issues.append("sku missing")
            if not warehouse:
                issues.append("assigned_warehouse missing")
            if position is None:
                issues.append("position missing")

            status = "OK" if not issues else "FAIL"
            print(f"  [{status}] {item_id} "
                  f"parent={parent_id} sku={sku} "
                  f"qty={qty} total=${line_total:.2f} wh={warehouse}")
            if issues:
                violations.append({'item_id': item_id, 'issues': issues})

    finally:
        consumer.close()

    print(f"\n{'─'*65}")
    print(f"Line item events : {len(records)}")
    print(f"Unique parents   : {len(by_parent)}")
    print(f"Violations       : {len(violations)}")
    print(f"\nItems per parent order:")
    for parent_id, items in sorted(by_parent.items()):
        print(f"  {parent_id}: {len(items)} item(s)")
    if not violations:
        print("\nRESULT: PASS — flat_map correct")
    else:
        print("\nRESULT: FAIL — see issues above")
    return len(records), len(violations)

if __name__ == '__main__':
    print("LAB 14 VERIFIER — 3-broker cluster")
    print(f"Bootstrap: {BOOTSTRAP_SERVERS}")

    hv_count, hv_violations = verify_high_value()
    li_count, li_violations = verify_line_items()

    print("\n" + "=" * 65)
    print("FINAL SUMMARY")
    print("=" * 65)
    print(f"  orders.high-value : {hv_count} records | {hv_violations} violations")
    print(f"  orders.line-items : {li_count} records | {li_violations} violations")
    if hv_violations == 0 and li_violations == 0:
        print("\n  ALL PIPELINES VALIDATED SUCCESSFULLY")
    else:
        print("\n  FAILURES DETECTED — review above")
