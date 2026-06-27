"""
Orders Pipeline Worker — confluent-kafka
3-broker cluster: run on kafka-1 AND kafka-2 for distributed processing

kafka-1: python3 app_orders.py
kafka-2: python3 app_orders.py

Both join consumer group 'orders-processor'.
3 partitions of orders.raw split between 2 workers.
Worker on kafka-1 processes partitions from broker leaders on cluster.
Worker on kafka-2 processes its partitions independently.

Two pipelines run in same process:
  Pipeline 1: filter + enrich → orders.high-value
  Pipeline 2: flat_map → orders.line-items
Both consume orders.raw, process independently.
"""

import json
import signal
import sys
from confluent_kafka import Consumer, Producer, KafkaError,  KafkaException
from topology.orders_pipeline import apply_filter, apply_enrich, apply_flat_map
from config.kafka_config_updated import (
    KAFKA_BROKER,
    ORDERS_RAW_TOPIC,
    ORDERS_HIGH_VALUE_TOPIC,
    ORDERS_LINE_ITEMS_TOPIC,
    ORDERS_APP_ID,
)
shutdown_flag = False
messages_processed = False

def handle_shutdown(signum, frame):
    global shutdown_flag
    print("\n[ORDERS-WORKER] Shutdown signal received.")
    shutdown_flag = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def delivery_callback(err, msg):
    if err:
        print(f"[OUTPUT ERROR] topic={msg.topic()} err={err}")

def on_assign(consumer, partitions):
    nums = [p.partition for p in partitions]
    print(f"\n[ORDERS-WORKER] PARTITIONS ASSIGNED: {nums}")
    print(f"[ORDERS-WORKER] Owns {len(partitions)} partition(s) of {ORDERS_RAW_TOPIC}")

def on_revoke(consumer, partitions):
    global messages_processed

    nums = [p.partition for p in partitions]

    print(f"\n[ORDERS-WORKER] PARTITIONS REVOKED: {nums}")

    # No offsets yet? Nothing to commit.
    if not messages_processed:
        print("[ORDERS-WORKER] No offsets stored — skipping commit")
        return

    try:
        consumer.commit(asynchronous=False)
        print("[ORDERS-WORKER] Offsets committed successfully")

    except KafkaException as e:
        err = e.args[0]

        if err.code() != KafkaError._NO_OFFSET:
            print(f"[ORDERS-WORKER] Commit error on revoke: {err}")
def on_lost(consumer, partitions):
    nums = [p.partition for p in partitions]
    print(f"\n[ORDERS-WORKER] PARTITIONS LOST (unclean): {nums}")

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': ORDERS_APP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false',
    'session.timeout.ms': '10000',
    'heartbeat.interval.ms': '3000',
    'partition.assignment.strategy': 'roundrobin',
})

producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',
    'enable.idempotence': 'true',
})

consumer.subscribe(
    [ORDERS_RAW_TOPIC],
    on_assign=on_assign,
    on_revoke=on_revoke,
    on_lost=on_lost,
)

print("[ORDERS-WORKER] Orders Pipeline — Filter + Enrich + Flat Map")
print(f"[ORDERS-WORKER] Bootstrap : {KAFKA_BROKER}")
print(f"[ORDERS-WORKER] Group     : {ORDERS_APP_ID}")
print(f"[ORDERS-WORKER] Input     : {ORDERS_RAW_TOPIC}")
print(f"[ORDERS-WORKER] Output-1  : {ORDERS_HIGH_VALUE_TOPIC} (filter + enrich)")
print(f"[ORDERS-WORKER] Output-2  : {ORDERS_LINE_ITEMS_TOPIC} (flat map)")
print("[ORDERS-WORKER] Waiting for partition assignment...")
print("=" * 65)

pipeline1_passed = 0
pipeline1_filtered = 0
pipeline2_items_produced = 0

try:
    while not shutdown_flag:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[ERROR] {msg.error()}")
            continue
        try:
            order = json.loads(msg.value().decode('utf-8'))

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[PARSE ERROR] partition={msg.partition()} offset={msg.offset()} err={e}")
            consumer.commit(asynchronous=False)
            continue

        messages_processed = True
        order_id = order.get('order_id', 'UNKNOWN')
        amount = float(order.get('amount', 0))

        print(f"\n[RAW] partition={msg.partition()} offset={msg.offset()} "
              f"order={order_id} amount=${amount:.2f}")

        # ────────────────────────────────────────────────────
        # PIPELINE 1: FILTER + ENRICH
        # Stateless: evaluate this record only, no state needed
        # ────────────────────────────────────────────────────
        if apply_filter(order):
            enriched = apply_enrich(order)
            tier = enriched['processing_tier']
            fc = enriched['fulfillment_center']

            print(f"[P1-FILTER]  PASSED  amount=${amount:.2f} > ${500:.2f}")
            print(f"[P1-ENRICH]  tier={tier} fc={fc} priority=HIGH")

            producer.produce(
                topic=ORDERS_HIGH_VALUE_TOPIC,
                key=order_id.encode('utf-8'),
                value=json.dumps(enriched).encode('utf-8'),
                callback=delivery_callback,
            )
            pipeline1_passed += 1
        else:
            print(f"[P1-FILTER]  DROPPED amount=${amount:.2f} <= ${500:.2f}")
            pipeline1_filtered += 1

        # ────────────────────────────────────────────────────
        # PIPELINE 2: FLAT MAP
        # Stateless: expand items array → individual events
        # Runs on ALL orders — independent of Pipeline 1 result
        # ────────────────────────────────────────────────────
        line_items = apply_flat_map(order)

        if not line_items:
            print(f"[P2-FLATMAP] order={order_id} has no items — 0 events produced")
        else:
            print(f"[P2-FLATMAP] order={order_id} "
                  f"items={len(line_items)} → {len(line_items)} events")
            for item in line_items:
                print(f"  → {item['line_item_id']} "
                      f"sku={item['sku']} "
                      f"qty={item['quantity']} "
                      f"wh={item['assigned_warehouse']}")
                producer.produce(
                    topic=ORDERS_LINE_ITEMS_TOPIC,
                    key=item['line_item_id'].encode('utf-8'),
                    value=json.dumps(item).encode('utf-8'),
                    callback=delivery_callback,
                )
                pipeline2_items_produced += 1

        producer.poll(0)
        consumer.commit(asynchronous=False)
finally:
    producer.flush()

    try:
        consumer.unsubscribe()
    except Exception:
        pass

    try:
        consumer.close()
    except KafkaException:
        pass

    print(f"\n[ORDERS-WORKER] Stopped cleanly.")
    print(f"[ORDERS-WORKER] Pipeline 1 — passed filter : {pipeline1_passed}")
    print(f"[ORDERS-WORKER] Pipeline 1 — filtered out  : {pipeline1_filtered}")
    print(f"[ORDERS-WORKER] Pipeline 2 — line items    : {pipeline2_items_produced}")
