"""
Kafka cluster connection and topic configuration.
3-broker KRaft cluster — all brokers in bootstrap list.
"""

# All 3 brokers — client connects via whichever is available
KAFKA_BROKER = '192.168.100.21:9092,192.168.100.22:9092,192.168.100.23:9092'

# ── Lab 13: Word Count Pipeline ──────────────────────────────
INPUT_TOPIC = 'input-text'
OUTPUT_TOPIC = 'word-count-output'
APP_ID = 'wordcount-processor'

# ── Lab 14: Orders Pipeline ───────────────────────────────────

# Raw orders — all orders from upstream order service
# RF=3: survives any single broker failure
ORDERS_RAW_TOPIC = 'orders.raw'

# High-value orders after filter + enrichment
# Only orders amount > 500 reach this topic
ORDERS_HIGH_VALUE_TOPIC = 'orders.high-value'

# Line items — output of flat_map
# One bulk order with N items → N line item events
ORDERS_LINE_ITEMS_TOPIC = 'orders.line-items'

# Consumer group ID for orders pipeline worker
ORDERS_APP_ID = 'orders-processor'

# Cluster topology
TOPIC_PARTITIONS = 3
REPLICATION_FACTOR = 3
