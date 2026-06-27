"""
Kafka cluster connection and topic configuration.
3-broker KRaft cluster — all three brokers in bootstrap list.
Single source of truth imported by all topology components.
"""

# All three brokers — client connects to whichever is available
# Standard production pattern: never hardcode single broker
KAFKA_BROKER = '192.168.100.21:9092,192.168.100.22:9092,192.168.100.23:9092'

# Input topic — sentences produced here from any node in cluster
INPUT_TOPIC = 'input-text'

# Output topic — word counts written here after processing
OUTPUT_TOPIC = 'word-count-output'

# Faust app ID = Kafka consumer group ID for the worker fleet
# Also prefix for all internal Faust topics
APP_ID = 'wordcount-processor'

# Faust-managed changelog topic — auto-created, compacted
# Stores current state of word_counts Table
# Replayed on worker restart to restore counts
CHANGELOG_TOPIC = f'{APP_ID}-word-counts-changelog'

# Replication factor — 3 brokers, replicate to all three
# One broker failure = no data loss, processing continues
REPLICATION_FACTOR = 3

# Partitions — 3 matches broker count for even leader distribution
# Also sets max parallelism ceiling for Faust worker fleet
TOPIC_PARTITIONS = 3
