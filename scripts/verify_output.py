"""
Verification consumer — reads word-count-output from 3-broker cluster.
Run on kafka-3 (monitoring node) while workers run on kafka-1 and kafka-2.
Demonstrates: output topic readable from any broker in the cluster.
"""

import time
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError

# 3-broker bootstrap — connects via whichever broker is available
BOOTSTRAP_SERVERS = '192.168.100.21:9092,192.168.100.22:9092,192.168.100.23:9092'
OUTPUT_TOPIC = 'word-count-output'

consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'wordcount-verifier',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'true',
})

consumer.subscribe([OUTPUT_TOPIC])

word_counts = defaultdict(int)
messages_read = 0

print("WORD COUNT VERIFIER — 3-broker cluster")
print(f"Bootstrap: {BOOTSTRAP_SERVERS}")
print(f"Reading: {OUTPUT_TOPIC}")
print("Ctrl+C for final summary\n")
print("=" * 50)

try:
    while True:
        msg = consumer.poll(timeout=2.0)

        if msg is None:
            if messages_read > 0:
                print(f"\n[{time.strftime('%H:%M:%S')}] Current counts:")
                sorted_counts = sorted(
                    word_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
                for word, count in sorted_counts[:20]:
                    bar = '█' * min(count, 40)
                    print(f"  {word:<20} {count:>5}  {bar}")
                print()
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Error: {msg.error()}")
            continue

        try:
            value = msg.value().decode('utf-8')
            word, count_str = value.split(':', 1)
            count = int(count_str)
            word_counts[word] = count
            messages_read += 1

            # Show which broker partition this came from
            print(f"  [{time.strftime('%H:%M:%S')}] "
                  f"partition={msg.partition()} "
                  f"UPDATE: '{word}' = {count}")

        except (ValueError, AttributeError) as e:
            print(f"Parse error: {e}")

except KeyboardInterrupt:
    print(f"\nFINAL WORD COUNTS:")
    print("=" * 50)
    for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True):
        bar = '█' * min(count, 40)
        print(f"  {word:<20} {count:>5}  {bar}")
    print(f"\nUnique words: {len(word_counts)}")
    print(f"Total updates: {messages_read}")

finally:
    consumer.close()
