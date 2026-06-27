"""
Word Count Stream Processor
confluent-kafka — works with Kafka 3.7 KRaft + Python 3.12
"""

import json
import signal
import sys
from confluent_kafka import Consumer, Producer, KafkaError
from topology.wordcount import normalize, split_to_words
from config.kafka_config import (
    KAFKA_BROKER,
    INPUT_TOPIC,
    OUTPUT_TOPIC,
    APP_ID,
)

# ----------------------------------------------------------------
word_counts = {}
shutdown_flag = False


def handle_shutdown(signum, frame):
    global shutdown_flag
    print("\n[WORKER] Shutdown signal. Finishing current message...")
    shutdown_flag = True


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def delivery_callback(err, msg):
    if err:
        print(f"[OUTPUT ERROR] {err}")


# ----------------------------------------------------------------
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': APP_ID,
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


# ---------------- SAFE CALLBACKS ----------------

def on_assign(consumer, partitions):
    try:
        partition_nums = [p.partition for p in partitions]
        print(f"\n[WORKER] PARTITIONS ASSIGNED: {partition_nums}")
        print(f"[WORKER] This worker owns {len(partitions)} partition(s)")
        print(f"[WORKER] Processing sentences from these partitions only")
    except Exception as e:
        print(f"[WORKER] ASSIGN ERROR: {e}")


def on_revoke(consumer, partitions):
    try:
        partition_nums = [p.partition for p in partitions]
        print(f"\n[WORKER] PARTITIONS REVOKED: {partition_nums}")

        # SAFE COMMIT (fix for _NO_OFFSET)
        try:
            consumer.commit(asynchronous=False)
        except KafkaError as e:
            if e.code() != KafkaError._NO_OFFSET:
                print(f"[WORKER] Commit error on revoke: {e}")

    except Exception as e:
        print(f"[WORKER] REVOKE ERROR: {e}")


consumer.subscribe(
    [INPUT_TOPIC],
    on_assign=on_assign,
    on_revoke=on_revoke,
)


print(f"[WORKER] Starting Word Count Stream Processor")
print(f"[WORKER] Bootstrap: {KAFKA_BROKER}")
print(f"[WORKER] Group: {APP_ID}")
print(f"[WORKER] Input:  {INPUT_TOPIC}")
print(f"[WORKER] Output: {OUTPUT_TOPIC}")
print(f"[WORKER] Waiting for partition assignment...")
print("=" * 60)


# ---------------- MAIN LOOP ----------------

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
            sentence = msg.value().decode('utf-8').strip()
        except Exception:
            continue

        if not sentence:
            continue

        print(f"\n[INPUT]  partition={msg.partition()} offset={msg.offset()}")
        print(f"         sentence='{sentence}'")

        normalized = normalize(sentence)
        print(f"[NORM]   '{normalized}'")

        words = split_to_words(normalized)

        if not words:
            print(f"[FILTER] All words filtered — skipping")
            consumer.commit(asynchronous=False)
            continue

        print(f"[SPLIT]  {words}")

        for word in words:
            word_counts[word] = word_counts.get(word, 0) + 1
            count = word_counts[word]

            print(f"[COUNT]  '{word}' -> {count}")

            producer.produce(
                topic=OUTPUT_TOPIC,
                key=word.encode('utf-8'),
                value=f"{word}:{count}".encode('utf-8'),
                callback=delivery_callback,
            )

            print(f"[OUTPUT] key='{word}' value='{word}:{count}'")

        producer.poll(0)

        consumer.commit(asynchronous=False)

finally:
    try:
        producer.flush()
    except Exception:
        pass

    try:
        consumer.close()
    except Exception:
        pass

    print(f"\n[WORKER] Stopped cleanly.")
    print(f"[WORKER] Final word counts this session:")
    for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {word:<20} {count}")
