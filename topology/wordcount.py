"""
Word Count Stream Processing Topology — 3-broker cluster edition

Topology:
  input-text (3 partitions, RF=3, leaders distributed across 3 brokers)
       |
  [DECODE] raw bytes -> string
       |
  [NORMALIZE] lowercase, strip punctuation
       |
  [FLAT_MAP] 1 sentence -> N word events
       |
  [FILTER] remove stop words, noise, single chars
       |
  [COUNT] word_counts Table[word] += 1 (stateful, per-partition)
       |
  word-count-output (3 partitions, RF=3)

State:
  word_counts Table backed by changelog topic (RF=3, compacted)
  State survives: broker failure, worker restart (with rocksdb in production)
  Worker running on kafka-2 owns partitions assigned to it
  Worker running on kafka-1 owns partitions assigned to it
  Each worker maintains its own slice of the Table state
"""

import re
import faust
from config.kafka_config import (
    KAFKA_BROKER,
    INPUT_TOPIC,
    OUTPUT_TOPIC,
    APP_ID,
    TOPIC_PARTITIONS,
)

app = faust.App(
    APP_ID,
    broker=f'kafka://{KAFKA_BROKER}',    # All 3 brokers in bootstrap list
    value_serializer='raw',
    store='memory://',
    topic_partitions=TOPIC_PARTITIONS,
    # Producer config — acks=all for durability on 3-broker cluster
    producer_acks='all',
    # Consumer group session timeout — consistent with cluster tuning
    consumer_session_timeout=10.0,
    consumer_heartbeat_interval=3.0,
)

input_topic = app.topic(
    INPUT_TOPIC,
    value_type=bytes,
    partitions=TOPIC_PARTITIONS,
    replicas=3,       # RF=3 — survives any single broker failure
)

output_topic = app.topic(
    OUTPUT_TOPIC,
    value_type=str,
    partitions=TOPIC_PARTITIONS,
    replicas=3,
)

word_counts = app.Table(
    'word-counts',
    default=int,
    help='Running count per word — partitioned across worker fleet',
)

STOP_WORDS = frozenset({
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at',
    'to', 'for', 'of', 'with', 'by', 'from', 'is', 'was',
    'are', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should',
    'may', 'might', 'must', 'shall', 'can', 'this', 'that',
    'it', 'its', 'as', 'not', 'no', 'so',
})

def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def split_to_words(sentence: str) -> list:
    words = sentence.split()
    return [
        word for word in words
        if word
        and word not in STOP_WORDS
        and len(word) >= 2
        and not word.isdigit()
    ]

@app.agent(input_topic)
async def process_sentences(sentences):
    """
    Main stream processing agent.
    In 3-broker cluster: this agent runs on each Faust worker.
    Each worker processes only the partitions assigned to it.
    kafka-1 worker and kafka-2 worker process different partitions
    simultaneously — true parallel stream processing.
    """
    async for sentence_bytes in sentences:
        try:
            sentence = sentence_bytes.decode('utf-8').strip()
        except (UnicodeDecodeError, AttributeError):
            continue

        if not sentence:
            continue

        normalized = normalize(sentence)

        print(f"\n[INPUT]  {sentence}")
        print(f"[NORM]   {normalized}")

        words = split_to_words(normalized)

        if not words:
            print(f"[FILTER] All words filtered — skipping")
            continue

        print(f"[SPLIT]  {words}")

        for word in words:
            word_counts[word] += 1
            current_count = word_counts[word]
            print(f"[COUNT]  '{word}' -> {current_count}")

            await output_topic.send(
                key=word.encode('utf-8'),
                value=f"{word}:{current_count}",
            )
            print(f"[OUTPUT] key='{word}' value='{word}:{current_count}'")
