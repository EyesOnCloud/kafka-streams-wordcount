"""
Word Count Stream Processing Topology
confluent-kafka implementation — Kafka 3.7 + Python 3.12 compatible

Topology:
  input-text (3 partitions, RF=3)
       |
  [DECODE] bytes -> string
       |
  [NORMALIZE] lowercase, strip punctuation
       |
  [FLAT_MAP] 1 sentence -> N word events
       |
  [FILTER] stop words, noise, single chars
       |
  [COUNT] in-memory dict (stateful aggregation)
       |
  word-count-output (3 partitions, RF=3)

State: in-memory dict — same concept as Faust Table
       production would use Redis/RocksDB for persistence
"""

import re

STOP_WORDS = frozenset({
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at',
    'to', 'for', 'of', 'with', 'by', 'from', 'is', 'was',
    'are', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should',
    'may', 'might', 'must', 'shall', 'can', 'this', 'that',
    'it', 'its', 'as', 'not', 'no', 'so',
})

def normalize(text: str) -> str:
    """Stateless — each sentence independent. No state needed."""
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def split_to_words(sentence: str) -> list:
    """
    Flat map — 1 sentence -> N words.
    Stateless: each sentence processed independently.
    """
    words = sentence.split()
    return [
        w for w in words
        if w
        and w not in STOP_WORDS
        and len(w) >= 2
        and not w.isdigit()
    ]
