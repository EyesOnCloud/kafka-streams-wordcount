"""
Entry point — Faust worker process.

3-broker cluster: 
  kafka-1: python app.py worker -l info
  kafka-2: python app.py worker -l info
  Each worker connects to all 3 brokers, joins same consumer group,
  receives subset of partitions, processes independently.
"""

from topology.wordcount import app

if __name__ == '__main__':
    app.main()
