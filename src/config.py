"""Application runtime configuration loaded from environment variables."""

import os

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "demo-topic")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "demo-group")
NUM_PARTITIONS = max(1, int(os.getenv("NUM_PARTITIONS", "2")))
