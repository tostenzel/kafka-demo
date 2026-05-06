"""Shared utilities for Kafka blog examples."""

import fcntl
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from src.config import BOOTSTRAP_SERVERS, CONSUMER_GROUP, NUM_PARTITIONS, TOPIC

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_LOG = LOG_DIR / "processed.jsonl"
PRODUCER_LOG = LOG_DIR / "producer.json"

def ensure_topic(topic: str = TOPIC, num_partitions: int = NUM_PARTITIONS) -> None:
    """Create topic if it doesn't exist."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    futures = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
    for _, future in futures.items():
        try:
            future.result()
        except Exception:
            pass  # topic already exists


def reset_logs() -> None:
    """Clear processed log for a fresh run."""
    PROCESSED_LOG.write_text("")
    if PRODUCER_LOG.exists():
        PRODUCER_LOG.unlink()


def append_processed(
    consumer_id: str, topic: str, partition: int, offset: int, key: str | None, value: str
) -> None:
    """Atomically append one JSON line to processed log."""
    entry = {
        "consumer_id": consumer_id,
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "key": key,
        "value": value,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    line = json.dumps(entry) + "\n"
    with open(PROCESSED_LOG, "a", encoding="utf-8") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(line)
        fcntl.flock(f, fcntl.LOCK_UN)


def read_processed_log() -> list[dict[str, Any]]:
    """Read all entries from the processed log."""
    if not PROCESSED_LOG.exists():
        return []
    with open(PROCESSED_LOG, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def get_consumer_lag(
    topic: str = TOPIC,
    group_id: str = CONSUMER_GROUP,
    num_partitions: int = NUM_PARTITIONS,
) -> int:
    """Return total consumer lag for a group on a topic."""
    probe = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    tps = [TopicPartition(topic, p) for p in range(num_partitions)]
    committed = probe.committed(tps, timeout=10)

    total_lag = 0
    for tp in committed:
        _, high = probe.get_watermark_offsets(tp, timeout=5.0)
        committed_offset = tp.offset if tp.offset >= 0 else 0
        total_lag += max(high - committed_offset, 0)

    probe.close()
    return total_lag


def wait_for_kafka(timeout: int = 30) -> None:
    """Block until Kafka broker is reachable."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin.list_topics(timeout=2)
            return
        except Exception:
            time.sleep(1)
    raise TimeoutError("Kafka not reachable")
