"""Fixtures for Kafka integration tests."""

import os
import signal
import time
from typing import Generator

import pytest

from src.config import BOOTSTRAP_SERVERS, CONSUMER_GROUP, NUM_PARTITIONS, TOPIC
from src.helpers import (
    PROCESSED_LOG,
    ensure_topic,
    reset_logs,
    wait_for_kafka,
)
from src.process_helpers import kill_consumer, start_consumer, stop_consumer, wait_until


def _reset_topic():
    """Delete and recreate topic + consumer group for a clean test."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Delete consumer group
    try:
        admin.delete_consumer_groups([CONSUMER_GROUP])[CONSUMER_GROUP].result()
    except Exception:
        pass

    # Delete and recreate topic for clean offsets
    try:
        admin.delete_topics([TOPIC])[TOPIC].result()
        time.sleep(2)
    except Exception:
        pass

    ensure_topic(TOPIC, NUM_PARTITIONS)
    time.sleep(1)


@pytest.fixture(autouse=True)
def kafka_ready():
    """Ensure Kafka is reachable before running tests."""
    if not os.environ.get("KAFKA_SKIP_WAIT"):
        wait_for_kafka(timeout=30)
    _reset_topic()
    reset_logs()


@pytest.fixture
def consumer_processes() -> Generator[list[subprocess.Popen], None, None]:
    """Manage consumer subprocess lifecycle with guaranteed cleanup."""
    processes: list[subprocess.Popen] = []
    yield processes
    for p in processes:
        if p.poll() is None:
            os.killpg(os.getpgid(p.pid), signal.SIGINT)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                p.wait()


