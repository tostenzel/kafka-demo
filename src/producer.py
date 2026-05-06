"""Simple Kafka producer — sends chunked text messages to a topic."""

import json

from confluent_kafka import Producer

from src.config import BOOTSTRAP_SERVERS, TOPIC
from src.helpers import PRODUCER_LOG, ensure_topic, reset_logs

SAMPLE_MESSAGES = [
    "Document uploaded",
    "OCR extraction complete",
    "Embeddings generated",
    "Search index updated",
    "User query received",
    "RAG response generated",
    "PDF pages segmented",
    "Table detected on page 4",
    "Text chunked (512 tokens)",
    "Metadata extracted",
]


def produce_messages(topic: str = TOPIC, messages: list[str] | None = None) -> int:
    """Produce messages to Kafka topic. Returns count of messages produced."""
    messages = messages or SAMPLE_MESSAGES
    ensure_topic(topic)
    reset_logs()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    produced_log: list[dict] = []

    for idx, msg in enumerate(messages):
        key = f"msg-{idx + 1:02d}"
        producer.produce(topic, value=msg.encode(), key=key.encode())
        produced_log.append({"key": key, "value": msg})

    producer.flush()

    with open(PRODUCER_LOG, "w", encoding="utf-8") as f:
        json.dump(produced_log, f, indent=2)

    print(f"Produced {len(produced_log)} messages to '{topic}'")
    return len(produced_log)


if __name__ == "__main__":
    produce_messages()
