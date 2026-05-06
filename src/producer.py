"""Simple Kafka producer — sends chunked text messages to a topic."""

import json
import uuid

from confluent_kafka import Producer

from src.config import BOOTSTRAP_SERVERS, TOPIC
from src.helpers import PRODUCER_LOG, ensure_topic, reset_logs

SAMPLE_MESSAGES = [
    "The document was uploaded to the platform for processing.",
    "OCR extraction identified three tables and twelve paragraphs.",
    "Semantic embeddings were generated using the latest model version.",
    "The search index was updated with the new document vectors.",
    "A user query matched two paragraphs with high confidence scores.",
    "The RAG system retrieved context and generated a summary response.",
    "Page segmentation split the PDF into individual page images.",
    "Table detection found a financial summary on page four.",
    "The chunking strategy used 512-token windows with 64-token overlap.",
    "Metadata extraction captured document title, date, and author fields.",
]


def produce_messages(topic: str = TOPIC, messages: list[str] | None = None) -> int:
    """Produce messages to Kafka topic. Returns count of messages produced."""
    messages = messages or SAMPLE_MESSAGES
    ensure_topic(topic)
    reset_logs()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    produced_log: list[dict] = []

    for msg in messages:
        key = str(uuid.uuid4())
        producer.produce(topic, value=msg.encode(), key=key.encode())
        produced_log.append({"key": key, "value": msg})

    producer.flush()

    with open(PRODUCER_LOG, "w", encoding="utf-8") as f:
        json.dump(produced_log, f, indent=2)

    print(f"Produced {len(produced_log)} messages to '{topic}'")
    return len(produced_log)


if __name__ == "__main__":
    produce_messages()
