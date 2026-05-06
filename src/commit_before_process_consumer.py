"""BROKEN Kafka consumer — commit before process (at-most-once).

This demonstrates the bug: committing the offset BEFORE processing means
if the consumer crashes mid-processing, the message is lost forever.
The offset already advanced — no redelivery will happen.

"""

import asyncio
import signal
import uuid

from confluent_kafka import Consumer, Message

from src.config import BOOTSTRAP_SERVERS, CONSUMER_GROUP, TOPIC
from src.helpers import append_processed


class CommitBeforeProcessConsumer:
    """BROKEN: Kafka consumer that commits before processing (at-most-once)."""

    def __init__(self, config: dict, process_fn):
        self.consumer = Consumer(config | {"enable.auto.commit": False})
        self.process_fn = process_fn
        self.running = True

    def shutdown(self, *_):
        self.running = False

    async def run(self, topic: str):
        self.consumer.subscribe([topic])
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None or msg.error():
                    await asyncio.sleep(0)
                    continue
                # BUG: commit BEFORE processing. If we crash between commit and
                # process completion, the message is gone — offset already advanced.
                self.consumer.commit(message=msg)
                await self.process_fn(msg)
        finally:
            self.consumer.close()


CONSUMER_ID = f"commit-first-{uuid.uuid4().hex[:8]}"


async def process(msg: Message) -> None:
    """Simulate slow processing (widens the window for the bug)."""
    await asyncio.sleep(1.0)
    key = msg.key().decode() if msg.key() else None
    value = msg.value().decode() if msg.value() else ""
    append_processed(CONSUMER_ID, msg.topic(), msg.partition(), msg.offset(), key, value)


async def main():
    consumer = CommitBeforeProcessConsumer(
        config={
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "client.id": CONSUMER_ID,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 6000,
        },
        process_fn=process,
    )
    signal.signal(signal.SIGINT, consumer.shutdown)
    signal.signal(signal.SIGTERM, consumer.shutdown)
    await consumer.run(TOPIC)


if __name__ == "__main__":
    asyncio.run(main())
