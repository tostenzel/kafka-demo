"""Correct Kafka consumer — direct poll/process/commit loop.

This is the RIGHT way to consume from Kafka with at-least-once semantics.
No buffering between poll and commit means rebalances cannot cause duplicates.
"""

import asyncio
import signal
import uuid
from typing import Awaitable, Callable

from confluent_kafka import Consumer, Message

from src.config import BOOTSTRAP_SERVERS, CONSUMER_GROUP, TOPIC
from src.helpers import append_processed


class SimpleConsumer:
    """Kafka consumer with direct poll → process → commit loop."""

    def __init__(self, config: dict, process_fn: Callable[[Message], Awaitable[None]]):
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
                await self.process_fn(msg)
                self.consumer.commit(message=msg)
        finally:
            self.consumer.close()


CONSUMER_ID = f"S:{uuid.uuid4().hex[:4]}"


async def process(msg: Message) -> None:
    """Simulate processing work."""
    await asyncio.sleep(0.5)
    key = msg.key().decode() if msg.key() else None
    value = msg.value().decode() if msg.value() else ""
    append_processed(CONSUMER_ID, msg.topic(), msg.partition(), msg.offset(), key, value)


async def main():
    consumer = SimpleConsumer(
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
