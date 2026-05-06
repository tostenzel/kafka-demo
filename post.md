# Understanding Kafka Consumer Groups by Breaking Them

Last year, we lost messages in production. Documents uploaded by users were never processed — no OCR, no embedding, no search index entry. They simply vanished. Our monitoring showed zero consumer lag. Kafka thought everything was consumed. But the documents were gone.

The root cause was a single line of code: we committed the offset *before* processing completed. When a consumer crashed mid-processing, the offset had already advanced. The message was marked as consumed but never actually processed. No redelivery. No retry. Gone.

This post teaches Kafka from the ground up — topics, partitions, consumer groups, offsets, and rebalancing — then shows you exactly how to build a correct consumer and proves it with an integration test against a real Kafka broker. By the end, the bug above will be obvious to you, and you'll know why it can only be caught with real infrastructure, not mocks.

---

## Why Kafka?

Our platform processes documents through multiple stages: ingestion, OCR extraction, chunking, embedding generation, semantic indexing, and serving RAG queries. Each stage has different performance characteristics. OCR takes seconds per page. Embedding generation is GPU-bound. Indexing is I/O-bound.

We need decoupled, asynchronous communication between services:

| Approach | Fits when | Breaks when |
|----------|-----------|-------------|
| REST | Simple request-reply, low volume | One slow stage blocks everything |
| Task queues (Celery) | Background jobs, fire-and-forget | You need replay, ordering, or fan-out |
| **Kafka** | Multi-consumer, replay, ordering, high throughput | Overkill for simple CRUD |

Kafka wins because:

1. **Decoupled stages**: ingestion, OCR, chunking, embedding, indexing — each is an independent consumer group.
2. **Fan-out**: one "document-uploaded" event triggers OCR, table extraction, and metadata processing simultaneously.
3. **Replay**: upgrading your embedding model? Replay all documents from offset zero.
4. **Backpressure**: consumers process at their own speed. Service down for maintenance? Messages queue up, no data loss.
5. **Ordering per document**: partition by document ID ensures all pages are processed in order.

---

## Kafka Fundamentals

### Topics and Partitions

A **topic** is a named, append-only log. It's split into **partitions** — ordered, immutable sequences of messages. Every message has a sequential **offset** within its partition.

```
Topic: "documents" (3 partitions)

Partition 0:  [msg0] [msg1] [msg2] [msg3] →
Partition 1:  [msg0] [msg1] [msg2] →
Partition 2:  [msg0] [msg1] [msg2] [msg3] [msg4] →
```

Messages are ordered *within* a partition. Across partitions, there is no global ordering.

### Producers

Producers write messages to topics. Each message optionally has a **key**. The key determines the partition: `hash(key) % num_partitions`. No key means round-robin.

```python
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

for document in documents:
    producer.produce(
        "documents",
        value=document.content.encode(),
        key=document.id.encode(),  # same doc → same partition → ordered
    )

producer.flush()
```

### Consumer Groups and Offsets

A **consumer group** is a set of consumers that share work. Kafka guarantees each partition is assigned to exactly one consumer within a group.

```
Topic: "documents" (4 partitions)
Consumer Group: "ocr-workers"

Consumer A: Partition 0, Partition 1
Consumer B: Partition 2, Partition 3
```

Each consumer tracks its progress via a **committed offset** — a bookmark stored in Kafka. On restart, the consumer resumes from its last committed offset.

This is Kafka's scalability model: add consumers (up to the number of partitions) to parallelize work.

### Rebalancing

A **rebalance** occurs when the group composition changes:
- A new consumer joins
- A consumer leaves or crashes
- Partitions are added to the topic

During rebalance, the broker reassigns partitions across remaining consumers. This is the critical moment: any in-flight work must be handled correctly.

### Delivery Semantics

Three guarantees are possible:

- **At-most-once**: commit the offset *before* processing. Crash mid-processing → message lost.
- **At-least-once**: process, *then* commit. Crash after processing but before commit → message redelivered. Duplicates possible but no loss.
- **Exactly-once**: Kafka transactions. Complex, high overhead, usually unnecessary.

We choose **at-least-once**. Our downstream processing is idempotent — receiving a document twice is harmless. Losing a document is not.

---

## The Correct Consumer

Here's the complete implementation:

```python
# src/simple_consumer.py

class SimpleConsumer:
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
                await self.process_fn(msg)
                self.consumer.commit(message=msg)
        finally:
            self.consumer.close()
```

Every design choice matters:

1. **`enable.auto.commit = False`** — We control exactly when offsets advance. No surprises.
2. **Process then commit** — At-least-once semantics. If we crash after processing but before committing, the message will be redelivered. Safe.
3. **Synchronous commit** — `commit()` blocks until the broker confirms the offset is stored. No window where the offset could be lost.
4. **Single-threaded loop** — Rebalance callbacks fire inside `poll()`. By the time `poll()` returns a message, partition assignment is settled. The message belongs to us. No stale state.

This is ~20 lines of code. If your consumer is correct, it's simple. If it's complex, it's probably wrong.

---

## The Bug: Commit Before Process

Here's a consumer that looks almost identical:

```python
# src/commit_before_process_consumer.py

class CommitBeforeProcessConsumer:
    def __init__(self, config: dict, process_fn):
        self.consumer = Consumer(config | {"enable.auto.commit": False})
        self.process_fn = process_fn
        self.running = True

    async def run(self, topic: str):
        self.consumer.subscribe([topic])
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None or msg.error():
                    await asyncio.sleep(0)
                    continue
                self.consumer.commit(message=msg)  # ← commit FIRST
                await self.process_fn(msg)          # ← process AFTER
        finally:
            self.consumer.close()
```

Two lines swapped. The offset advances *before* processing completes. This is at-most-once semantics.

### What goes wrong

```
Time →

Consumer 1:  [poll M5] [commit M5] [process M5 ............] ← CRASH
                                          ↑
                                     offset already at 6

Consumer 2 (replacement):           [poll M6] [process M6] [commit M6] ...

M5: committed (offset 6), never processed. Gone forever.
```

1. Consumer polls message M5.
2. Consumer commits offset — Kafka now thinks M5 is done.
3. Consumer starts processing M5 (slow — OCR takes seconds).
4. Consumer crashes mid-processing.
5. Replacement consumer starts from committed offset 6.
6. M5 was never processed. No redelivery. Lost.

### Why it's subtle

This works perfectly in development. It works perfectly in staging. It works perfectly in production — until something crashes. Which in a distributed system is not a question of *if* but *when*.

You cannot catch this with a unit test. The failure requires a real crash (or SIGKILL) at exactly the right moment. It requires a real consumer group with real offset storage so the replacement consumer starts from the wrong offset.

---

## The Integration Test

This is why we test with real Kafka infrastructure. No mocks. Real broker, real consumer groups, real kill signals.

### The approach

- **Docker Kafka** (KRaft mode, no Zookeeper) with 2 partitions
- **Subprocess consumers** — real OS processes, real group members
- **SIGKILL** to simulate a crash (no graceful shutdown, no pending commits flushed)
- **File-based log** for cross-process verification (each consumer appends to a shared JSONL file)
- **Retry-polling assertions** instead of fragile `time.sleep()` waits

### The test

```python
# tests/test_rebalance.py (simplified)

class TestCorrectConsumerRebalance:
    def test_no_message_loss_after_rebalance(self, consumer_processes):
        produced = produce_test_messages(count=10)

        # Start first consumer
        c1 = start_consumer("src.simple_consumer")

        # Wait until some (not all) messages are processed
        assert wait_until(lambda: 0 < len(read_processed_log()) < produced)

        # Second consumer joins → triggers rebalance
        c2 = start_consumer("src.simple_consumer")

        # Wait for all messages to be processed
        assert wait_until(lambda: len(read_processed_log()) >= produced)

        # Kill first consumer → triggers second rebalance
        stop_consumer(c1)
        wait_until(lambda: get_consumer_lag() == 0)
        stop_consumer(c2)

        # Verify: no loss
        entries = read_processed_log()
        produced_keys = load_producer_keys()
        processed_keys = {e["key"] for e in entries}
        assert not (produced_keys - processed_keys), "Messages lost"

        # Verify: no duplicates
        offsets = [(e["partition"], e["offset"]) for e in entries]
        assert len(offsets) == len(set(offsets)), "Duplicates detected"

        # Verify: zero lag
        assert get_consumer_lag() == 0
```

### Why subprocess consumers?

Real OS processes create real consumer group members. When you SIGKILL one, the broker detects failure through session timeout — exactly like production. A thread-based test can't trigger a real rebalance.

### The crash test

The same infrastructure proves the bug:

```python
class TestCorrectConsumerCrash:
    def test_no_message_loss_after_crash(self, consumer_processes):
        produce_test_messages()

        c1 = start_consumer("src.simple_consumer")
        wait_until(lambda: len(read_processed_log()) > 0)

        kill_consumer(c1)  # SIGKILL — no graceful shutdown
        time.sleep(8)      # wait for session timeout

        # Replacement consumer picks up from last committed offset
        c2 = start_consumer("src.simple_consumer")
        wait_until(lambda: get_consumer_lag() == 0)
        stop_consumer(c2)

        # No loss — at-least-once guarantee holds even after crash
        entries = read_processed_log()
        produced_keys = load_producer_keys()
        assert not (produced_keys - {e["key"] for e in entries})


class TestCommitBeforeProcessBug:
    @pytest.mark.xfail(reason="Commit-before-process loses messages on crash")
    def test_commit_before_process_loses_messages(self, consumer_processes):
        # Same scenario — but with the broken consumer
        # Messages ARE lost. The test fails. Bug confirmed.
        ...
```

The correct consumer survives the crash. The broken consumer loses messages. Same infrastructure, same kill signal, same timing. The only difference: which line comes first — `commit` or `process`.

---

## Production Hardening

The simple consumer is correct but minimal. For production, add:

**Dead Letter Queue**: messages that fail processing go to a separate topic rather than blocking forever.

```python
try:
    await self.process_fn(msg)
    self.consumer.commit(message=msg)
except Exception:
    dlq_producer.produce("dlq-topic", value=msg.value(), key=msg.key())
    dlq_producer.flush()
    self.consumer.commit(message=msg)  # advance past the failed message
```

**Graceful shutdown**: handle SIGINT/SIGTERM to stop the loop and close the consumer cleanly (triggers cooperative rebalance instead of waiting for session timeout).

**Lag monitoring**: track `high_watermark - committed_offset` per partition. Alert when lag grows.

```python
def get_consumer_lag(topic, group_id, num_partitions):
    probe = Consumer({"bootstrap.servers": SERVERS, "group.id": group_id})
    tps = [TopicPartition(topic, p) for p in range(num_partitions)]
    committed = probe.committed(tps)
    total_lag = 0
    for tp in committed:
        _, high = probe.get_watermark_offsets(tp)
        total_lag += max(high - max(tp.offset, 0), 0)
    probe.close()
    return total_lag
```

**Session timeout tuning**: the default (45s) means 45 seconds of inactivity before Kafka considers a consumer dead. Too short causes unnecessary rebalances during slow processing. Too long means slow failure detection. We use 6s for fast rebalance in tests, 30–45s in production.

---

## Takeaways

1. **Kafka offsets are the single source of truth for consumer progress.** Commit at the wrong time and messages are lost or duplicated.

2. **Process then commit. Always.** At-least-once with idempotent processing is almost always what you want.

3. **If your consumer is correct, it's simple.** A direct poll → process → commit loop. No threads, no queues, no buffers between poll and commit.

4. **Integration tests with real Kafka are non-negotiable.** The bugs that matter only manifest with real rebalances, real crashes, and real offset storage. Mocking Kafka means mocking the bugs away.


---

*All code in this post is runnable. See the [companion repository](.) for Docker Compose, a Makefile, and both the correct and broken consumers you can test yourself.*
