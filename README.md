# Kafka Blog Post: Understanding Consumer Groups by Breaking Them

Companion code for the blog post. All examples are runnable.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop

## Quick start

```bash
# Install dependencies
uv sync

# Start Kafka broker
docker compose up -d

# Run the integration tests
uv run pytest tests/ -v

# Demonstrate the correct consumer (no loss, no duplicates)
uv run python -m scripts.demo_correct

# Demonstrate the bug (commit-before-process loses messages on crash)
uv run python -m scripts.demo_bug

# Show a summary of the last demo run
uv run python -m src.log_report

# Stop Kafka
docker compose down -v
```

## Project structure

```
kafka-blog-post/
├── post.md                          # The blog post
├── pyproject.toml                   # uv project config
├── docker-compose.yml               # Single-node Kafka (KRaft, no Zookeeper)
├── scripts/
│   ├── demo_correct.py              # Runs the correct consumer scenario
│   └── demo_bug.py                  # Runs the broken consumer scenario
├── src/
│   ├── simple_consumer.py           # Correct: process → commit (at-least-once)
│   ├── commit_before_process_consumer.py  # Broken: commit → process (at-most-once)
│   ├── producer.py                  # Produces sample messages
│   ├── log_report.py                # Pretty-prints the demo logs
│   ├── process_helpers.py           # Subprocess helpers shared by demos and tests
│   └── helpers.py                   # Shared utilities (logging, lag check, topic mgmt)
└── tests/
    ├── test_rebalance.py            # Integration tests proving correctness and exposing bugs
    └── conftest.py                  # Fixtures and helpers
```

## What's demonstrated

1. **Correct consumer** (`simple_consumer.py`): direct poll → process → commit loop. Handles rebalances and crashes safely. No message loss.
2. **Broken consumer** (`commit_before_process_consumer.py`): commits offset before processing completes. Loses messages on crash.
3. **Integration tests** (`test_rebalance.py`): proves no message loss with real Kafka rebalances and SIGKILL crashes using subprocess consumers.

## Reading the logs

Each demo run writes two log files:

- **`logs/producer.json`** — the messages sent to Kafka, in order, with human-readable keys (`msg-01` … `msg-10`).
- **`logs/processed.jsonl`** — one line per message that was actually processed, with the consumer that handled it, the partition, and the Kafka offset.

Consumer IDs are prefixed to make their type immediately visible:
- `S:xxxx` — correct consumer (`simple_consumer.py`): process → commit
- `B:xxxx` — broken consumer (`commit_before_process_consumer.py`): commit → process

Run `uv run python -m src.log_report` after any demo for a summary report.

### Example: `uv run python -m scripts.demo_bug`

**`logs/producer.json`** — 10 messages produced:
```json
[
  {"key": "msg-01", "value": "Document uploaded"},
  {"key": "msg-02", "value": "OCR extraction complete"},
  ...
  {"key": "msg-10", "value": "Metadata extracted"}
]
```

**`logs/processed.jsonl`** — one line per message actually processed:
```
{"consumer": "B:9283", "partition": 1, "offset": 0, "key": "msg-04", "value": "..."}
{"consumer": "B:9283", "partition": 1, "offset": 1, "key": "msg-05", "value": "..."}
{"consumer": "B:4673", "partition": 0, "offset": 0, "key": "msg-01", "value": "..."}
...
```

Two things stand out in a run where the bug triggers:

1. **Missing offsets**: some offsets on a partition are skipped — those messages were committed but never processed.
2. **Two consumer IDs**: `B:xxxx` ran first, then a different `B:yyyy` took over after the kill triggered a rebalance.

**`uv run python -m src.log_report`** output:
```
─── Log summary ─────────────────────────
 Produced     10
 Processed     8
 Lost          2
 Duplicates    0

─── Lost messages ───────────────────────
  msg-08  Table detected on page 4
  msg-09  Text chunked (512 tokens)

─── Processed entries ───────────────────
 consumer   partition   offset   key      value
 ────────────────────────────────────────────────────────────────────────
 B:9283             1        0   msg-04   Search index updated
 B:9283             1        1   msg-05   User query received
 B:4673             0        0   msg-01   Document uploaded
 ...
```

What happened:
- `B:9283` polled a message, **committed the offset immediately** — Kafka now considers it done.
- The script killed `B:9283` during the 3-second processing sleep — work never finished, nothing written to the log.
- `B:4673` joined and asked Kafka "where did we leave off?" — Kafka returned the already-committed offset, permanently skipping the lost messages.

Run `uv run python -m scripts.demo_correct` to see the contrast — the correct consumer never loses a message because it only commits *after* processing completes.

> **Note:** the bug is timing-sensitive. The kill has to land in the window between commit and the end of the 3-second processing sleep. Most runs will show at least one lost message, but occasionally the kill lands before the first commit and nothing is lost — in that case just re-run the script.
