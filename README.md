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
make up

# Run the integration tests
make test

# Demonstrate the correct consumer (no loss, no duplicates)
make demo-correct

# Demonstrate the bug (commit-before-process loses messages on crash)
make demo-bug

# Stop Kafka
make down
```

## Project structure

```
kafka-blog-post/
├── post.md                          # The blog post
├── pyproject.toml                   # uv project config
├── docker-compose.yml               # Single-node Kafka (KRaft, no Zookeeper)
├── Makefile                         # Convenience targets
├── src/
│   ├── simple_consumer.py           # Correct: process → commit (at-least-once)
│   ├── commit_before_process_consumer.py  # Broken: commit → process (at-most-once)
│   ├── producer.py                  # Produces sample messages
│   └── helpers.py                   # Shared utilities (logging, lag check, topic mgmt)
└── tests/
    ├── test_rebalance.py            # Integration tests proving correctness and exposing bugs
    └── conftest.py                  # Fixtures and helpers
```

## What's demonstrated

1. **Correct consumer** (`simple_consumer.py`): direct poll → process → commit loop. Handles rebalances and crashes safely. No message loss.
2. **Broken consumer** (`commit_before_process_consumer.py`): commits offset before processing completes. Loses messages on crash.
3. **Integration tests** (`test_rebalance.py`): proves no message loss with real Kafka rebalances and SIGKILL crashes using subprocess consumers.
