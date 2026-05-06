"""Integration tests: prove correctness of simple consumer and expose the commit-order bug.

Requires a running Kafka broker (docker compose up).

Tests use subprocess consumers (real OS processes = real consumer group members).
This is necessary to trigger real Kafka rebalances and simulate real crashes (SIGKILL).

Two implementations tested:
- src.simple_consumer → process then commit (at-least-once, correct)
- src.commit_before_process_consumer → commit then process (at-most-once, loses messages)
"""

import json
import subprocess
import sys
import time

import pytest

from src.commit_before_process_consumer import CommitBeforeProcessConsumer  # noqa: F401
from src.helpers import (
    PRODUCER_LOG,
    get_consumer_lag,
    read_processed_log,
)
from src.simple_consumer import SimpleConsumer  # noqa: F401
from tests.conftest import kill_consumer, start_consumer, stop_consumer, wait_until


def produce_test_messages(count: int = 10) -> int:
    """Run producer and return number of messages produced."""
    result = subprocess.run(
        [sys.executable, "-m", "src.producer"],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(PRODUCER_LOG.parent.parent),
    )
    assert "Produced" in result.stdout
    return count


# ---------------------------------------------------------------------------
# Rebalance test: only the correct consumer survives without loss or duplicates
# ---------------------------------------------------------------------------


class TestRebalance:
    """Start two consumers, trigger rebalances, verify no loss and no duplicates."""

    def test_correct_consumer_no_loss_after_rebalance(self, consumer_processes):
        produced = produce_test_messages()

        c1 = start_consumer("src.simple_consumer")
        consumer_processes.append(c1)

        assert wait_until(
            lambda: 0 < len(read_processed_log()) < produced,
            timeout=15,
        ), "Consumer #1 should process some but not all messages"

        # Second consumer joins -> rebalance
        c2 = start_consumer("src.simple_consumer")
        consumer_processes.append(c2)

        assert wait_until(
            lambda: len(read_processed_log()) >= produced,
            timeout=30,
        ), f"Expected {produced} processed, got {len(read_processed_log())}"

        # Kill first -> second rebalance
        stop_consumer(c1)
        wait_until(lambda: get_consumer_lag() == 0, timeout=15)
        stop_consumer(c2)

        # Verify
        entries = read_processed_log()
        with open(PRODUCER_LOG, encoding="utf-8") as f:
            produced_keys = {d["key"] for d in json.load(f)}

        missing = produced_keys - {e["key"] for e in entries}
        assert not missing, f"{len(missing)} messages lost"

        offsets = [(e["partition"], e["offset"]) for e in entries]
        assert len(offsets) == len(set(offsets)), "Duplicate offsets detected"

        assert get_consumer_lag() == 0


# ---------------------------------------------------------------------------
# Crash recovery: same scenario, two implementations, opposite outcomes
# ---------------------------------------------------------------------------


CORRECT = "src.simple_consumer"
BROKEN = "src.commit_before_process_consumer"


class TestCrashRecovery:
    """Kill consumer mid-processing (SIGKILL), start replacement, check for loss.

    The correct consumer (process -> commit) loses nothing.
    The broken consumer (commit -> process) loses messages.
    """

    @pytest.mark.parametrize(
        "module",
        [
            CORRECT,
            pytest.param(
                BROKEN,
                marks=pytest.mark.xfail(
                    reason="commit-before-process loses messages on crash",
                    strict=False,
                ),
            ),
        ],
        ids=["process-then-commit", "commit-then-process"],
    )
    def test_no_message_loss_after_crash(self, module, consumer_processes):
        produce_test_messages()

        c1 = start_consumer(module)
        consumer_processes.append(c1)

        # Wait until at least one message is processed
        assert wait_until(
            lambda: len(read_processed_log()) > 0,
            timeout=15,
        ), "Consumer should have processed at least one message"

        # SIGKILL: simulate crash (no graceful shutdown, no pending commits flushed)
        kill_consumer(c1)

        # Wait for session timeout so replacement can take over partitions
        time.sleep(8)

        # Replacement consumer picks up from last committed offset
        c2 = start_consumer(module)
        consumer_processes.append(c2)
        wait_until(lambda: get_consumer_lag() == 0, timeout=30)
        stop_consumer(c2)

        # Verify: no messages lost
        entries = read_processed_log()
        with open(PRODUCER_LOG, encoding="utf-8") as f:
            produced_keys = {d["key"] for d in json.load(f)}
            processed_keys = {e["key"] for e in entries}

        missing = produced_keys - processed_keys
        assert not missing, f"{len(missing)} messages lost"