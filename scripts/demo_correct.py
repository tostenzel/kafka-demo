"""Demo: at-least-once delivery with the correct consumer.

The correct consumer follows the poll → process → commit order:
  1. Poll a message from Kafka
  2. Process it (do the work)
  3. Only then commit the offset

If the consumer crashes between steps 1 and 3, Kafka redelivers the message
to the replacement consumer — no message is ever permanently lost.
The trade-off is at-least-once: a crash after processing but before commit
can cause a duplicate, but never a loss.

What this script does:
  1. Produce 10 messages
  2. Start consumer C1, wait until it has processed some but not all messages
  3. Start consumer C2 — joining mid-flight triggers a Kafka rebalance
  4. Wait for all messages to be processed
  5. Kill C1 — C2 takes over cleanly, no messages lost
  6. Wait for zero consumer lag, then stop C2

Expected result: all 10 messages processed, no loss, no duplicates.
"""

import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.helpers import PRODUCER_LOG, get_consumer_lag, read_processed_log
from src.process_helpers import kill_consumer, start_consumer, stop_consumer, wait_until

CONSUMER = "src.simple_consumer"


def main():
    print("=== Correct consumer: process → commit (at-least-once) ===\n")

    # Produce the demo messages (also clears logs from any previous run)
    subprocess.run([sys.executable, "-m", "src.producer"], check=True)

    with open(PRODUCER_LOG) as f:
        total = len(json.load(f))

    # Start first consumer
    c1 = start_consumer(CONSUMER)
    print(f"Started C1 (pid {c1.pid})")

    # Wait until C1 has processed some but not all messages — then join C2 to
    # trigger a rebalance while work is still in flight.
    assert wait_until(
        lambda: 0 < len(read_processed_log()) < total, timeout=20
    ), "C1 should have processed some messages by now"
    print(f"C1 processed {len(read_processed_log())}/{total} — starting C2 to trigger rebalance")

    c2 = start_consumer(CONSUMER)
    print(f"Started C2 (pid {c2.pid}) — rebalance in progress")

    # Wait for all messages to be processed across both consumers
    assert wait_until(
        lambda: len(read_processed_log()) >= total, timeout=30
    ), f"Expected {total} processed, got {len(read_processed_log())}"
    print(f"All {total} messages processed — killing C1 to trigger a second rebalance")

    # Kill C1; C2 inherits its partitions cleanly because the commit already happened
    kill_consumer(c1)
    wait_until(lambda: get_consumer_lag() == 0, timeout=15)
    stop_consumer(c2)

    # Verify
    entries = read_processed_log()
    with open(PRODUCER_LOG) as f:
        produced_keys = {d["key"] for d in json.load(f)}

    missing = produced_keys - {e["key"] for e in entries}
    offsets = [(e["partition"], e["offset"]) for e in entries]
    duplicates = len(offsets) - len(set(offsets))

    print(f"\nLost:       {len(missing)}")
    print(f"Duplicates: {duplicates}")
    print("\nRun 'uv run python -m src.log_report' to see the full report.")


if __name__ == "__main__":
    main()
