"""Demo: at-most-once delivery with the broken consumer.

The broken consumer commits the offset BEFORE processing:
  1. Poll a message from Kafka
  2. Commit the offset  ← Kafka now considers this message "done"
  3. Process it (do the work)

If the consumer crashes between steps 2 and 3, the message is gone forever.
Kafka won't redeliver it — the offset already advanced past it.
The replacement consumer starts from the next offset and never sees it.

What this script does:
  1. Produce 10 messages
  2. Start the broken consumer C1 — it commits each offset before processing
  3. Wait until the first message appears in the log. At that point C1 has
     committed the *next* message's offset but is mid-sleep processing it.
  4. SIGKILL C1 — the committed-but-unprocessed message is permanently lost
  5. Wait for the Kafka session timeout (6 s) so the broker detects C1 is gone
  6. Start replacement consumer C2 — it resumes from the committed offset,
     skipping the lost message
  7. Wait for zero consumer lag, then stop C2

Expected result: at least one message lost (committed by C1, never processed).
"""

import json
import subprocess
import sys
import time

from src.helpers import PRODUCER_LOG, get_consumer_lag, read_processed_log
from src.process_helpers import kill_consumer, start_consumer, stop_consumer, wait_until

CONSUMER = "src.commit_before_process_consumer"
SESSION_TIMEOUT_S = 8  # slightly above session.timeout.ms=6000 in consumer config


def main():
    print("=== Broken consumer: commit → process (at-most-once) ===\n")

    # Produce the demo messages (also clears logs from any previous run)
    subprocess.run([sys.executable, "-m", "src.producer"], check=True)

    # Start the broken consumer — it commits offsets before processing
    c1 = start_consumer(CONSUMER)
    print(f"Started broken consumer C1 (pid {c1.pid})")
    print("C1 commits each offset before processing (the bug)\n")

    # Wait until the first message has been fully processed and written to the log.
    # At this point C1 has already committed the *next* message's offset and is
    # sleeping mid-processing — the perfect moment to simulate a crash.
    assert wait_until(
        lambda: len(read_processed_log()) >= 1, timeout=20
    ), "C1 should have processed at least one message by now"

    print("C1 wrote its first entry — it has committed the next offset but is mid-processing.")
    print("Killing C1 now (simulating a crash)...")

    # SIGKILL — no graceful shutdown, no pending work completes.
    # The message C1 committed but hadn't finished processing is now permanently lost.
    kill_consumer(c1)

    # Wait for Kafka to detect the crash via session timeout and reassign partitions
    print(f"Waiting {SESSION_TIMEOUT_S}s for Kafka session timeout...")
    time.sleep(SESSION_TIMEOUT_S)

    # Start replacement consumer — asks Kafka "where did we leave off?"
    # Kafka returns the last committed offset, which skips the lost message.
    c2 = start_consumer(CONSUMER)
    print(f"Started replacement consumer C2 (pid {c2.pid})")
    print("C2 resumes from the committed offset — lost messages are already skipped\n")

    wait_until(lambda: get_consumer_lag() == 0, timeout=30)
    stop_consumer(c2)

    # Report
    entries = read_processed_log()
    with open(PRODUCER_LOG) as f:
        produced_keys = {d["key"] for d in json.load(f)}

    missing = produced_keys - {e["key"] for e in entries}
    print(f"Lost: {len(missing)} message(s): {sorted(missing)}")
    print("\nRun 'uv run python -m src.log_report' to see the full report.")


if __name__ == "__main__":
    main()
