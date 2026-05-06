#!/usr/bin/env bash
# demo_bug.sh — demonstrates at-most-once delivery with the broken consumer.
#
# The broken consumer commits the offset BEFORE processing:
#   1. Poll a message from Kafka
#   2. Commit the offset  ← Kafka now considers this message "done"
#   3. Process it (do the work)
#
# If the consumer crashes between steps 2 and 3, the message is gone forever.
# Kafka won't redeliver it — the offset already advanced past it.
# The replacement consumer starts from the next offset and never sees it.
#
# What this script does:
#   1. Produce 10 messages
#   2. Start the broken consumer (C1) — it commits each offset before processing
#   3. After 4 seconds, SIGKILL C1 mid-processing
#      → some messages were committed but their processing was interrupted
#   4. Wait for the Kafka session timeout (6 s) so the broker detects C1 is gone
#   5. Start a replacement consumer (C2) — it resumes from the committed offsets,
#      skipping any messages C1 committed but never finished processing
#
# Expected result: at least one message lost (produced and committed, never processed).
# Note: the kill is timing-based — on rare runs it may land before the first commit,
# in which case no messages are lost. Re-run if that happens.

set -euo pipefail

RUN_ID=${RUN_ID:-$(date +%s)}
TOPIC=${TOPIC:-demo-topic-$RUN_ID}
CONSUMER_GROUP=${CONSUMER_GROUP:-demo-group-$RUN_ID}

echo "Run ID:  $RUN_ID"
echo "Topic:   $TOPIC"
echo "Group:   $CONSUMER_GROUP"
echo ""
echo "=== Broken consumer: commit → process (at-most-once) ==="
echo ""

# Produce the demo messages
TOPIC=$TOPIC CONSUMER_GROUP=$CONSUMER_GROUP uv run python -m src.producer

# Start the broken consumer — it commits offsets before processing
TOPIC=$TOPIC CONSUMER_GROUP=$CONSUMER_GROUP uv run python -m src.commit_before_process_consumer &
C1=$!
echo "Started broken consumer C1 (pid $C1)"
echo "C1 will commit each offset before processing (the bug)"

# Wait long enough for C1 to commit at least one message, but not finish processing it.
# The processing sleep is 3 s per message, so a 4 s kill lands reliably mid-processing.
sleep 4

# SIGKILL — no graceful shutdown, no pending work completes.
# Any message C1 committed but hadn't finished processing is now permanently lost.
echo "Killing C1 (simulating a crash)..."
kill -9 $C1 2>/dev/null || true

# Wait for the Kafka session timeout so the broker detects C1 is gone
# and reassigns its partitions (session.timeout.ms = 6000 ms in the consumer config).
echo "Waiting for session timeout so Kafka reassigns partitions..."
sleep 8

# Start a replacement consumer — it asks Kafka "where did we leave off?"
# Kafka answers with the last committed offset, which skips the lost messages.
TOPIC=$TOPIC CONSUMER_GROUP=$CONSUMER_GROUP uv run python -m src.commit_before_process_consumer &
C2=$!
echo "Started replacement consumer C2 (pid $C2)"
echo "C2 will start from the committed offset — lost messages are already skipped"

# Let C2 finish processing the remaining messages
sleep 12
echo "Stopping C2..."
kill $C2 2>/dev/null || true

echo ""
echo "Done. Run 'uv run python -m src.log_report' to see what was lost."
