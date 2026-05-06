#!/usr/bin/env bash
# demo_correct.sh — demonstrates at-least-once delivery with the correct consumer.
#
# The correct consumer follows the poll → process → commit order:
#   1. Poll a message from Kafka
#   2. Process it (do the work)
#   3. Only then commit the offset
#
# This means that if the consumer crashes between steps 1 and 3, Kafka will
# redeliver the message to the replacement consumer — no message is ever lost.
# The trade-off is at-least-once: a crash after processing but before commit
# causes a duplicate, not a loss.
#
# What this script does:
#   1. Produce 10 messages
#   2. Start consumer C1, let it process for a few seconds
#   3. Start consumer C2 mid-flight, triggering a rebalance
#   4. Kill C1 — C2 takes over any in-flight partitions cleanly
#   5. Let C2 finish, then stop it
#
# Expected result: all 10 messages processed, no loss, no duplicates.

set -euo pipefail

RUN_ID=${RUN_ID:-$(date +%s)}
TOPIC=${TOPIC:-demo-topic-$RUN_ID}
CONSUMER_GROUP=${CONSUMER_GROUP:-demo-group-$RUN_ID}

echo "Run ID:  $RUN_ID"
echo "Topic:   $TOPIC"
echo "Group:   $CONSUMER_GROUP"
echo ""
echo "=== Correct consumer: process → commit (at-least-once) ==="
echo ""

# Produce the demo messages
TOPIC=$TOPIC CONSUMER_GROUP=$CONSUMER_GROUP uv run python -m src.producer

# Start first consumer
TOPIC=$TOPIC CONSUMER_GROUP=$CONSUMER_GROUP uv run python -m src.simple_consumer &
C1=$!
echo "Started consumer C1 (pid $C1)"

# Let C1 process a few messages, then join a second consumer to trigger a rebalance
sleep 3
TOPIC=$TOPIC CONSUMER_GROUP=$CONSUMER_GROUP uv run python -m src.simple_consumer &
C2=$!
echo "Started consumer C2 (pid $C2) — rebalance will occur"

# Kill C1 to simulate a crash; C2 takes over its partitions
sleep 5
echo "Killing C1..."
kill $C1 2>/dev/null || true

# Let C2 finish processing the remaining messages
sleep 5
echo "Stopping C2..."
kill $C2 2>/dev/null || true

echo ""
echo "Done. Run 'uv run python -m src.log_report' to see results."
