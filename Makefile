.PHONY: up down test demo-correct demo-bug

RUN_ID ?= $(shell date +%s)
DEMO_TOPIC ?= demo-topic-$(RUN_ID)
DEMO_GROUP ?= demo-group-$(RUN_ID)
DEMO_ENV = TOPIC=$(DEMO_TOPIC) CONSUMER_GROUP=$(DEMO_GROUP)

up:
	docker compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 5
	@echo "Kafka ready on localhost:9092"

down:
	docker compose down -v

test:
	uv run pytest tests/ -v

demo-correct:
	@echo "Run ID: $(RUN_ID)"
	@echo "Topic: $(DEMO_TOPIC), Group: $(DEMO_GROUP)"
	@echo "=== Correct consumer: process → commit (at-least-once) ==="
	$(DEMO_ENV) uv run python -m src.producer
	$(DEMO_ENV) uv run python -m src.simple_consumer &
	@sleep 3
	$(DEMO_ENV) uv run python -m src.simple_consumer &
	@sleep 5
	@kill %1 2>/dev/null || true
	@sleep 5
	@kill %2 2>/dev/null || true
	@echo ""
	@echo "=== Results (expect: no loss, no duplicates): ==="
	@uv run python -c "import json; from src.helpers import read_processed_log, PRODUCER_LOG; entries = read_processed_log(); produced = set(d['key'] for d in json.load(open(PRODUCER_LOG))); processed = set(e['key'] for e in entries); offsets = [(e['partition'], e['offset']) for e in entries]; print(f'Produced: {len(produced)}, Processed: {len(entries)}, Lost: {len(produced - processed)}, Duplicates: {len(offsets) - len(set(offsets))}')"

demo-bug:
	@echo "Run ID: $(RUN_ID)"
	@echo "Topic: $(DEMO_TOPIC), Group: $(DEMO_GROUP)"
	@echo "=== Broken consumer: commit → process (at-most-once) ==="
	@echo "=== Killing mid-processing to demonstrate message loss ==="
	$(DEMO_ENV) uv run python -m src.producer
	$(DEMO_ENV) uv run python -m src.commit_before_process_consumer &
	@sleep 4
	@kill -9 %1 2>/dev/null || true
	@sleep 8
	$(DEMO_ENV) uv run python -m src.commit_before_process_consumer &
	@sleep 12
	@kill %1 2>/dev/null || true
	@echo ""
	@echo "=== Results (expect: messages lost): ==="
	@uv run python -c "import json; from src.helpers import read_processed_log, PRODUCER_LOG; entries = read_processed_log(); produced = set(d['key'] for d in json.load(open(PRODUCER_LOG))); processed = set(e['key'] for e in entries); print(f'Produced: {len(produced)}, Processed: {len(entries)}, Lost: {len(produced - processed)}')"
