#!/bin/bash
# Usage: ./script/kafka/producer.sh <csv_file>
# Example: ./script/kafka/producer.sh event_test_1000.csv

set -e

CSV_FILE="${1:-event_test_1000.csv}"
BROKER="${KAFKA_BROKER:-kafka-kraft:29092}"
TOPIC="${KAFKA_TOPIC:-ecommerce_events}"
CHUNK_SIZE="${PRODUCER_CHUNK_SIZE:-10000}"

echo "==> Producing $CSV_FILE to $TOPIC on $BROKER"
CMD=(docker exec airflow python /opt/project/code/kafka/producer.py
  --file "/opt/project/data/$CSV_FILE" \
  --broker "$BROKER" \
  --topic "$TOPIC" \
  --chunk-size "$CHUNK_SIZE")

if [ -n "${PRODUCER_BATCH_ID:-}" ]; then
  CMD+=(--batch-id "$PRODUCER_BATCH_ID")
fi

"${CMD[@]}"
