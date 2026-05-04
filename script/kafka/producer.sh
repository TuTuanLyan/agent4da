#!/bin/bash
# Usage: ./script/produce.sh <csv_file>
# Example: ./script/produce.sh event_test_1000.csv

set -e

CSV_FILE="${1:-event_test_1000.csv}"
BROKER="${KAFKA_BROKER:-kafka-kraft:29092}"
TOPIC="${KAFKA_TOPIC:-ecommerce_events}"

echo "==> Producing $CSV_FILE to $TOPIC on $BROKER"
docker exec airflow python /opt/project/code/kafka/producer.py \
  --file "/opt/project/data/$CSV_FILE" \
  --broker "$BROKER" \
  --topic "$TOPIC"
