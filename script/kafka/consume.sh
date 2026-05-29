#!/bin/bash
# Usage: ./script/consume.sh [max_messages]
# Example: ./script/consume.sh 10

MAX_MSG="${1:-5}"
TOPIC="${KAFKA_TOPIC:-ecommerce_events}"

echo "==> Consuming $MAX_MSG messages from $TOPIC"
docker exec kafka-kraft /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-kraft:29092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages "$MAX_MSG"