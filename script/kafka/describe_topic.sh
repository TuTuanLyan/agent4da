#!/bin/bash
TOPIC="${1:-ecommerce_events}"
echo "==> Describing topic $TOPIC"
docker exec kafka-kraft /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-kraft:29092 \
  --describe --topic "$TOPIC"