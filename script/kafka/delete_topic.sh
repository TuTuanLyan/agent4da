#!/bin/bash
TOPIC="$1"
if [ -z "$TOPIC" ]; then
  echo "Usage: $0 <topic_name>"
  exit 1
fi
echo "==> Deleting topic $TOPIC"
docker exec kafka-kraft /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-kraft:29092 \
  --delete --topic "$TOPIC"