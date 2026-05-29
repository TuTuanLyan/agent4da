#!/bin/bash
echo "==> Listing topics"
docker exec kafka-kraft /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-kraft:29092 \
  --list