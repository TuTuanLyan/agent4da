## View topic in kafka:lastest
```bash
docker exec --workdir /opt/kafka/bin kafka-kraft ./kafka-topics.sh --list --bootstrap-server localhost:9092
```

## View spec topic
```bash
docker exec --workdir /opt/kafka/bin kafka-kraft ./kafka-topics.sh --describe --topic <topic-name>--bootstrap-server localhost:9092
```
## View msg
```bash
docker exec --workdir /opt/kafka/bin kafka-kraft ./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ecommerce_raw \
  --from-beginning \
  --max-messages 20
```

## Create
```bash
docker exec --workdir /opt/kafka/bin kafka-kraft ./kafka-topics.sh --create --topic ecommerce_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```