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

## Produce CSV

Producer vẫn chạy thủ công theo file CSV và không chia batch theo ngày/tháng.
Mỗi record được thêm metadata:

- `source_file`
- `ingestion_batch_id`
- `chunk_id`
- `ingest_time`

Wrapper:

```bash
PRODUCER_CHUNK_SIZE=10000 ./script/kafka/producer.sh event_test_1000.csv
```

Tuỳ chọn batch id cố định để debug/trace:

```bash
PRODUCER_BATCH_ID=manual-test-001 ./script/kafka/producer.sh event_test_1000.csv
```

Khi gọi trực tiếp Python:

```bash
python code/kafka/producer.py \
  --file data/event_test_1000.csv \
  --broker localhost:9092 \
  --topic ecommerce_events \
  --chunk-size 10000 \
  --batch-id manual-test-001
```

`--batch` là alias backward-compatible của `--batch-id`:

```bash
python code/kafka/producer.py --file data/event_test_1000.csv --batch manual-test-001
```

Ý nghĩa:

- `--batch-id`/`--batch` chỉ set metadata `ingestion_batch_id` trên từng record.
- Nếu không truyền, producer tự sinh UUID cho mỗi lần chạy.
- Đây không phải batch theo ngày/tháng và không ảnh hưởng Kafka offset.
- `--chunk-size` chỉ quyết định producer flush sau bao nhiêu dòng; `chunk_id`
  tăng theo nhóm dòng trong file để dễ quan sát file lớn.
- Batch xử lý nghiệp vụ được xác định sau đó ở Spark bằng `event_date` parse từ
  `event_time`.
