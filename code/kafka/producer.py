import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def create_producer(broker: str) -> KafkaProducer:
    print(f"[Producer] Connecting to Kafka at {broker} ...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=10,
            acks="all",
        )
        print("[Producer] Connected successfully.")
        return producer
    except NoBrokersAvailable:
        print(f"[Producer] ERROR: Cannot reach Kafka at {broker}")
        print("  - Make sure Kafka container is running")
        sys.exit(1)

def send_csv(
    producer: KafkaProducer,
    filepath: str,
    topic: str,
    delay: float = 0,
    log_every: int = 200,
    chunk_size: int = 10000,
    ingestion_batch_id: str | None = None,
) -> int:
    if not os.path.exists(filepath):
        print(f"[Producer] ERROR: File not found: {filepath}")
        sys.exit(1)

    source_file = os.path.basename(filepath)
    batch_id = ingestion_batch_id or str(uuid.uuid4())
    ingest_time = datetime.now(timezone.utc).isoformat(timespec="seconds")
    chunk_size = max(int(chunk_size or 0), 1)

    print(f"[Producer] Batch id   : {batch_id}")
    print(f"[Producer] Source file: {source_file}")
    print(f"[Producer] Chunk size : {chunk_size}")

    sent = 0
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            chunk_id = (sent // chunk_size) + 1
            event = dict(row)
            event.update(
                {
                    "source_file": source_file,
                    "ingestion_batch_id": batch_id,
                    "chunk_id": str(chunk_id),
                    "ingest_time": ingest_time,
                }
            )
            producer.send(topic, value=event)
            sent += 1
            if sent % log_every == 0:
                print(f"[Producer] Sent {sent} messages...")
            if sent % chunk_size == 0:
                producer.flush()
                print(f"[Producer] Flushed chunk {chunk_id} ({sent} messages).")
            if delay > 0:
                time.sleep(delay)

    return sent


def get_parser(argv=None):
    parser = argparse.ArgumentParser(description="Kafka CSV Producer")

    parser.add_argument("--file", required=True, help="path to input CSV file")
    parser.add_argument("--broker", default="localhost:9092",help="Kafka broker (default: localhost:9092)")
    parser.add_argument("--topic", default="ecommerce_events", help=f"Kafka topic (default: ecommerce_events)")
    parser.add_argument("--delay", type=float, default=0, help="Delay between messages (seconds, default: 0)")
    parser.add_argument("--log-every", type=int, default=200, help="Print log every N messages (default: 200)")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Flush producer every N rows (default: 10000)")
    parser.add_argument(
        "--batch-id",
        "--batch",
        dest="batch_id",
        default=None,
        help="Optional ingestion_batch_id; default is a UUID",
    )

    return parser.parse_args(argv)

def main():
    args = get_parser()
    producer = create_producer(args.broker)

    print(f"  File  : {args.file}")
    print(f"  Broker: {args.broker}")
    print(f"  Topic : {args.topic}")

    start = time.time()
    sent = send_csv(
        producer,
        args.file,
        args.topic,
        delay=args.delay,
        log_every=args.log_every,
        chunk_size=args.chunk_size,
        ingestion_batch_id=args.batch_id,
    )

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    rate = sent / elapsed if elapsed > 0 else sent
    print(f"[Producer] Done. {sent} messages in {elapsed:.1f}s "
          f"({rate:.0f} msg/s)")


if __name__ == "__main__":
    main()
