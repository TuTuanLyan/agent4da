import argparse
import csv
import json
import os
import sys
import time
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

def send_csv(producer: KafkaProducer, filepath: str, topic: str,
             delay: float = 0, log_every: int = 200) -> int:
    if not os.path.exists(filepath):
        print(f"[Producer] ERROR: File not found: {filepath}")
        sys.exit(1)

    sent = 0
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(topic, value=row)
            sent += 1
            if sent % log_every == 0:
                print(f"[Producer] Sent {sent} messages...")
            if delay > 0:
                time.sleep(delay)

    return sent


def get_parser():
    parser = argparse.ArgumentParser(description="Kafka CSV Producer")

    parser.add_argument("--file", required=True, help="path to input CSV file")
    parser.add_argument("--broker", default="localhost:9092",help="Kafka broker (default: localhost:9092)")
    parser.add_argument("--topic", default="ecommerce_events", help=f"Kafka topic (default: ecommerce_events)")
    parser.add_argument("--delay", type=float, default=0, help="Delay between messages (seconds, default: 0)")
    parser.add_argument("--log-every", type=int, default=200, help="Print log every N messages (default: 200)")

    return parser.parse_args()

def main():
    args = get_parser()
    producer = create_producer(args.broker)

    print(f"  File  : {args.file}")
    print(f"  Broker: {args.broker}")
    print(f"  Topic : {args.topic}")

    producer = create_producer(args.broker)
    start = time.time()
    sent = send_csv(producer, args.file, args.topic, args.delay, args.log_every)

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    print(f"[Producer] Done. {sent} messages in {elapsed:.1f}s "
          f"({sent/elapsed:.0f} msg/s)")


if __name__ == "__main__":
    main()