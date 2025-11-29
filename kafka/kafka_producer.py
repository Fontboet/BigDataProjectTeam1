import os
import csv
import json
import time
from kafka import KafkaProducer

def wait_for_kafka(bootstrap_servers: str, timeout_sec: int = 120):
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            p = KafkaProducer(bootstrap_servers=bootstrap_servers)
            p.close()
            return True
        except Exception:
            time.sleep(2)
    return False

def produce_messages(file_path, producer, topic, flush_every=1000):
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return
    sent = 0
    start = time.time()
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # async send; add simple error callback
                future = producer.send(topic, value=row)
                future.add_errback(lambda exc: print(f"Send failed: {exc}"))
                sent += 1
                if sent % flush_every == 0:
                    producer.flush()   # flush periodically to keep memory bounded
        producer.flush()
    except Exception as e:
        print(f"Error while producing messages: {e}")
    finally:
        elapsed = time.time() - start
        if elapsed > 0:
            print(f"Sent {sent} messages in {elapsed:.2f}s ({sent/elapsed:.1f} msg/s)")

def main(file_path: str):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "flights_topic")

    if not wait_for_kafka(bootstrap):
        raise RuntimeError("Kafka not reachable within timeout")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        acks=1,                     # faster (tradeoff: lower durability). Use 'all' for safety.
        compression_type='lz4',     # reduce network usage (requires kafka broker support)
        linger_ms=100,              # wait up to 100ms to batch records
        batch_size=64 * 1024,       # larger batch size (64KB)
        max_request_size=2 * 1024 * 1024,
    )

    produce_messages(file_path, producer, topic, flush_every=1000)

    try:
        producer.close()
        print("Producer closed.")
    except Exception:
        pass

if __name__ == "__main__":
    file_path = "data/flights.csv"
    print("Starting to produce messages...")
    main(file_path)
    