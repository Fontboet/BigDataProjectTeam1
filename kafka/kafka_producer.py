import os
import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    acks=1,                     # faster (tradeoff: lower durability). Use 'all' for safety.
    compression_type='lz4',     # reduce network usage (requires kafka broker support)
    linger_ms=100,              # wait up to 100ms to batch records
    batch_size=64 * 1024,       # larger batch size (64KB)
    max_request_size=2 * 1024 * 1024,
)

def produce_messages(file_path, topic, flush_every=1000, stop_row=1000):
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return
    sent = 0
    start = time.time()
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if stop_row and sent >= stop_row:
                    break
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

if __name__ == "__main__":
    file_path = "data/flights.csv"
    topic = 'bdsp_topic_test'
    print("Starting to produce messages...")
    try:
        produce_messages(file_path, topic, flush_every=1000, stop_row=1000)
    finally:
        try:
            producer.close()
            print("Producer closed.")
        except Exception:
            pass