import os
import json
import time
import random
from datetime import datetime

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


def make_message(i: int):
    airlines = ["AA", "DL", "UA", "WN", "AS"]
    airports = ["JFK", "LAX", "SFO", "SEA", "ORD", "ATL"]

    origin = random.choice(airports)
    dest = random.choice([a for a in airports if a != origin])
    airline = random.choice(airlines)

    return {
        "YEAR": 2024,
        "MONTH": random.randint(1, 12),
        "DAY": random.randint(1, 28),
        "DAY_OF_WEEK": random.randint(1, 7),
        "AIRLINE": airline,
        "FLIGHT_NUMBER": 1000 + i,
        "TAIL_NUMBER": f"N{random.randint(10000,99999)}",
        "ORIGIN_AIRPORT": origin,
        "DESTINATION_AIRPORT": dest,
        "SCHEDULED_DEPARTURE": random.randint(0, 2359),
        "DEPARTURE_TIME": random.randint(0, 2359),
        "DEPARTURE_DELAY": random.randint(-20, 120),
        "TAXI_OUT": random.randint(5, 40),
        "WHEELS_OFF": random.randint(0, 2359),
        "SCHEDULED_TIME": random.randint(30, 400),
        "ELAPSED_TIME": random.randint(30, 400),
        "AIR_TIME": random.randint(20, 350),
        "DISTANCE": random.randint(200, 3000),
        "WHEELS_ON": random.randint(0, 2359),
        "TAXI_IN": random.randint(5, 40),
        "SCHEDULED_ARRIVAL": random.randint(0, 2359),
        "ARRIVAL_TIME": random.randint(0, 2359),
        "ARRIVAL_DELAY": random.randint(-20, 120),
        "DIVERTED": 0,
        "CANCELLED": 0,
        "CANCELLATION_REASON": None,
        "AIR_SYSTEM_DELAY": 0,
        "SECURITY_DELAY": 0,
        "AIRLINE_DELAY": 0,
        "LATE_AIRCRAFT_DELAY": 0,
        "WEATHER_DELAY": 0,
        "_sent_at": datetime.utcnow().isoformat() + "Z",
    }


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "bdsp_topic_test")

    if not wait_for_kafka(bootstrap):
        raise RuntimeError("Kafka not reachable within timeout")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="all",
    )

    i = 0
    while True:
        msg = make_message(i)
        producer.send(topic, value=msg)
        i += 1
        if i % 100 == 0:
            producer.flush()
        time.sleep(0.5)


if __name__ == "__main__":
    main()

