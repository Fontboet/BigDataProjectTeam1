FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python pandas lz4

COPY kafka_producer.py /app/kafka_producer.py

COPY ./flights.csv /data/flights.csv

ENV CSV_FILE=/data/flights.csv

CMD ["python", "kafka_producer.py"]
