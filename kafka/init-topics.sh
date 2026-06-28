#!/bin/bash
set -e

echo "Waiting for Kafka..."

until /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-broker:29092 \
    --list >/dev/null 2>&1
do
    sleep 2
done

echo "Kafka is ready."

# Create topics
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-broker:29092 \
  --create --if-not-exists \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-broker:29092 \
  --create --if-not-exists \
  --topic fraud-alerts \
  --partitions 3 \
  --replication-factor 1

echo "Topics created."

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-broker:29092 \
  --list