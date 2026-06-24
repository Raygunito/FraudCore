#!/bin/bash

set -e

echo "Creating Kafka topics for FraudCore..."

# Input stream (producer → spark)
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-broker:29092 \
  --create \
  --if-not-exists \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1

# Output stream (spark → sinks)
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-broker:29092 \
  --create \
  --if-not-exists \
  --topic fraud-alerts \
  --partitions 3 \
  --replication-factor 1

echo "Topics created successfully."

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-broker:29092 \
  --list