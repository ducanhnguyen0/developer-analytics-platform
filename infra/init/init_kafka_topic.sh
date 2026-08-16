#!/usr/bin/env bash
# Run once, after `docker-compose up -d` and the brokers are healthy.

set -e

TOPIC="github-events"
PARTITIONS=2
REPLICATION_FACTOR=1

echo "==> Creating topic '$TOPIC' (partitions=$PARTITIONS, replication=$REPLICATION_FACTOR)..."
docker exec kafka kafka-topics \
  --bootstrap-server kafka:19092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

echo "==> Topic details:"
docker exec kafka kafka-topics \
  --bootstrap-server kafka:19092 \
  --describe \
  --topic "$TOPIC"
