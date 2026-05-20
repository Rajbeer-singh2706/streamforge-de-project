#!/bin/bash
set -e

BROKER="kafka:29092"
PARTITIONS=3
REPLICATION=1
RETENTION_MS=604800000

echo "==> Waiting for Kafka..."
sleep 5

create_topic() {
  local topic=$1
  echo "  Creating: $topic"
  kafka-topics --bootstrap-server $BROKER \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION \
    --config retention.ms=$RETENTION_MS
}

echo "==> Lifecycle topics"
create_topic "sub.new_subscription"
create_topic "sub.renewal"
create_topic "sub.cancellation"
create_topic "sub.refund"
create_topic "sub.expiry"
create_topic "sub.extension"

echo "==> Dead letter queues"
create_topic "sub.new_subscription.dlq"
create_topic "sub.renewal.dlq"
create_topic "sub.cancellation.dlq"
create_topic "sub.refund.dlq"
create_topic "sub.expiry.dlq"
create_topic "sub.extension.dlq"

echo "==> Audit topic"
create_topic "sub.audit_log"

echo ""
echo "==> Done. All topics:"
kafka-topics --bootstrap-server $BROKER --list