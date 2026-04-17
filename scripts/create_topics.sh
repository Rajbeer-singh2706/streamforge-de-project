#!/bin/bash
set -e

BROKER="kafka:29092"
PARTITIONS=3
REPLICATION=1
RETENTION_MS=604800000   # 7 days

echo "==> Wiating for kafka to be ready..."
sleep 5 

create_topic() {
  local topic=$1
  echo "  Creating topic: $topic"
  kafka-topics --bootstrap-server $BROKER \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION \
    --config retention.ms=$RETENTION_MS
}


echo "===> Creating Lifecycle event topics.."
create_topic "sub.new_subscription"
create_topic "sub.renewal"
create_topic "sub.cancellation"
create_topic "sub.refund"
create_topic "sub.expiry"
create_topic "sub.extension"

echo "==> Creating dead letter topics..."
create_topic "sub.new_subscription.dlq"
create_topic "sub.renewal.dlq"
create_topic "sub.cancellation.dlq"
create_topic "sub.refund.dlq"
create_topic "sub.expiry.dlq"
create_topic "sub.extension.dlq"

echo "==> Creating internal topics..."
create_topic "sub.audit_log"


echo ""
echo "==> All topics created. Listing:"
kafka-topics --bootstrap-server $BROKER --list