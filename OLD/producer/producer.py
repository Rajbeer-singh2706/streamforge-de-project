"""
StreamForge — Event Producer (Module 1)

Modes:
  --mode simulate   Continuously emit random subscription events (dev/demo)
  --mode once       Emit one of each event type then exit
  --mode single     Emit a single event passed via --event <type>

Usage:
  python producer.py --mode simulate
  python producer.py --mode once
  python producer.py --mode single --event new_subscription
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from confluent_kafka import Producer, KafkaException
from faker import Faker

from producer.schemas import (
    NewSubscriptionEvent, RenewalEvent, CancellationEvent,
    RefundEvent, ExpiryEvent, ExtensionEvent,
    TOPIC_MAP, DLQ_TOPIC_MAP,
)

# ── Config ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PLANS = ["basic", "premium", "enterprise"]
PLAN_PRICES = {"basic": 4.99, "premium": 9.99, "enterprise": 29.99}
fake = Faker()

# Seed customer/subscription IDs for simulation
SEED_CUSTOMERS = [str(uuid4()) for _ in range(50)]
SEED_SUBSCRIPTIONS = {cid: str(uuid4()) for cid in SEED_CUSTOMERS}


# ── Kafka Producer ────────────────────────────────────────────────────────────

def make_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",                   # wait for all replicas
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",
        "linger.ms": 10,                 # small batch window
        "batch.size": 16384,
    }
    return Producer(conf)


def delivery_callback(err, msg):
    if err:
        log.error("Delivery FAILED | topic=%s key=%s error=%s",
                  msg.topic(), msg.key(), err)
    else:
        log.debug("Delivered | topic=%-30s partition=%d offset=%d key=%s",
                  msg.topic(), msg.partition(), msg.offset(),
                  msg.key().decode() if msg.key() else None)


def publish(producer: Producer, event, topic_override: str | None = None):
    topic = topic_override or TOPIC_MAP[event.event_type]
    payload = event.model_dump_json().encode("utf-8")
    key = event.kafka_key().encode("utf-8")
    try:
        producer.produce(topic, value=payload, key=key, callback=delivery_callback)
        producer.poll(0)   # trigger callbacks without blocking
    except KafkaException as exc:
        log.error("Produce failed, routing to DLQ | error=%s", exc)
        producer.produce(
            DLQ_TOPIC_MAP[event.event_type],
            value=payload,
            key=key,
        )
    log.info("Published | %-20s → %-35s sub=%s",
             event.event_type, topic, event.subscription_id[:8])


# ── Event Factories ───────────────────────────────────────────────────────────

def _future(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()

def _past(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def make_new_subscription(customer_id: str | None = None,
                          sub_id: str | None = None) -> NewSubscriptionEvent:
    cid = customer_id or random.choice(SEED_CUSTOMERS)
    plan = random.choice(PLANS)
    return NewSubscriptionEvent(
        subscription_id=sub_id or SEED_SUBSCRIPTIONS[cid],
        customer_id=cid,
        plan=plan,
        amount_usd=PLAN_PRICES[plan],
        expires_at=_future(30),
        auto_renew=random.choice([True, True, False]),  # 66% auto-renew
    )


def make_renewal(customer_id: str | None = None) -> RenewalEvent:
    cid = customer_id or random.choice(SEED_CUSTOMERS)
    plan = random.choice(PLANS)
    return RenewalEvent(
        subscription_id=SEED_SUBSCRIPTIONS[cid],
        customer_id=cid,
        plan=plan,
        amount_usd=PLAN_PRICES[plan],
        previous_expiry=_past(1),
        new_expiry=_future(30),
        renewal_attempt=random.randint(1, 3),
    )


def make_cancellation(customer_id: str | None = None) -> CancellationEvent:
    cid = customer_id or random.choice(SEED_CUSTOMERS)
    reasons = ["user_requested", "payment_failed", "downgrade", "competition"]
    return CancellationEvent(
        subscription_id=SEED_SUBSCRIPTIONS[cid],
        customer_id=cid,
        reason=random.choice(reasons),
        cancelled_by=random.choice(["customer", "customer", "cs_agent"]),
        refund_eligible=random.random() < 0.3,
    )


def make_refund(customer_id: str | None = None) -> RefundEvent:
    cid = customer_id or random.choice(SEED_CUSTOMERS)
    original = random.choice(list(PLAN_PRICES.values()))
    return RefundEvent(
        subscription_id=SEED_SUBSCRIPTIONS[cid],
        customer_id=cid,
        transaction_id=str(uuid4()),
        refund_amount=round(original * random.uniform(0.3, 1.0), 2),
        original_amount=original,
        reason=random.choice(["customer_request", "duplicate_charge", "service_issue"]),
        initiated_by=random.choice(["cs_agent", "system"]),
    )


def make_expiry(customer_id: str | None = None) -> ExpiryEvent:
    cid = customer_id or random.choice(SEED_CUSTOMERS)
    return ExpiryEvent(
        subscription_id=SEED_SUBSCRIPTIONS[cid],
        customer_id=cid,
        plan=random.choice(PLANS),
        auto_renew=random.choice([True, False]),
    )


def make_extension(customer_id: str | None = None) -> ExtensionEvent:
    cid = customer_id or random.choice(SEED_CUSTOMERS)
    days = random.choice([7, 14, 30])
    return ExtensionEvent(
        subscription_id=SEED_SUBSCRIPTIONS[cid],
        customer_id=cid,
        extended_by_days=days,
        previous_expiry=_future(0),
        new_expiry=_future(days),
        reason=random.choice(["cs_goodwill", "service_outage", "billing_error"]),
        extended_by=f"cs_agent_{random.randint(1, 5)}",
    )


EVENT_FACTORIES = {
    "new_subscription": make_new_subscription,
    "renewal":          make_renewal,
    "cancellation":     make_cancellation,
    "refund":           make_refund,
    "expiry":           make_expiry,
    "extension":        make_extension,
}

# Realistic event distribution weights
EVENT_WEIGHTS = {
    "new_subscription": 30,
    "renewal":          35,
    "cancellation":     10,
    "refund":           5,
    "expiry":           15,
    "extension":        5,
}


# ── Modes ─────────────────────────────────────────────────────────────────────

def run_simulate(producer: Producer, interval_sec: float = 1.5):
    """Emit random events continuously — simulates real traffic."""
    log.info("=== StreamForge Producer: SIMULATE mode (interval=%.1fs) ===", interval_sec)
    event_types = list(EVENT_WEIGHTS.keys())
    weights = list(EVENT_WEIGHTS.values())
    count = 0
    try:
        while True:
            etype = random.choices(event_types, weights=weights, k=1)[0]
            event = EVENT_FACTORIES[etype]()
            publish(producer, event)
            count += 1
            if count % 20 == 0:
                log.info("── Throughput checkpoint: %d events published ──", count)
                producer.flush()
            time.sleep(interval_sec + random.uniform(-0.3, 0.3))
    except KeyboardInterrupt:
        log.info("Stopping. Flushing remaining messages...")
        producer.flush(30)
        log.info("Done. Total events published: %d", count)


def run_once(producer: Producer):
    """Emit exactly one of each event type."""
    log.info("=== StreamForge Producer: ONCE mode ===")
    for etype, factory in EVENT_FACTORIES.items():
        event = factory()
        publish(producer, event)
    producer.flush(10)
    log.info("One of each event type published successfully.")


def run_single(producer: Producer, event_type: str):
    """Emit a single specific event type."""
    if event_type not in EVENT_FACTORIES:
        raise ValueError(f"Unknown event type: {event_type}. Choose from {list(EVENT_FACTORIES)}")
    event = EVENT_FACTORIES[event_type]()
    publish(producer, event)
    producer.flush(10)
    log.info("Single event published: %s", event_type)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="StreamForge Kafka Producer")
    parser.add_argument("--mode", choices=["simulate", "once", "single"],
                        default="simulate", help="Producer mode")
    parser.add_argument("--event", help="Event type (for --mode single)")
    parser.add_argument("--interval", type=float, default=1.5,
                        help="Seconds between events in simulate mode")
    args = parser.parse_args()

    log.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP)
    producer = make_producer()

    if args.mode == "simulate":
        run_simulate(producer, args.interval)
    elif args.mode == "once":
        run_once(producer)
    elif args.mode == "single":
        if not args.event:
            parser.error("--event is required with --mode single")
        run_single(producer, args.event)


if __name__ == "__main__":
    main()
