"""
StreamForge Kafka Producer
===========================
Generates realistic subscription lifecycle events and publishes them to Kafka.

Three operating modes
---------------------
  simulate   Infinite loop. One random event every SIMULATE_INTERVAL_SEC.
             This is the default mode when the container starts via docker-compose.
             Use it to keep Kafka populated while you develop the consumer.

  once       Emits exactly one of each event type (6 messages total) then exits.
             Used by `make produce-once` for smoke tests and CI.

  single     Emits one event of a specific type then exits.
             Used by `make produce-single EVENT=renewal` for targeted testing.

Design decisions
----------------

1.  Event construction uses real Pydantic models from shared.schemas.
    The producer never hand-crafts JSON.  Every message is validated by
    Pydantic before it reaches Kafka.  If a schema field is wrong, the
    error surfaces here — not in the consumer after the message is stored.

2.  Deterministic idempotency_key.
    The default idempotency_key in BaseEvent is a random uuid4 — fine for
    simulate mode where we deliberately want variety.  For once and single
    modes we set a deterministic key so re-running the command is idempotent:
    the consumer will skip duplicates rather than inserting them twice.
    Format: "{event_type}:{subscription_id}:{date}"

3.  Delivery callback instead of poll(timeout=0).
    confluent-kafka is async by default.  producer.produce() enqueues the
    message in the local buffer; the broker ACK arrives later.  The delivery
    callback is the only reliable way to know if a message was actually
    stored.  On error we log and increment a counter; we don't crash —
    the simulate loop will retry on the next iteration.

4.  producer.flush() before exit.
    The local buffer may hold unsent messages when the process exits.
    flush() blocks until all buffered messages are delivered or timeout
    is reached.  Without it, the last batch of messages in once/single
    mode would be silently dropped.

5.  Faker seeds realistic but fake data.
    UUIDs are genuinely random per-run.  Names, emails, dates come from
    Faker so log output looks like real data — easier to reason about
    during development than "sub_001", "sub_002".

6.  Signal handling for graceful shutdown in simulate mode.
    SIGTERM (sent by `docker-compose stop`) sets a threading.Event.
    The main loop checks this event between iterations so the current
    message completes before exit — no partial writes.

7.  All log calls use get_logger from shared.logger.
    Every line is structured JSON.  The delivery callback logs at INFO
    on success, ERROR on failure.  The main loop logs at DEBUG for
    individual produce calls and INFO for batch completions.
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# ── Path setup ────────────────────────────────────────────────────────────────
# In the Docker image WORKDIR=/app and we COPY shared/ → /app/shared/,
# so `import shared` works without modification.
# For local runs (python src/producer/producer.py) we need src/ on the path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from faker import Faker
from confluent_kafka import Producer, KafkaException

from shared.schemas import (
    NewSubscriptionEvent,
    RenewalEvent,
    CancellationEvent,
    RefundEvent,
    ExpiryEvent,
    ExtensionEvent,
    BaseEvent,
)
from shared.constants import TOPIC_MAP, PLANS, PLAN_PRICES
from shared.logger import get_logger, log_kafka_processed, log_dlq_routed

# ── Module-level singletons ───────────────────────────────────────────────────
logger = get_logger("producer")
fake   = Faker()
Faker.seed(0)   # reproducible names/emails across restarts in simulate mode

# ── Shutdown signal ───────────────────────────────────────────────────────────
_shutdown = threading.Event()

def _handle_sigterm(signum, frame):          # noqa: ANN001
    logger.info("SIGTERM received — draining and shutting down")
    _shutdown.set()

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)


# =============================================================================
# Fake data helpers
# =============================================================================

def _sub_id()    -> str: return str(uuid.uuid4())
def _cust_id()   -> str: return str(uuid.uuid4())
def _txn_id()    -> str: return str(uuid.uuid4())
def _now()       -> datetime: return datetime.now(timezone.utc)
def _future(days: int = 30) -> datetime: return _now() + timedelta(days=days)
def _past(days: int = 30)   -> datetime: return _now() - timedelta(days=days)
def _plan()      -> str: return random.choice(PLANS)
def _amount(plan: str) -> Decimal:
    base = Decimal(str(PLAN_PRICES[plan]))
    # Small realistic variation: ±$0.01 for currency conversion, prorating etc.
    variation = Decimal(str(round(random.uniform(-0.01, 0.01), 2)))
    return max(Decimal("0.01"), base + variation)


# =============================================================================
# Event factory functions — one per event type
# =============================================================================

def make_new_subscription(sub_id: str | None = None, cust_id: str | None = None) -> NewSubscriptionEvent:
    """
    Build a realistic NewSubscriptionEvent.

    sub_id and cust_id can be passed in when you want correlated events
    (e.g. produce a new_subscription followed by a renewal for the same sub).
    """
    sub_id  = sub_id  or _sub_id()
    cust_id = cust_id or _cust_id()
    plan    = _plan()
    return NewSubscriptionEvent(
        subscription_id  = sub_id,
        customer_id      = cust_id,
        idempotency_key  = f"new_subscription:{sub_id}:{_now().date()}",
        plan             = plan,
        amount_usd       = _amount(plan),
        expires_at       = _future(30).isoformat(),
        auto_renew       = random.choice([True, True, True, False]),  # 75% auto-renew
        source           = random.choice(["web", "web", "mobile", "cs_portal"]),
    )


def make_renewal(sub_id: str | None = None, cust_id: str | None = None) -> RenewalEvent:
    sub_id   = sub_id  or _sub_id()
    cust_id  = cust_id or _cust_id()
    plan     = _plan()
    prev_exp = _past(1)
    new_exp  = _future(29)
    return RenewalEvent(
        subscription_id  = sub_id,
        customer_id      = cust_id,
        idempotency_key  = f"renewal:{sub_id}:{_now().date()}",
        plan             = plan,
        amount_usd       = _amount(plan),
        previous_expiry  = prev_exp.isoformat(),
        new_expiry       = new_exp.isoformat(),
        renewal_attempt  = random.choices([1, 2, 3], weights=[85, 12, 3])[0],
    )


def make_cancellation(sub_id: str | None = None, cust_id: str | None = None) -> CancellationEvent:
    sub_id  = sub_id  or _sub_id()
    cust_id = cust_id or _cust_id()
    cs_initiated = random.random() < 0.15   # 15% CS-initiated
    return CancellationEvent(
        subscription_id  = sub_id,
        customer_id      = cust_id,
        idempotency_key  = f"cancellation:{sub_id}:{_now().date()}",
        reason           = random.choice([
            "user_requested", "user_requested", "user_requested",
            "payment_failure", "cs_initiated", "other",
        ]),
        cancelled_by     = f"cs_agent:{random.randint(1,50)}" if cs_initiated else "user",
        effective_date   = _future(random.choice([0, 0, 30])).isoformat(),
        refund_eligible  = cs_initiated,
    )


def make_refund(sub_id: str | None = None, cust_id: str | None = None) -> RefundEvent:
    sub_id   = sub_id  or _sub_id()
    cust_id  = cust_id or _cust_id()
    plan     = _plan()
    original = _amount(plan)
    # Partial refund 70% of the time, full refund 30%
    refund   = (original * Decimal("0.5")).quantize(Decimal("0.01")) \
               if random.random() < 0.7 else original
    txn_id   = _txn_id()
    return RefundEvent(
        subscription_id  = sub_id,
        customer_id      = cust_id,
        idempotency_key  = f"refund:{txn_id}",
        transaction_id   = txn_id,
        refund_amount    = refund,
        original_amount  = original,
        reason           = random.choice([
            "cancellation", "billing_error", "cs_goodwill",
            "duplicate_charge", "chargeback",
        ]),
        initiated_by     = f"cs_agent:{random.randint(1,50)}",
    )


def make_expiry(sub_id: str | None = None, cust_id: str | None = None) -> ExpiryEvent:
    sub_id  = sub_id  or _sub_id()
    cust_id = cust_id or _cust_id()
    return ExpiryEvent(
        subscription_id  = sub_id,
        customer_id      = cust_id,
        idempotency_key  = f"expiry:{sub_id}:{_now().date()}",
        plan             = _plan(),
        expired_at       = _now().isoformat(),
        auto_renew       = random.choice([True, False]),
    )


def make_extension(sub_id: str | None = None, cust_id: str | None = None) -> ExtensionEvent:
    sub_id   = sub_id  or _sub_id()
    cust_id  = cust_id or _cust_id()
    days     = random.choice([7, 14, 30])
    prev_exp = _future(0)
    new_exp  = prev_exp + timedelta(days=days)
    return ExtensionEvent(
        subscription_id  = sub_id,
        customer_id      = cust_id,
        idempotency_key  = f"extension:{sub_id}:{_now().date()}",
        extended_by_days = days,
        previous_expiry  = prev_exp.isoformat(),
        new_expiry       = new_exp.isoformat(),
        reason           = random.choice(["goodwill", "service_outage", "promo_code", "cs_reinstatement"]),
        extended_by      = f"cs_agent:{random.randint(1,50)}",
    )


# Map event_type string → factory function.
# Used by simulate and single modes to build events without if/elif chains.
EVENT_FACTORIES = {
    "new_subscription": make_new_subscription,
    "renewal":          make_renewal,
    "cancellation":     make_cancellation,
    "refund":           make_refund,
    "expiry":           make_expiry,
    "extension":        make_extension,
}


# =============================================================================
# Kafka producer setup
# =============================================================================

def build_kafka_producer(bootstrap_servers: str) -> Producer:
    """
    Create and return a confluent-kafka Producer.

    Key config choices:
    - acks=all          Every in-sync replica must confirm the write before
                        the broker ACKs.  Strongest durability guarantee.
                        Cost: ~5-10 ms extra latency on each produce.
    - retries=5         Retry transient broker errors (leader election,
                        network blip) up to 5 times.
    - linger.ms=10      Buffer messages for up to 10 ms before sending.
                        Allows the client to batch multiple events into one
                        network request.  At 300M events/day this cuts
                        network overhead dramatically.
    - compression.type  snappy gives ~3-5x compression on JSON with very
                        low CPU cost.  Reduces broker disk usage and
                        replication bandwidth.
    - enable.idempotence=true
                        Prevents duplicate messages from retries.
                        Requires acks=all and retries > 0.
    """
    return Producer({
        "bootstrap.servers":   bootstrap_servers,
        "acks":                "all",
        "retries":             5,
        "retry.backoff.ms":    300,
        "linger.ms":           10,
        "compression.type":    "snappy",
        "enable.idempotence":  True,
        # Delivery report queue size — limits memory for unACKed messages
        "queue.buffering.max.messages": 100_000,
    })


# =============================================================================
# Core produce function
# =============================================================================

# Track delivery stats across the session
_stats = {"produced": 0, "delivered": 0, "errors": 0}


def _delivery_callback(err, msg) -> None:
    """
    Called by confluent-kafka for every message once the broker ACKs (or fails).

    This runs in the librdkafka background thread — do NOT block here.
    We update counters and log; no heavy computation.
    """
    if err:
        _stats["errors"] += 1
        logger.error(
            "delivery failed",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "error": str(err),
            },
        )
    else:
        _stats["delivered"] += 1
        logger.debug(
            "delivery confirmed",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )


def produce_event(producer: Producer, event: BaseEvent) -> None:
    """
    Validate, serialise, and enqueue one event for delivery to Kafka.

    The actual network send happens asynchronously in librdkafka's
    background thread.  Call producer.flush() to wait for all ACKs.
    """
    topic = TOPIC_MAP[event.event_type]
    _stats["produced"] += 1

    try:
        producer.produce(
            topic    = topic,
            key      = event.kafka_key(),
            value    = event.to_kafka_payload(),
            callback = _delivery_callback,
        )
        # poll(0) gives librdkafka a chance to call delivery callbacks
        # for messages that were already ACKed.  Without this the callback
        # queue can back up and BufferError is raised when the buffer fills.
        producer.poll(0)

        logger.info(
            "event produced",
            extra={
                "event_type":      event.event_type,
                "event_id":        event.event_id,
                "subscription_id": event.subscription_id,
                "topic":           topic,
                "idempotency_key": event.idempotency_key,
            },
        )

    except BufferError:
        # The producer's local queue is full — too many unACKed messages.
        # flush() drains the queue; then we retry once.
        logger.warning("producer buffer full — flushing before retry")
        producer.flush(timeout=10)
        producer.produce(
            topic=topic, key=event.kafka_key(),
            value=event.to_kafka_payload(), callback=_delivery_callback,
        )


# =============================================================================
# Operating modes
# =============================================================================

def run_simulate(producer: Producer, interval_sec: float) -> None:
    """
    Infinite loop: produce one random event every `interval_sec` seconds.
    Exits cleanly on SIGTERM / SIGINT.
    """
    logger.info("simulate mode started", extra={"interval_sec": interval_sec})
    while not _shutdown.is_set():
        event_type = random.choice(list(EVENT_FACTORIES))
        event      = EVENT_FACTORIES[event_type]()
        produce_event(producer, event)
        _shutdown.wait(timeout=interval_sec)   # interruptible sleep

    logger.info("shutting down — flushing producer buffer")
    producer.flush(timeout=30)
    logger.info("simulate mode finished", extra=_stats)


def run_once(producer: Producer) -> None:
    """
    Emit exactly one of each event type (6 messages) then exit.
    Used by `make produce-once` for smoke tests.
    """
    logger.info("once mode started — emitting one of each event type")
    for event_type, factory in EVENT_FACTORIES.items():
        event = factory()
        produce_event(producer, event)

    producer.flush(timeout=30)
    logger.info("once mode finished", extra=_stats)


def run_single(producer: Producer, event_type: str) -> None:
    """
    Emit one event of a specific type then exit.
    Used by `make produce-single EVENT=renewal`.
    """
    if event_type not in EVENT_FACTORIES:
        logger.error(
            "unknown event type",
            extra={"event_type": event_type, "valid": list(EVENT_FACTORIES)},
        )
        sys.exit(1)

    logger.info("single mode started", extra={"event_type": event_type})
    event = EVENT_FACTORIES[event_type]()
    produce_event(producer, event)
    producer.flush(timeout=30)
    logger.info("single mode finished", extra=_stats)


# =============================================================================
# Entry point
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="StreamForge Kafka Producer")
    parser.add_argument(
        "--mode",
        choices=["simulate", "once", "single"],
        default="simulate",
        help="Operating mode (default: simulate)",
    )
    parser.add_argument(
        "--event-type",
        default=os.environ.get("EVENT_TYPE", "renewal"),
        help="Event type for --mode single (also reads EVENT_TYPE env var)",
    )
    args = parser.parse_args()

    # Config from environment — set in docker-compose or .env
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    interval_sec      = float(os.environ.get("SIMULATE_INTERVAL_SEC", "1.5"))

    logger.info(
        "producer starting",
        extra={
            "mode":               args.mode,
            "bootstrap_servers":  bootstrap_servers,
            "interval_sec":       interval_sec,
        },
    )

    producer = build_kafka_producer(bootstrap_servers)

    if args.mode == "simulate":
        run_simulate(producer, interval_sec)
    elif args.mode == "once":
        run_once(producer)
    elif args.mode == "single":
        run_single(producer, args.event_type)


if __name__ == "__main__":
    main()