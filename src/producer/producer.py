"""
StreamForge Kafka Producer
==========================
Generates realistic subscription lifecycle events and publishes them to Kafka.

Three operating modes (--mode flag):
  simulate  — infinite loop, one random event every SIMULATE_INTERVAL_SEC (default)
  once      — emit one of each of the 6 event types, then exit cleanly
  single    — emit one specific event type (--event <type>), then exit cleanly

Design principles:
  - FakeDataFactory holds shared UUID pools so events reference consistent
    subscription/customer IDs across simulate mode (renewals reference subs
    that were "created", not random orphans).
  - Weighted event distribution mirrors real antivirus SaaS traffic patterns.
  - Deterministic idempotency_key = sha256(sub_id + event_type + date) so
    producer restarts don't create phantom duplicates in the consumer.
  - Delivery callback on every produce() call — async Kafka produce means
    produce() returns before the broker ACKs; the callback is the only way
    to detect send failures.
  - poll(0) drains the callback queue in the simulate loop without blocking.
  - flush() after once/single ensures all in-flight messages are delivered
    before the process exits (avoids silent message loss on clean exit).
"""

import argparse
import hashlib
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from confluent_kafka import Producer, KafkaException
from faker import Faker

# ---------------------------------------------------------------------------
# Path bootstrap: allow `from shared.X import Y` when running inside Docker
# (WORKDIR /app, shared lives at /app/shared/) or locally from repo root.
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.constants import (
    TOPIC_MAP,
    PLANS,
    PLAN_PRICES,
    CONSUMER_GROUP_ID,
)
from shared.schemas import (
    NewSubscriptionEvent,
    RenewalEvent,
    CancellationEvent,
    RefundEvent,
    ExpiryEvent,
    ExtensionEvent,
    BaseEvent,
)
from shared.logger import get_logger, log_kafka_processed

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
logger = get_logger("producer", service="producer")
fake = Faker()

# Weighted distribution: mirrors real antivirus SaaS event ratios.
# renewals >> new_subscriptions >> expiry ≈ cancellation > refund > extension
EVENT_WEIGHTS: list[tuple[str, int]] = [
    ("renewal",          40),
    ("new_subscription", 25),
    ("expiry",           15),
    ("cancellation",     10),
    ("refund",            7),
    ("extension",         3),
]
EVENT_TYPES  = [e for e, _ in EVENT_WEIGHTS]
WEIGHTS      = [w for _, w in EVENT_WEIGHTS]

# Shutdown flag: set by SIGTERM/SIGINT handler so simulate loop exits cleanly
_shutdown = False


def _handle_signal(signum: int, frame) -> None:  # noqa: ANN001
    global _shutdown
    logger.info("Shutdown signal received — draining producer and exiting")
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ---------------------------------------------------------------------------
# FakeDataFactory
# ---------------------------------------------------------------------------
class FakeDataFactory:
    """
    Maintains pools of UUIDs so simulate mode produces correlated events.

    Without this, every event would reference a random UUID that has never
    been seen before, making the event stream incoherent (renewals for subs
    that don't exist, refunds with no prior transaction, etc.).

    The factory pre-generates POOL_SIZE customer + subscription pairs on
    init. Subsequent event builders draw from the same pool, so events
    for the same subscription_id are naturally correlated.
    """

    POOL_SIZE = 200  # large enough to avoid always hitting the same sub

    def __init__(self) -> None:
        # Pre-generate customer ↔ subscription pairs
        self._pool: list[dict] = [
            {
                "customer_id":    str(uuid4()),
                "subscription_id": str(uuid4()),
                "plan":           random.choice(PLANS),
            }
            for _ in range(self.POOL_SIZE)
        ]
        # Track "active" transaction IDs for realistic refund events
        self._transaction_ids: list[str] = [str(uuid4()) for _ in range(50)]

    def _pick(self) -> dict:
        """Return a random entry from the pool."""
        return random.choice(self._pool)

    @staticmethod
    def _idempotency_key(subscription_id: str, event_type: str, occurred_at: datetime) -> str:
        """
        Deterministic idempotency key.

        Formula: sha256(subscription_id + "|" + event_type + "|" + YYYY-MM-DD)

        Why sha256 instead of a format string?
        - Fixed length (64 hex chars) regardless of input variance
        - Collision-resistant: two different (sub, type, date) triples never
          produce the same key
        - Not guessable: prevents accidental key collisions if field values
          happen to share substrings

        Why day-level granularity?
        - One renewal per sub per day is the realistic business constraint.
          Finer granularity (hour/minute) would generate new keys on restarts
          and defeat idempotency. Coarser (month) would reject legitimate
          same-month re-subscriptions after a cancellation.
        """
        date_str = occurred_at.strftime("%Y-%m-%d")
        raw = f"{subscription_id}|{event_type}|{date_str}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _future(self, days: int = 30) -> datetime:
        return self._now() + timedelta(days=days)

    def _past(self, days: int = 30) -> datetime:
        return self._now() - timedelta(days=days)

    # ------------------------------------------------------------------
    # One builder per event type
    # ------------------------------------------------------------------

    def new_subscription(self) -> NewSubscriptionEvent:
        entry = self._pick()
        plan  = entry["plan"]
        now   = self._now()
        sub_duration = random.choice([30, 90, 365])
        return NewSubscriptionEvent(
            event_id=str(uuid4()),
            event_type="new_subscription",
            event_version="1.0",
            occurred_at=now.isoformat(),
            subscription_id=entry["subscription_id"],
            customer_id=entry["customer_id"],
            idempotency_key=self._idempotency_key(
                entry["subscription_id"], "new_subscription", now
            ),
            plan=plan,
            amount_usd=Decimal(str(PLAN_PRICES[plan])),
            expires_at=(now + timedelta(days=sub_duration)).isoformat(),
            auto_renew=random.choice([True, True, False]),  # 2:1 favour auto-renew
            source=random.choice(["web", "mobile_ios", "mobile_android", "partner"]),
        )

    def renewal(self) -> RenewalEvent:
        entry = self._pick()
        plan  = entry["plan"]
        now   = self._now()
        prev_expiry = self._past(days=random.randint(0, 2))
        new_expiry  = prev_expiry + timedelta(days=random.choice([30, 90, 365]))
        return RenewalEvent(
            event_id=str(uuid4()),
            event_type="renewal",
            event_version="1.0",
            occurred_at=now.isoformat(),
            subscription_id=entry["subscription_id"],
            customer_id=entry["customer_id"],
            idempotency_key=self._idempotency_key(
                entry["subscription_id"], "renewal", now
            ),
            plan=plan,
            amount_usd=Decimal(str(PLAN_PRICES[plan])),
            previous_expiry=prev_expiry.isoformat(),
            new_expiry=new_expiry.isoformat(),
            renewal_attempt=random.randint(1, 3),
        )

    def cancellation(self) -> CancellationEvent:
        entry = self._pick()
        now   = self._now()
        return CancellationEvent(
            event_id=str(uuid4()),
            event_type="cancellation",
            event_version="1.0",
            occurred_at=now.isoformat(),
            subscription_id=entry["subscription_id"],
            customer_id=entry["customer_id"],
            idempotency_key=self._idempotency_key(
                entry["subscription_id"], "cancellation", now
            ),
            reason=random.choice([
                "too_expensive", "not_needed", "switching_product",
                "poor_support", "technical_issues", "other",
            ]),
            cancelled_by=random.choice(["customer", "cs_agent"]),
            effective_date=now.isoformat(),
            refund_eligible=random.choice([True, False]),
        )

    def refund(self) -> RefundEvent:
        entry = self._pick()
        plan  = entry["plan"]
        now   = self._now()
        original = Decimal(str(PLAN_PRICES[plan]))
        # Partial refunds are common; pro-rate up to the full original amount
        refund_pct    = random.choice([Decimal("0.25"), Decimal("0.50"), Decimal("1.00")])
        refund_amount = (original * refund_pct).quantize(Decimal("0.01"))
        return RefundEvent(
            event_id=str(uuid4()),
            event_type="refund",
            event_version="1.0",
            occurred_at=now.isoformat(),
            subscription_id=entry["subscription_id"],
            customer_id=entry["customer_id"],
            idempotency_key=self._idempotency_key(
                entry["subscription_id"], "refund", now
            ),
            transaction_id=random.choice(self._transaction_ids),
            refund_amount=refund_amount,
            original_amount=original,
            reason=random.choice([
                "customer_request", "billing_error", "product_defect",
                "duplicate_charge", "cs_goodwill",
            ]),
            initiated_by=random.choice([
                "customer", "cs_agent:1001", "cs_agent:1042", "billing_system",
            ]),
        )

    def expiry(self) -> ExpiryEvent:
        entry = self._pick()
        plan  = entry["plan"]
        now   = self._now()
        return ExpiryEvent(
            event_id=str(uuid4()),
            event_type="expiry",
            event_version="1.0",
            occurred_at=now.isoformat(),
            subscription_id=entry["subscription_id"],
            customer_id=entry["customer_id"],
            idempotency_key=self._idempotency_key(
                entry["subscription_id"], "expiry", now
            ),
            plan=plan,
            expired_at=now.isoformat(),
            auto_renew=False,  # by definition: if auto_renew=True it would have renewed
        )

    def extension(self) -> ExtensionEvent:
        entry = self._pick()
        now   = self._now()
        prev_expiry = self._future(days=random.randint(0, 7))
        extend_days = random.choice([7, 14, 30])
        new_expiry  = prev_expiry + timedelta(days=extend_days)
        return ExtensionEvent(
            event_id=str(uuid4()),
            event_type="extension",
            event_version="1.0",
            occurred_at=now.isoformat(),
            subscription_id=entry["subscription_id"],
            customer_id=entry["customer_id"],
            idempotency_key=self._idempotency_key(
                entry["subscription_id"], "extension", now
            ),
            extended_by_days=extend_days,
            previous_expiry=prev_expiry.isoformat(),
            new_expiry=new_expiry.isoformat(),
            reason=random.choice([
                "cs_goodwill", "service_outage", "billing_failure",
                "promotional", "technical_error",
            ]),
            extended_by=random.choice([
                "cs_agent:1001", "cs_agent:1042", "cs_agent:2099",
            ]),
        )

    def build(self, event_type: str) -> BaseEvent:
        """Dispatch to the correct builder by event_type string."""
        builders = {
            "new_subscription": self.new_subscription,
            "renewal":          self.renewal,
            "cancellation":     self.cancellation,
            "refund":           self.refund,
            "expiry":           self.expiry,
            "extension":        self.extension,
        }
        if event_type not in builders:
            raise ValueError(
                f"Unknown event_type '{event_type}'. "
                f"Valid: {sorted(builders)}"
            )
        return builders[event_type]()

    def random_weighted(self) -> BaseEvent:
        """
        Pick an event type using the weighted distribution and build it.

        random.choices() with weights is the cleanest stdlib approach:
        no need to normalise to probabilities — the raw integers work as-is.
        """
        [event_type] = random.choices(EVENT_TYPES, weights=WEIGHTS, k=1)
        return self.build(event_type)


# ---------------------------------------------------------------------------
# Kafka producer helpers
# ---------------------------------------------------------------------------

def _make_producer(bootstrap_servers: str) -> Producer:
    """
    Create a confluent_kafka.Producer with production-appropriate settings.

    Key config choices:
      acks=all          — wait for all in-sync replicas to ACK (durability).
                          In local single-broker Docker this is equivalent to
                          acks=1 but keeps the config correct for MSK migration.
      retries=3         — transient broker errors auto-retry without our
                          involvement; delivery callback still fires on final fail.
      linger.ms=5       — batch messages for up to 5ms before sending.
                          Tiny latency cost; significant throughput gain during
                          bursts (hundreds of events in a short window).
      compression.type  — lz4 is fast CPU-wise and gives ~3-5× size reduction
                          on JSON payloads. Important at 300M-sub scale.
    """
    return Producer({
        "bootstrap.servers":  bootstrap_servers,
        "acks":               "all",
        "retries":            3,
        "linger.ms":          5,
        "compression.type":   "lz4",
        "client.id":          "streamforge-producer",
    })


def _delivery_callback(err, msg) -> None:
    """
    Called by confluent_kafka after every produce() settles.

    Why not just check the return value of produce()?
    produce() is non-blocking — it enqueues the message internally and
    returns immediately. The actual send happens on a background thread.
    The delivery callback is the only reliable notification mechanism.

    We log both success and failure so the structured logs feed into
    CloudWatch metrics (Day 26) without any extra instrumentation.
    """
    if err:
        logger.error(
            "Kafka delivery failed",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "error":     str(err),
            },
        )
    else:
        logger.debug(
            "Kafka delivery confirmed",
            extra={
                "topic":     msg.topic(),
                "partition": msg.partition(),
                "offset":    msg.offset(),
            },
        )


def produce_event(producer: Producer, event: BaseEvent) -> None:
    """
    Serialize and publish a single event to its canonical topic.

    Partition key = event.kafka_key() = subscription_id encoded as bytes.
    This guarantees all events for the same subscription land on the same
    partition, preserving ordering for the consumer state machine (Day 8).
    """
    topic = TOPIC_MAP[event.event_type]
    t0    = time.perf_counter()
    try:
        producer.produce(
            topic=topic,
            key=event.kafka_key(),
            value=event.to_kafka_payload(),
            on_delivery=_delivery_callback,
        )
        duration_ms = (time.perf_counter() - t0) * 1000
        log_kafka_processed(
            logger,
            event_type=event.event_type,
            event_id=event.event_id,
            subscription_id=event.subscription_id,
            duration_ms=round(duration_ms, 2),
            success=True,
        )
    except KafkaException as exc:
        logger.error(
            "produce() raised KafkaException",
            extra={
                "event_type": event.event_type,
                "event_id":   event.event_id,
                "error":      str(exc),
            },
        )
        raise


# ---------------------------------------------------------------------------
# Operating modes
# ---------------------------------------------------------------------------

def mode_simulate(producer: Producer, factory: FakeDataFactory, interval: float) -> None:
    """
    Infinite loop: emit one weighted-random event every `interval` seconds.

    Loop structure:
      1. Build event
      2. produce() — enqueues on background thread
      3. poll(0)   — drains delivery callbacks WITHOUT blocking
      4. sleep     — honour SIMULATE_INTERVAL_SEC

    poll(0) vs flush():
      poll(0) returns immediately after processing any pending callbacks.
      flush() would block until ALL in-flight messages are delivered —
      far too slow for a tight loop. We call flush() only on shutdown.
    """
    logger.info(
        "Producer starting in simulate mode",
        extra={"interval_sec": interval, "weight_distribution": dict(EVENT_WEIGHTS)},
    )
    while not _shutdown:
        event = factory.random_weighted()
        produce_event(producer, event)
        producer.poll(0)
        time.sleep(interval)

    # Graceful shutdown: deliver anything still buffered
    logger.info("Flushing in-flight messages before exit")
    producer.flush(timeout=15)
    logger.info("Producer shut down cleanly")


def mode_once(producer: Producer, factory: FakeDataFactory) -> None:
    """
    Emit exactly one of each event type, then exit.

    Useful for smoke testing (Day 7): confirms all 6 topics accept messages
    and that each schema validates end-to-end.

    We emit in a logical lifecycle order (new → renewal → cancel → refund
    → expiry → extension) so the Kafka UI shows a coherent sequence.
    """
    lifecycle_order = [
        "new_subscription",
        "renewal",
        "cancellation",
        "refund",
        "expiry",
        "extension",
    ]
    logger.info("Producer starting in once mode — emitting one of each event type")
    for event_type in lifecycle_order:
        event = factory.build(event_type)
        produce_event(producer, event)

    # flush() blocks until all 6 messages are ACKed or timeout is reached.
    # Essential here: the process exits after this function returns, and
    # any un-flushed messages in the internal queue would be silently dropped.
    remaining = producer.flush(timeout=15)
    if remaining > 0:
        logger.warning(
            "flush() timed out with messages still in queue",
            extra={"remaining": remaining},
        )
    else:
        logger.info("All 6 events delivered successfully")


def mode_single(producer: Producer, factory: FakeDataFactory, event_type: str) -> None:
    """
    Emit exactly one event of the specified type, then exit.

    Used by `make produce-single EVENT=renewal` for targeted testing of
    specific consumer handlers or state machine transitions.
    """
    logger.info(
        "Producer starting in single mode",
        extra={"event_type": event_type},
    )
    event = factory.build(event_type)
    produce_event(producer, event)
    remaining = producer.flush(timeout=15)
    if remaining > 0:
        logger.warning(
            "flush() timed out",
            extra={"remaining": remaining},
        )
    else:
        logger.info("Event delivered", extra={"event_type": event_type})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="StreamForge Kafka producer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Continuous simulation (Docker default)
  python producer/producer.py --mode simulate

  # One of each event type then exit
  python producer/producer.py --mode once

  # One specific event then exit
  python producer/producer.py --mode single --event renewal
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["simulate", "once", "single"],
        default="simulate",
        help="Operating mode (default: simulate)",
    )
    parser.add_argument(
        "--event",
        choices=EVENT_TYPES,
        default=None,
        help="Event type for single mode (required when --mode single)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.mode == "single" and args.event is None:
        logger.error("--event is required when --mode is single")
        sys.exit(1)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    interval  = float(os.getenv("SIMULATE_INTERVAL_SEC", "1.5"))

    logger.info(
        "Initialising Kafka producer",
        extra={
            "bootstrap_servers": bootstrap,
            "mode":              args.mode,
        },
    )

    producer = _make_producer(bootstrap)
    factory  = FakeDataFactory()

    if args.mode == "simulate":
        mode_simulate(producer, factory, interval)
    elif args.mode == "once":
        mode_once(producer, factory)
    elif args.mode == "single":
        mode_single(producer, factory, args.event)


if __name__ == "__main__":
    main()