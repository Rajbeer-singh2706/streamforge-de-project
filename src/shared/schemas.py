"""
StreamForge Pydantic v2 Event Schemas
======================================
All six lifecycle event models, plus the shared BaseEvent envelope.
Every Kafka message produced or consumed in StreamForge is validated
against one of these models.

Design decisions
----------------

1.  BaseEvent carries the envelope; subclasses carry the payload.
    Envelope fields (event_id, event_type, occurred_at, subscription_id,
    customer_id, idempotency_key) are present on *every* event.  They are
    what the consumer needs to route, deduplicate, and audit — before it
    even inspects the payload.  Keeping them on the base class means the
    consumer can write its dedup check with a single BaseEvent parse,
    before deciding which concrete class to use.

2.  event_id and idempotency_key default to uuid4 strings.
    Producers generate both automatically.  event_id is unique per message
    instance (even a retry gets a new event_id).  idempotency_key is
    business-level: the producer sets it to a deterministic value based on
    the business operation so the consumer can detect duplicates across
    Kafka redeliveries.  Two different messages with the same idempotency_key
    + event_type = same business operation; the second is a duplicate.

3.  occurred_at is an ISO 8601 string, not a datetime object.
    This survives JSON round-trips through Kafka and Postgres JSONB without
    any serialiser config.  Consumers parse it to datetime only when they
    need to do time arithmetic (e.g. computing days_until_expiry).  The
    field validator enforces the format so garbage strings are rejected early.

4.  kafka_key() returns subscription_id.
    Partitioning by subscription_id guarantees all events for a given
    subscription land on the same partition in order.  The consumer can
    apply state-machine transitions without cross-partition coordination.

5.  to_kafka_payload() returns bytes.
    Producers call this one method — no per-producer serialisation logic.
    model_dump_json() (Pydantic v2) handles UUID→str and Decimal→str via
    the json_encoders config; encode("utf-8") gives the bytes Kafka wants.

6.  from_kafka_payload() is the consumer's entry point.
    It decodes bytes → dict → validated model in one call.  Validation
    errors surface as Pydantic ValidationError, which the consumer catches
    and routes to the DLQ.

7.  amount_usd is Decimal, not float.
    0.1 + 0.2 ≠ 0.3 in IEEE 754.  At 300 M subscriptions the rounding
    error compounds into material revenue misreporting.  Decimal("9.99")
    is exact.  The Postgres column is NUMERIC(10,2) — same guarantee at
    the storage layer.

8.  model_config extra="forbid".
    If a producer sends an unknown field (schema drift, typo), the consumer
    raises immediately instead of silently dropping the data.  This surfaces
    producer bugs in staging before they corrupt the warehouse.

9.  All string fields that map to DB enums carry a Literal type.
    plan: Literal["basic","premium","enterprise"] means Pydantic rejects
    "gold" at the producer before it ever reaches Kafka.  The Postgres CHECK
    constraint is a second layer; this is the first.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """Return current UTC time as ISO 8601 string with timezone offset."""
    return datetime.now(timezone.utc).isoformat()


def _uuid4_str() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Base Event (envelope)
# ---------------------------------------------------------------------------

class BaseEvent(BaseModel):
    """
    Envelope fields present on every StreamForge Kafka event.

    Do not instantiate directly — use the concrete subclasses.
    The consumer can parse an unknown message as BaseEvent to read
    the routing fields before deciding which subclass to use.
    """

    model_config = ConfigDict(
        # Reject unknown fields — catches schema drift immediately.
        extra="forbid",
        # Use enum values (str) not enum members when serialising.
        use_enum_values=True,
        # Freeze after construction — events are immutable facts.
        frozen=True,
    )

    # --- Envelope fields ---

    event_id: str = Field(
        default_factory=_uuid4_str,
        description="Unique ID for this message instance. New ID on every produce call.",
    )
    event_type: str = Field(
        description="Matches a key in TOPIC_MAP: new_subscription | renewal | cancellation "
                    "| refund | expiry | extension",
    )
    event_version: str = Field(
        default="1.0",
        description="Schema version. Consumers gate migration logic on this.",
    )
    occurred_at: str = Field(
        default_factory=_now_iso,
        description="ISO 8601 UTC timestamp of when the business event happened.",
    )
    subscription_id: str = Field(
        description="UUID of the subscription this event belongs to. Used as Kafka partition key.",
    )
    customer_id: str = Field(
        description="UUID of the customer who owns the subscription.",
    )
    idempotency_key: str = Field(
        default_factory=_uuid4_str,
        description=(
            "Business-level dedup key. The consumer checks this against the "
            "transactions.idempotency_key UNIQUE column before processing. "
            "Producers should set this to a deterministic value, e.g. "
            "'renewal:{subscription_id}:{billing_cycle_date}'."
        ),
    )

    @field_validator("occurred_at", mode="before")
    @classmethod
    def validate_iso_datetime(cls, v: object) -> str:
        """Accept datetime objects and ISO strings; reject garbage."""
        if isinstance(v, datetime):
            # Normalise to UTC if naive
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            # Attempt to parse — raises ValueError on bad format
            datetime.fromisoformat(v)
            return v
        raise ValueError(f"occurred_at must be a datetime or ISO string, got {type(v)}")

    @field_validator("subscription_id", "customer_id", mode="before")
    @classmethod
    def validate_uuid_str(cls, v: object) -> str:
        """Accept UUID objects or UUID-format strings; reject malformed values."""
        if isinstance(v, uuid.UUID):
            return str(v)
        if isinstance(v, str):
            uuid.UUID(v)  # raises ValueError if invalid
            return v
        raise ValueError(f"Expected UUID string, got {type(v)}")

    # --- Kafka helpers ---

    def kafka_key(self) -> bytes:
        """
        Partition key for Kafka.  All events for one subscription go to
        the same partition, guaranteeing ordering within that subscription.
        """
        return self.subscription_id.encode("utf-8")

    def to_kafka_payload(self) -> bytes:
        """
        Serialise to UTF-8 JSON bytes for KafkaProducer.send(value=...).

        Usage (producer):
            producer.produce(
                topic=TOPIC_MAP[event.event_type],
                key=event.kafka_key(),
                value=event.to_kafka_payload(),
            )
        """
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_payload(cls, raw: bytes) -> "BaseEvent":
        """
        Deserialise a raw Kafka message into a typed event.

        Usage (consumer):
            try:
                event = NewSubscriptionEvent.from_kafka_payload(msg.value())
            except ValidationError as exc:
                route_to_dlq(msg, reason=str(exc))
        """
        data = json.loads(raw.decode("utf-8"))
        return cls.model_validate(data)


# ---------------------------------------------------------------------------
# Event 1: NewSubscriptionEvent
# ---------------------------------------------------------------------------

class NewSubscriptionEvent(BaseEvent):
    """
    Emitted when a user completes a new subscription purchase.

    Producer:  billing-api / UI checkout flow
    Consumers: warehouse-loader writes a new row to subscriptions table,
               email-service triggers welcome email,
               analytics marks activation funnel step.

    auto_renew defaults True — the vast majority of XYS subscriptions
    are card-on-file recurring.  The field exists so the expiry processor
    knows whether to emit an ExpiryEvent or schedule a renewal attempt.

    source distinguishes the purchase channel so growth analytics can
    attribute revenue to UI / mobile / CS / partner without a separate
    attribution system.
    """

    event_type: Literal["new_subscription"] = "new_subscription"

    plan:       Literal["basic", "premium", "enterprise"]
    amount_usd: Decimal = Field(
        gt=Decimal("0"),
        decimal_places=2,
        description="Amount charged for this subscription period.",
    )
    expires_at:  str   = Field(description="ISO 8601 UTC expiry timestamp.")
    auto_renew:  bool  = True
    source:      Literal["web", "mobile", "cs_portal", "partner"] = "web"

    @field_validator("expires_at", mode="before")
    @classmethod
    def validate_expires_at(cls, v: object) -> str:
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            datetime.fromisoformat(v)
            return v
        raise ValueError(f"expires_at must be datetime or ISO string, got {type(v)}")


# ---------------------------------------------------------------------------
# Event 2: RenewalEvent
# ---------------------------------------------------------------------------

class RenewalEvent(BaseEvent):
    """
    Emitted on each successful auto-renewal charge.

    This is the highest-volume event type at 300M subscriptions.
    Renewals cluster around billing-cycle dates, creating thundering-herd
    patterns.  The Kafka partition key = subscription_id ensures ordering
    within a subscription, which matters for the renewal_attempt counter.

    previous_expiry / new_expiry let the consumer update subscriptions.expires_at
    without a prior SELECT — the event carries the full before/after state.

    renewal_attempt starts at 1.  Values > 1 mean the initial charge failed
    and dunning succeeded on a retry.  The analytics team tracks this for
    payment-health cohorts.
    """

    event_type: Literal["renewal"] = "renewal"

    plan:            Literal["basic", "premium", "enterprise"]
    amount_usd:      Decimal = Field(gt=Decimal("0"), decimal_places=2)
    previous_expiry: str = Field(description="ISO 8601 UTC — expiry before this renewal.")
    new_expiry:      str = Field(description="ISO 8601 UTC — expiry after this renewal.")
    renewal_attempt: int = Field(default=1, ge=1, description="1 = first attempt, 2+ = dunning retry.")

    @field_validator("previous_expiry", "new_expiry", mode="before")
    @classmethod
    def validate_expiry_fields(cls, v: object) -> str:
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            datetime.fromisoformat(v)
            return v
        raise ValueError(f"Expected datetime or ISO string, got {type(v)}")


# ---------------------------------------------------------------------------
# Event 3: CancellationEvent
# ---------------------------------------------------------------------------

class CancellationEvent(BaseEvent):
    """
    Emitted when a subscription is cancelled.

    cancelled_by distinguishes user self-service from CS-initiated
    cancellations, which matters for churn attribution and refund-eligibility
    policies (CS-initiated cancellations within 30 days are always refundable).

    effective_date separates cancellation intent from access termination.
    A user may cancel today but retain access until their paid-through date.
    The consumer sets subscriptions.status = 'cancelled' immediately but
    keeps expires_at unchanged; the expiry processor handles access termination.

    refund_eligible is computed by the billing system at cancellation time
    and embedded here so the consumer does not need to call back to billing.
    """

    event_type: Literal["cancellation"] = "cancellation"

    reason: Literal[
        "user_requested",
        "payment_failure",
        "cs_initiated",
        "fraud",
        "duplicate",
        "other",
    ]
    cancelled_by:   str  = Field(
        description="'user' | 'system' | 'cs_agent:<agent_id>'",
    )
    effective_date: str  = Field(description="ISO 8601 UTC — when access actually ends.")
    refund_eligible: bool = False

    @field_validator("effective_date", mode="before")
    @classmethod
    def validate_effective_date(cls, v: object) -> str:
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            datetime.fromisoformat(v)
            return v
        raise ValueError(f"Expected datetime or ISO string, got {type(v)}")


# ---------------------------------------------------------------------------
# Event 4: RefundEvent
# ---------------------------------------------------------------------------

class RefundEvent(BaseEvent):
    """
    Emitted when a refund is issued against a previous transaction.

    refund_amount is stored as a *positive* Decimal here — the sign
    convention (negative = money out) is applied at the Postgres write layer
    (consumer/db.py) to match the transactions table contract defined in
    init_db.sql.  Keeping it positive in the event model avoids confusion
    when the analytics team reads raw Kafka events.

    original_amount lets consumers validate that refund_amount ≤ original_amount
    without a DB lookup.  Partial refunds (e.g. prorated cancellation) are
    the common case; full refunds are the exception.

    initiated_by mirrors cancelled_by — enables audit queries like
    "all refunds initiated by cs_agent:42 in the last 30 days".
    """

    event_type: Literal["refund"] = "refund"

    transaction_id:  str     = Field(description="UUID of the original transaction being refunded.")
    refund_amount:   Decimal = Field(gt=Decimal("0"), decimal_places=2)
    original_amount: Decimal = Field(gt=Decimal("0"), decimal_places=2)
    reason: Literal[
        "cancellation",
        "billing_error",
        "duplicate_charge",
        "cs_goodwill",
        "chargeback",
        "other",
    ]
    initiated_by: str = Field(
        description="'user' | 'system' | 'cs_agent:<agent_id>'",
    )

    @model_validator(mode="before")
    @classmethod
    def refund_not_exceeds_original(cls, data: dict) -> dict:
        """Reject refunds that exceed the original charge. Runs before field validation."""
        refund = data.get("refund_amount")
        original = data.get("original_amount")
        if refund is not None and original is not None:
            from decimal import Decimal as D
            if D(str(refund)) > D(str(original)):
                raise ValueError(
                    f"refund_amount {refund} cannot exceed original_amount {original}"
                )
        return data


# ---------------------------------------------------------------------------
# Event 5: ExpiryEvent
# ---------------------------------------------------------------------------

class ExpiryEvent(BaseEvent):
    """
    Emitted by the expiry processor when a subscription's expires_at is reached
    and either auto_renew=False or all renewal attempts have failed.

    This event drives:
    - Email: "Your subscription has expired, renew now"
    - Telemetry: device protection status update (AV signatures stop updating)
    - Analytics: churned subscription cohort entry

    auto_renew is carried forward from the subscription record so the email
    service can tailor messaging ("Your auto-renewal is off — click to renew"
    vs "We were unable to charge your card").
    """

    event_type: Literal["expiry"] = "expiry"

    plan:       Literal["basic", "premium", "enterprise"]
    expired_at: str  = Field(description="ISO 8601 UTC — exact moment of expiry.")
    auto_renew: bool = Field(
        description="Was auto-renew on at time of expiry? False = user chose not to renew.",
    )

    @field_validator("expired_at", mode="before")
    @classmethod
    def validate_expired_at(cls, v: object) -> str:
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            datetime.fromisoformat(v)
            return v
        raise ValueError(f"Expected datetime or ISO string, got {type(v)}")


# ---------------------------------------------------------------------------
# Event 6: ExtensionEvent
# ---------------------------------------------------------------------------

class ExtensionEvent(BaseEvent):
    """
    Emitted when a CS agent extends a subscription's expiry date.

    Extensions are a CS tool for:
    - Compensating for service outages (goodwill extensions)
    - Reinstating cancelled or expired subscriptions for missed-payment edge cases
    - Honouring promo codes applied post-purchase

    extended_by is the CS agent identifier that flows into lifecycle_events.performed_by,
    providing a complete audit trail for compliance reviews.

    previous_expiry / new_expiry follow the same before/after pattern as RenewalEvent
    so the consumer's update logic is identical — no special-casing needed.
    """

    event_type: Literal["extension"] = "extension"

    extended_by_days: int = Field(
        gt=0,
        description="Number of calendar days added to the subscription.",
    )
    previous_expiry: str = Field(description="ISO 8601 UTC — expiry before extension.")
    new_expiry:      str = Field(description="ISO 8601 UTC — expiry after extension.")
    reason: Literal[
        "goodwill",
        "service_outage",
        "promo_code",
        "payment_grace",
        "cs_reinstatement",
        "other",
    ]
    extended_by: str = Field(
        description="'cs_agent:<agent_id>' or 'system' for automated extensions.",
    )

    @field_validator("previous_expiry", "new_expiry", mode="before")
    @classmethod
    def validate_expiry_fields(cls, v: object) -> str:
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.isoformat()
        if isinstance(v, str):
            datetime.fromisoformat(v)
            return v
        raise ValueError(f"Expected datetime or ISO string, got {type(v)}")


# ---------------------------------------------------------------------------
# Convenience: event_type string → model class
# Used by the consumer to dispatch without a chain of if/elif.
# ---------------------------------------------------------------------------

EVENT_TYPE_MAP: dict[str, type[BaseEvent]] = {
    "new_subscription": NewSubscriptionEvent,
    "renewal":          RenewalEvent,
    "cancellation":     CancellationEvent,
    "refund":           RefundEvent,
    "expiry":           ExpiryEvent,
    "extension":        ExtensionEvent,
}