"""
StreamForge Constants
=====================
Single source of truth for topic names, plan definitions, and pricing.

Design decisions
----------------

1.  TOPIC_MAP and DLQ_TOPIC_MAP are plain dicts, not Enums.
    Producers and consumers index them by the event_type string that arrives
    in every event payload (e.g. event.event_type == "renewal").  A dict
    lookup is the most direct mapping and keeps producer/consumer code readable:

        topic = TOPIC_MAP[event.event_type]          # "sub.renewal"
        dlq   = DLQ_TOPIC_MAP[event.event_type]      # "sub.renewal.dlq"

    If the topic name changes, update here — one place, every service picks it up.

2.  Topic naming convention: sub.<event_type> and sub.<event_type>.dlq
    The "sub." prefix groups all StreamForge topics for ACL management,
    monitoring dashboards, and quota policies.  Dotted names render as a
    tree in Kafka UI and kcat, making the namespace immediately readable.

3.  PLANS is an ordered list, not a set.
    Order matters: basic < premium < enterprise.  The state machine and the
    dashboard use this ordering to detect upgrade vs downgrade transitions.

4.  PLAN_PRICES stores the monthly USD list price.
    These are reference prices for analytics enrichment only — never used
    for billing arithmetic (that lives in the billing system).  Keeping them
    here means the revenue dashboard and refund-eligibility logic stay in sync
    without querying an external service.

5.  No environment reads at import time.
    This module is pure Python constants.  Services read env vars in their own
    settings blocks (producer.py, consumer.py).  That makes unit tests fast
    and hermetic — import shared.constants anywhere without mocking.
"""

# ---------------------------------------------------------------------------
# Kafka topic map  (event_type → primary topic)
# ---------------------------------------------------------------------------

TOPIC_MAP: dict[str, str] = {
    "new_subscription": "sub.new_subscription",
    "renewal":          "sub.renewal",
    "cancellation":     "sub.cancellation",
    "refund":           "sub.refund",
    "expiry":           "sub.expiry",
    "extension":        "sub.extension",
}

# ---------------------------------------------------------------------------
# Dead-letter queue map  (event_type → DLQ topic)
# Mirrors TOPIC_MAP 1-to-1; kept separate so consumers can route without
# string concatenation scattered through the codebase.
# ---------------------------------------------------------------------------

DLQ_TOPIC_MAP: dict[str, str] = {
    event_type: f"{topic}.dlq"
    for event_type, topic in TOPIC_MAP.items()
}
# Resolves to:
#   "new_subscription" → "sub.new_subscription.dlq"
#   "renewal"          → "sub.renewal.dlq"
#   ... etc.

# ---------------------------------------------------------------------------
# Audit topic — not in TOPIC_MAP because it is written by the consumer,
# not keyed by event_type.
# ---------------------------------------------------------------------------

AUDIT_TOPIC = "sub.audit_log"

# ---------------------------------------------------------------------------
# Subscription plans
# ---------------------------------------------------------------------------

# Ordered list: index position encodes tier level (0 = lowest).
# Use PLANS.index(plan) to compare tiers: upgrade when new index > old index.
PLANS: list[str] = ["basic", "premium", "enterprise"]

# Monthly USD list price per plan — analytics reference only, not for billing.
PLAN_PRICES: dict[str, float] = {
    "basic":      4.99,
    "premium":    9.99,
    "enterprise": 29.99,
}

# Valid subscription statuses — mirrors the Postgres enum subscription_status.
# Defined here so the state machine and consumer can import without a DB query.
VALID_STATUSES: list[str] = [
    "active",
    "expired",
    "cancelled",
    "pending_renewal",
    "suspended",
]

# Consumer group ID — single place so producer smoke tests and consumer
# lag scripts reference the same string.
CONSUMER_GROUP_ID = "streamforge-etl-v1"