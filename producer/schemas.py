"""
StreamForge — Event Schemas (Module 1)
Pydantic models for all 6 subscription lifecycle events.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Base ──────────────────────────────────────────────────────────────────────

class BaseEvent(BaseModel):
    event_id:        str = Field(default_factory=lambda: str(uuid4()))
    event_type:      str
    event_version:   str = "1.0"
    occurred_at:     str = Field(default_factory=_now)
    subscription_id: str
    customer_id:     str
    idempotency_key: str = Field(default_factory=lambda: str(uuid4()))

    def kafka_key(self) -> str:
        """Partition by subscription_id so all events for a sub go to same partition."""
        return self.subscription_id


# ── Lifecycle Events ──────────────────────────────────────────────────────────

class NewSubscriptionEvent(BaseEvent):
    event_type:  str = "new_subscription"
    plan:        str                     # basic | premium | enterprise
    amount_usd:  float
    expires_at:  str
    auto_renew:  bool = True
    source:      str = "ui_purchase"     # ui_purchase | api | cs_agent


class RenewalEvent(BaseEvent):
    event_type:      str = "renewal"
    plan:            str
    amount_usd:      float
    previous_expiry: str
    new_expiry:      str
    renewal_attempt: int = 1


class CancellationEvent(BaseEvent):
    event_type:      str = "cancellation"
    reason:          str = "user_requested"
    cancelled_by:    str = "customer"    # customer | cs_agent | system
    effective_date:  str = Field(default_factory=_now)
    refund_eligible: bool = False


class RefundEvent(BaseEvent):
    event_type:      str = "refund"
    transaction_id:  str
    refund_amount:   float
    original_amount: float
    reason:          str = "customer_request"
    initiated_by:    str = "cs_agent"


class ExpiryEvent(BaseEvent):
    event_type:  str = "expiry"
    plan:        str
    expired_at:  str = Field(default_factory=_now)
    auto_renew:  bool


class ExtensionEvent(BaseEvent):
    event_type:        str = "extension"
    extended_by_days:  int
    previous_expiry:   str
    new_expiry:        str
    reason:            str = "cs_goodwill"
    extended_by:       str = "cs_agent"


# ── Topic mapping ─────────────────────────────────────────────────────────────

TOPIC_MAP: dict[str, str] = {
    "new_subscription": "sub.new_subscription",
    "renewal":          "sub.renewal",
    "cancellation":     "sub.cancellation",
    "refund":           "sub.refund",
    "expiry":           "sub.expiry",
    "extension":        "sub.extension",
}

DLQ_TOPIC_MAP: dict[str, str] = {
    k: f"{v}.dlq" for k, v in TOPIC_MAP.items()
}
