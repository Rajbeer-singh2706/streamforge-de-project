"""
StreamForge Shared Library
--------------------------
Import from here across all services. Never cross-import between services.
 
    from shared.schemas   import NewSubscriptionEvent, RenewalEvent
    from shared.constants import TOPIC_MAP, DLQ_TOPIC_MAP, PLANS, PLAN_PRICES
    from shared.logger    import get_logger
"""

from shared.schemas import (
    BaseEvent,
    NewSubscriptionEvent,
    RenewalEvent,
    CancellationEvent,
    RefundEvent,
    ExpiryEvent,
    ExtensionEvent,
)
from shared.constants import TOPIC_MAP, DLQ_TOPIC_MAP, PLANS, PLAN_PRICES
from shared.logger import get_logger
 
__version__ = "0.5.0"
 
__all__ = [
    "BaseEvent",
    "NewSubscriptionEvent",
    "RenewalEvent",
    "CancellationEvent",
    "RefundEvent",
    "ExpiryEvent",
    "ExtensionEvent",
    "TOPIC_MAP",
    "DLQ_TOPIC_MAP",
    "PLANS",
    "PLAN_PRICES",
    "get_logger",
]
