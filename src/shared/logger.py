"""
StreamForge Structured Logger
==============================
A zero-dependency wrapper around stdlib logging that emits
newline-delimited JSON — one object per log line.

Why structured JSON logging?
-----------------------------
At scale, logs are queried, not read.  When an on-call engineer asks
"show me all DLQ events for subscription X in the last hour", they run
a log query, not grep.  Structured logs make every field queryable with
zero parsing config in Datadog / CloudWatch / Loki.

    logger.info("event processed", extra={"subscription_id": "abc", "duration_ms": 12})

Emits:
    {"ts":"2025-01-15T10:23:01+00:00","level":"INFO","service":"consumer",
     "msg":"event processed","subscription_id":"abc","duration_ms":12}

Design decisions
----------------

1.  get_logger(name) returns a stdlib Logger — no new type to learn.
    Every Python developer knows logger.info() / logger.warning().
    The only thing we change is the formatter.  This means third-party
    libraries that use stdlib logging (confluent-kafka, psycopg2) also
    emit JSON when we configure the root logger.

2.  JSON formatter is a single StreamHandler to stdout.
    Containers log to stdout and the orchestration layer (Docker, ECS)
    ships logs to the log aggregator.  Writing to files inside a container
    is an antipattern — the files disappear when the container restarts.

3.  Logger registry prevents duplicate handlers.
    Python's logging module is global state.  Calling get_logger("consumer")
    twice without a registry would attach two StreamHandlers and double-print
    every line.  The registry dict maps name → Logger and returns the cached
    instance on subsequent calls.

4.  Log level from LOG_LEVEL env var, default INFO.
    DEBUG in local docker-compose (set LOG_LEVEL=DEBUG in .env).
    INFO in production.  Never hardcode levels in source.

5.  Extra fields are merged into the top-level JSON object.
    logger.info("msg", extra={"k": "v"}) → {"msg":"msg","k":"v",...}
    This is more queryable than nesting under an "extra" key, which would
    require nested field syntax in every log query.

6.  Exception tracebacks are stringified into "exc_info" field.
    try/except blocks call logger.exception() which sets exc_info=True
    automatically.  The formatter captures the traceback as a string so it
    appears in the same JSON line, not as a separate unstructured blob.

Shared log-line helpers
-----------------------
log_kafka_received, log_kafka_processed, log_dlq_routed are called by
every consumer and producer at the same points in their lifecycle.
Standardising these means Datadog dashboards and alert rules work identically
across all six event types without per-service customisation.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# JSON Formatter
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Formats a LogRecord as a single-line JSON object."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    # Fields that stdlib adds to every LogRecord — we don't want them
    # cluttering the JSON output alongside user-supplied extra fields.
    _SKIP = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "taskName",   # Python 3.12 asyncio task name
    })

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts":      datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level":   record.levelname,
            "service": self.service,
            "logger":  record.name,
            "msg":     record.getMessage(),
        }

        # Merge user-supplied extra={} fields into the top level
        for key, val in record.__dict__.items():
            if key not in self._SKIP and not key.startswith("_"):
                doc[key] = val

        # Stringify exception traceback into a single field
        if record.exc_info:
            doc["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(doc, default=self._default)

    @staticmethod
    def _default(obj: Any) -> Any:
        """Fallback for types json.dumps cannot handle natively."""
        try:
            return str(obj)
        except Exception:
            return "<unserializable>"


# ---------------------------------------------------------------------------
# Logger registry
# ---------------------------------------------------------------------------

_registry: dict[str, logging.Logger] = {}


def get_logger(name: str, service: str | None = None) -> logging.Logger:
    """
    Return a structured JSON logger.

    Parameters
    ----------
    name : str
        Logger name — use __name__ for module-level loggers so the dotted
        module path appears in the "logger" field.  For service-level loggers
        use the service name directly: get_logger("consumer").

    service : str, optional
        Value for the "service" JSON field.  Defaults to `name`.
        Useful when name=__name__ (e.g. "shared.schemas") but you still
        want service="consumer" in every line from that module.

    Returns
    -------
    logging.Logger
        Cached logger — safe to call multiple times with the same name.

    Examples
    --------
    Module-level (most common):
        logger = get_logger(__name__, service="producer")
        logger.info("producing event", extra={"event_type": "renewal"})

    Service entry-point:
        logger = get_logger("consumer")
        logger.warning("DLQ route", extra={"topic": "sub.renewal.dlq"})
    """
    if name in _registry:
        return _registry[name]

    logger = logging.getLogger(name)

    level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logger.setLevel(level)

    # Prevent double-printing if the root logger also has handlers
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter(service=service or name))
    logger.addHandler(handler)

    _registry[name] = logger
    return logger


# ---------------------------------------------------------------------------
# Standardised log-line helpers
# ---------------------------------------------------------------------------

def log_kafka_received(
    logger: logging.Logger,
    *,
    topic: str,
    partition: int,
    offset: int,
    event_type: str,
    event_id: str,
) -> None:
    """
    Emit a structured line when a Kafka message is received by the consumer.

    Calling this at the top of the poll loop gives a consistent "intake"
    audit trail across all six event types.  The Datadog query
        service:consumer msg:"kafka message received" event_type:renewal
    returns every renewal message the consumer ever saw.
    """
    logger.info(
        "kafka message received",
        extra={
            "kafka_topic":     topic,
            "kafka_partition": partition,
            "kafka_offset":    offset,
            "event_type":      event_type,
            "event_id":        event_id,
        },
    )


def log_kafka_processed(
    logger: logging.Logger,
    *,
    event_type: str,
    event_id: str,
    subscription_id: str,
    duration_ms: float,
    success: bool = True,
) -> None:
    """
    Emit a structured line after an event has been fully processed
    (Postgres write committed, offset committed to Kafka).

    duration_ms feeds the consumer-latency histogram in Datadog.
    Target SLA: p99 < 500 ms for all event types.
    """
    msg = "kafka message processed" if success else "kafka message failed"
    level = "info" if success else "error"
    getattr(logger, level)(
        msg,
        extra={
            "event_type":      event_type,
            "event_id":        event_id,
            "subscription_id": subscription_id,
            "duration_ms":     round(duration_ms, 2),
            "success":         success,
        },
    )


def log_dlq_routed(
    logger: logging.Logger,
    *,
    source_topic: str,
    dlq_topic: str,
    event_id: str,
    reason: str,
    retry_count: int,
) -> None:
    """
    Emit a WARNING when a message is routed to a Dead Letter Queue.

    DLQ events are WARNING not ERROR because the pipeline is still
    functioning — this specific message failed, but processing continues.
    ERROR is reserved for conditions that halt the consumer entirely
    (e.g. Postgres connection lost, Kafka broker unreachable).
    """
    logger.warning(
        "routing message to DLQ",
        extra={
            "source_topic": source_topic,
            "dlq_topic":    dlq_topic,
            "event_id":     event_id,
            "dlq_reason":   reason,
            "retry_count":  retry_count,
        },
    )