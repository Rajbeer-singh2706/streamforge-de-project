# StreamForge — Project Context
> Paste this file at the start of any new chat:
> "Here is my context.md — continue building StreamForge from where I left off."

---

## 1. Who I am

- **Role:** Senior Data Engineer, 10+ years experience
- **Company:** XYS (antivirus company)
- **Project name:** StreamForge
- **Goal:** Build a subscription lifecycle data pipeline end-to-end — locally with Docker first, then migrate to AWS

---

## 2. What StreamForge is

A real-time subscription lifecycle data platform that processes:

| Operation | Kafka topic | DLQ |
|---|---|---|
| New subscription (UI purchase) | `sub.new_subscription` | `sub.new_subscription.dlq` |
| Renewal | `sub.renewal` | `sub.renewal.dlq` |
| Cancellation | `sub.cancellation` | `sub.cancellation.dlq` |
| Refund | `sub.refund` | `sub.refund.dlq` |
| Expiry | `sub.expiry` | `sub.expiry.dlq` |
| Extension (CS action) | `sub.extension` | `sub.extension.dlq` |

Plus one audit topic: `sub.audit_log`

**Scale:** 300M total subscriptions, hundreds of thousands of transactions in short burst windows.

**Tech stack (local):** Apache Kafka · PostgreSQL · Python · FastAPI · Streamlit
**Tech stack (AWS):** MSK · AWS Glue (PySpark) · Amazon Redshift · S3 · CloudWatch · Terraform

---

## 3. Directory structure (FINALISED)

```
streamforge/
├── context.md                      ← this file — update after each day
├── docker-compose.yml
├── Makefile
├── .env.example
├── .gitignore
├── README.md
│
├── src/
│   ├── shared/                     ← imported by ALL services
│   │   ├── __init__.py             ✅ Day 5
│   │   ├── schemas.py              ✅ Day 5 — 6 Pydantic v2 event models
│   │   ├── constants.py            ✅ Day 5 — TOPIC_MAP, DLQ_TOPIC_MAP, PLANS, PLAN_PRICES
│   │   └── logger.py               ✅ Day 5 — structured JSON logging
│   │
│   ├── producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt        ← confluent-kafka, faker, pydantic
│   │   └── producer.py             ← 3 modes: simulate / once / single
│   │
│   ├── consumer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt        ← confluent-kafka, psycopg2, tenacity
│   │   ├── state_machine.py        ← (status, event_type) → new_status
│   │   ├── db.py                   ← Postgres operations, transaction ctx manager
│   │   ├── handlers.py             ← one handler per event type
│   │   ├── consumer.py             ← Kafka poll loop, manual offset commit, DLQ
│   │   └── tests/
│   │       ├── test_state_machine.py
│   │       ├── test_handlers.py
│   │       └── test_idempotency.py
│   │
│   ├── api/
│   │   ├── Dockerfile
│   │   ├── requirements.txt        ← fastapi, uvicorn, python-jose, psycopg2
│   │   ├── main.py
│   │   ├── routes/
│   │   │   ├── subscriptions.py
│   │   │   ├── actions.py
│   │   │   ├── auth.py
│   │   │   └── audit.py
│   │   ├── services/
│   │   │   ├── subscription_service.py
│   │   │   ├── action_service.py
│   │   │   └── audit_service.py
│   │   └── middleware/
│   │       ├── auth.py
│   │       └── logging.py
│   │
│   ├── dashboard/
│   │   ├── Dockerfile
│   │   ├── requirements.txt        ← streamlit, psycopg2, confluent-kafka
│   │   └── app.py
│   │
│   ├── glue/                       ← AWS phase
│   │   ├── job_subscriptions.py
│   │   └── job_transactions.py
│   │
│   ├── infra/                      ← Terraform (AWS phase)
│   │   ├── msk.tf
│   │   ├── s3.tf
│   │   ├── redshift.tf
│   │   ├── cloudwatch.tf
│   │   ├── alarms.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   │
│   └── redshift/
│       ├── schema.sql
│       ├── copy_commands.sql
│       └── spectrum.sql
│
├── scripts/
│   ├── create_topics.sh            ✅ Day 2
│   ├── init_db.sql                 ✅ Day 3
│   ├── migrate_day4.sql            ✅ Day 4
│   ├── smoke_test.sh               ← Day 7
│   ├── load_test.py                ← Day 27
│   └── aws_e2e_test.sh             ← Day 29
│
└── docs/
    ├── architecture.md
    ├── runbook.md
    ├── api.md
    └── resume_profile.md
```

**Dockerfile pattern** (all services follow this):
```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev
COPY src/shared/ ./shared/
COPY src/<service>/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/<service>/ ./<service>/
CMD ["python", "<service>/<service>.py"]
```

**docker-compose build context** must be the project root:
```yaml
<service>:
  build:
    context: .
    dockerfile: src/<service>/Dockerfile
```

---

## 4. Postgres schema — DONE (Days 3–4)

### ENUMs
```sql
subscription_status: active | expired | cancelled | pending_renewal | suspended
event_type:          new_subscription | renewal | cancellation | refund | expiry | extension
transaction_status:  success | failed | refunded | pending
```

### Tables
| Table | PK | Notes |
|---|---|---|
| `customers` | UUID | email UNIQUE, country CHAR(2) |
| `subscriptions` | UUID | FK→customers, status enum, amount_usd NUMERIC(10,2) |
| `transactions` | UUID | idempotency_key UNIQUE, kafka_offset/partition/topic |
| `lifecycle_events` | BIGSERIAL | append-only audit log, previous→new status |
| `dlq_events` | BIGSERIAL | source_topic, error_reason, resolved BOOL |

### Views
- `v_subscription_summary` — customers + subscriptions + lifecycle_events
- `v_expiring_soon` — active subs expiring within 7 days
- `v_dlq_summary` — unresolved/resolved counts per topic
- `v_revenue_by_plan` — active MRR per plan

### Seed data
```
alice@example.com   premium     active  30 days  $9.99
bob@example.com     basic       active  15 days  $4.99
carol@example.com   enterprise  active  60 days  $29.99
```

### Day 4 hardening (migrate_day4.sql)
CHECK constraints (NOT VALID → VALIDATE pattern for zero downtime):
- email format regex, country code 2-char, amount > 0
- expires_at > started_at, valid plan values
- cancelled_at consistency, refund amounts negative
- kafka meta completeness (offset/partition/topic all-or-nothing)

Partial indexes (all CONCURRENTLY):
- `idx_sub_active_only`, `idx_sub_expiring_soon`, `idx_dlq_unresolved`
- `idx_txn_sub_recent`, `idx_sub_customer_status`, `idx_lc_sub_history`

---

## 5. Kafka setup — DONE (Day 2)

**Broker:** `confluentinc/cp-kafka:7.5.0`
- Internal: `kafka:29092` | External: `localhost:9092`
- `AUTO_CREATE_TOPICS_ENABLE=false`
- 3 partitions, 168h retention

**13 topics** (created by `scripts/create_topics.sh`):
```
sub.new_subscription      sub.new_subscription.dlq
sub.renewal               sub.renewal.dlq
sub.cancellation          sub.cancellation.dlq
sub.refund                sub.refund.dlq
sub.expiry                sub.expiry.dlq
sub.extension             sub.extension.dlq
sub.audit_log
```

**Kafka UI:** `provectuslabs/kafka-ui:latest` → `http://localhost:8080`

---

## 6. Shared library — DONE (Day 5)

### src/shared/constants.py
```python
TOPIC_MAP: dict[str, str]       # event_type → "sub.<event_type>"
DLQ_TOPIC_MAP: dict[str, str]   # event_type → "sub.<event_type>.dlq"
AUDIT_TOPIC = "sub.audit_log"
PLANS = ["basic", "premium", "enterprise"]  # ordered: index = tier level
PLAN_PRICES = {"basic": 4.99, "premium": 9.99, "enterprise": 29.99}
VALID_STATUSES = ["active", "expired", "cancelled", "pending_renewal", "suspended"]
CONSUMER_GROUP_ID = "streamforge-etl-v1"
```

### src/shared/schemas.py — 6 Pydantic v2 event models

**BaseEvent** (envelope on every event):
```python
event_id:        str   # uuid4, unique per message
event_type:      str   # matches TOPIC_MAP key
event_version:   str   # "1.0"
occurred_at:     str   # ISO 8601 UTC
subscription_id: str   # UUID — Kafka partition key
customer_id:     str   # UUID
idempotency_key: str   # uuid4 default; producers set deterministic value

def kafka_key() -> bytes          # subscription_id.encode()
def to_kafka_payload() -> bytes   # model_dump_json().encode("utf-8")
@classmethod from_kafka_payload(raw: bytes) -> BaseEvent
```

**Subclasses and their extra fields:**

| Class | Key extra fields |
|---|---|
| `NewSubscriptionEvent` | plan, amount_usd (Decimal), expires_at, auto_renew, source |
| `RenewalEvent` | plan, amount_usd, previous_expiry, new_expiry, renewal_attempt |
| `CancellationEvent` | reason, cancelled_by, effective_date, refund_eligible |
| `RefundEvent` | transaction_id, refund_amount, original_amount, reason, initiated_by |
| `ExpiryEvent` | plan, expired_at, auto_renew |
| `ExtensionEvent` | extended_by_days, previous_expiry, new_expiry, reason, extended_by |

**EVENT_TYPE_MAP** — `dict[str, type[BaseEvent]]` for consumer dispatch without if/elif chains.

**Validation rules enforced:**
- `plan` is `Literal["basic","premium","enterprise"]` — rejects unknown plans at producer
- `amount_usd` / `refund_amount` are `Decimal` — exact monetary arithmetic
- `refund_amount` ≤ `original_amount` — model_validator rejects over-refunds
- All datetime fields accept `datetime` objects or ISO strings; naive datetimes coerced to UTC
- `extra="forbid"` — unknown fields raise immediately (catches schema drift)
- `frozen=True` — events are immutable after construction

### src/shared/logger.py
```python
get_logger(name, service=None) -> logging.Logger
# Returns cached stdlib Logger with JSON formatter → stdout
# Level from LOG_LEVEL env var (default INFO)

# Standardised helpers called at the same points in every service:
log_kafka_received(logger, *, topic, partition, offset, event_type, event_id)
log_kafka_processed(logger, *, event_type, event_id, subscription_id, duration_ms, success)
log_dlq_routed(logger, *, source_topic, dlq_topic, event_id, reason, retry_count)
```

JSON output format:
```json
{"ts":"2025-01-15T10:23:01+00:00","level":"INFO","service":"consumer",
 "logger":"consumer.handlers","msg":"kafka message processed",
 "event_type":"renewal","duration_ms":14.2,"success":true}
```

---

## 7. State machine design — NOT BUILT (Day 8)

```python
TRANSITIONS: dict[tuple, str | None] = {
    (None,              "new_subscription"): "active",
    ("active",          "renewal"):          "active",
    ("pending_renewal", "renewal"):          "active",
    ("expired",         "renewal"):          "active",
    ("active",          "cancellation"):     "cancelled",
    ("pending_renewal", "cancellation"):     "cancelled",
    ("suspended",       "cancellation"):     "cancelled",
    ("active",          "refund"):           None,
    ("cancelled",       "refund"):           None,
    ("expired",         "refund"):           None,
    ("active",          "expiry"):           "expired",
    ("pending_renewal", "expiry"):           "expired",
    ("active",          "extension"):        "active",
    ("expired",         "extension"):        "active",
    ("cancelled",       "extension"):        "active",
    ("suspended",       "extension"):        "active",
    ("pending_renewal", "extension"):        "active",
}
# resolve_transition(current_status, event_type) → (is_valid: bool, new_status: str|None, reason: str)
```

---

## 8. Consumer design — NOT BUILT (Days 8–11)

Key principles:
- **Manual offset commit** — commit AFTER successful Postgres write
- **Idempotency** — check `idempotency_key` in transactions table before processing
- **DLQ routing** — failed messages → `sub.<type>.dlq` topic AND `dlq_events` table
- **At-least-once delivery** — Kafka may redeliver; idempotency handles duplicates
- **Graceful shutdown** — SIGTERM drains in-flight messages then exits
- **Stats logging** — processed/skipped/dlq/errors every 30 seconds

Consumer group: `streamforge-etl-v1`

---

## 9. CS API design — NOT BUILT (Days 15–19)

FastAPI. All write actions publish a Kafka event (not direct DB write).

| Endpoint | Role | Action |
|---|---|---|
| GET /subscriptions/:id | agent, admin | full detail |
| GET /subscriptions | agent, admin | list with filter |
| GET /subscriptions/:id/history | agent, admin | lifecycle_events |
| POST /subscriptions/:id/extend | agent, admin | publishes ExtensionEvent |
| POST /subscriptions/:id/cancel | agent, admin | publishes CancellationEvent |
| POST /transactions/:id/refund | admin only | publishes RefundEvent |
| GET /audit | admin only | filter by agent_id, sub_id, date |
| POST /auth/login | public | returns JWT |
| GET /health | public | health check |

JWT roles: `agent`, `admin`. `performed_by` = `cs_agent:<agent_id>` in lifecycle_events.

---

## 10. 30-day progress tracker

| Day | Topic | Status |
|---|---|---|
| 1 | Project scaffolding | ✅ Done |
| 2 | Docker: Zookeeper + Kafka + 13 topics | ✅ Done |
| 3 | Docker: Postgres schema + Kafka UI | ✅ Done |
| 4 | Schema hardening — CHECK + partial indexes | ✅ Done |
| 5 | src/shared/ — schemas, constants, logger | ✅ Done |
| 6 | Kafka producer service | ⬜ Next |
| 7 | End-to-end smoke test | ⬜ |
| 8 | State machine | ⬜ |
| 9 | Postgres DB layer | ⬜ |
| 10 | Event handlers | ⬜ |
| 11 | Kafka consumer loop | ⬜ |
| 12 | Integration test — full pipeline | ⬜ |
| 13 | Idempotency + DLQ tests | ⬜ |
| 14 | Week 2 review + docs | ⬜ |
| 15 | FastAPI setup | ⬜ |
| 16 | Subscription read routes | ⬜ |
| 17 | CS action routes | ⬜ |
| 18 | JWT auth middleware | ⬜ |
| 19 | Audit logging | ⬜ |
| 20 | Streamlit dashboard | ⬜ |
| 21 | Week 3 review + API docs | ⬜ |
| 22 | AWS MSK (Terraform) | ⬜ |
| 23 | S3 data lake (Terraform) | ⬜ |
| 24 | AWS Glue PySpark jobs | ⬜ |
| 25 | Redshift schema + COPY | ⬜ |
| 26 | CloudWatch monitoring + alarms | ⬜ |
| 27 | Load testing (100K burst) | ⬜ |
| 28 | Redshift Spectrum (cold queries) | ⬜ |
| 29 | End-to-end AWS test | ⬜ |
| 30 | Docs + resume handoff | ⬜ |

---

## 11. Key design decisions

| Decision | Choice | Reason |
|---|---|---|
| Repo layout | Monorepo with src/ | All services share schemas via src/shared/ |
| Event schemas | Pydantic v2, single file | Producer + consumer import same models — zero drift |
| Kafka partitioning | By subscription_id | All events for one sub go to same partition — ordering |
| Offset commit | Manual, after DB write | At-least-once delivery; idempotency handles duplicates |
| DB primary keys | UUID for subs, BIGSERIAL for events | UUID = producer-generated; BIGSERIAL = natural audit ordering |
| Money storage | NUMERIC(10,2) / Decimal | Float cannot represent 0.10 exactly — billing corruption |
| Timestamps | TIMESTAMPTZ / ISO 8601 str | XYS users in every timezone; UTC internally |
| Constraint additions | NOT VALID + VALIDATE | Zero-downtime for large table changes |
| CS actions | Publish Kafka event, not direct DB | Same audit trail, same state machine as UI events |
| Topic naming | sub.<event_type> + .dlq | Clear prefix, DLQ matches source 1:1 |
| amount_usd sign | Positive in event, negative in DB | Events are readable; DB convention is money-out = negative |
| EVENT_TYPE_MAP | dict[str, type[BaseEvent]] | Consumer dispatch without if/elif chains |
| extra="forbid" | On all event models | Schema drift surfaces immediately, not silently |

---

## 12. Local service ports

| Service | Port | URL |
|---|---|---|
| Kafka (host) | 9092 | `localhost:9092` |
| Kafka (Docker internal) | 29092 | `kafka:29092` |
| Zookeeper | 2181 | internal only |
| Postgres | 5432 | `localhost:5432` |
| Kafka UI | 8080 | `http://localhost:8080` |
| FastAPI CS API | 8000 | `http://localhost:8000/docs` |
| Streamlit dashboard | 8501 | `http://localhost:8501` |

---

## 13. Environment variables (.env.example)

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092    # host; containers use kafka:29092
CONSUMER_GROUP=streamforge-etl-v1
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=streamforge
POSTGRES_USER=sfuser
POSTGRES_PASSWORD=sfpassword
POSTGRES_DSN=postgresql://sfuser:sfpassword@localhost:5432/streamforge
LOG_LEVEL=INFO
SIMULATE_INTERVAL_SEC=1.5
```

---

## 14. Makefile commands

```bash
make up               # start all services
make down             # stop all
make clean            # stop + delete volumes (resets DB)
make logs             # follow all logs
make logs-producer    # producer logs only
make logs-consumer    # consumer logs only
make ps               # container statuses
make db-shell         # psql inside sf_postgres
make kafka-shell      # bash inside sf_kafka
make topics           # list all Kafka topics
make produce-once     # emit one of each event type    ← added Day 6
make produce-single EVENT=renewal  # one specific event ← added Day 6
```

---

## 15. Day 6 — what to build next

**`src/producer/`** — Kafka producer service:

```
src/producer/
├── Dockerfile
├── requirements.txt   ← confluent-kafka==2.3.0, faker==24.0.0, pydantic==2.x
└── producer.py        ← 3 modes controlled by --mode flag
```

**Three operating modes:**

| Mode | Trigger | Behaviour |
|---|---|---|
| `simulate` | `make up` (default) | Infinite loop, random event every `SIMULATE_INTERVAL_SEC` |
| `once` | `make produce-once` | Emit one of each of the 6 event types then exit |
| `single` | `make produce-single EVENT=renewal` | Emit one specific event type then exit |

**Producer responsibilities:**
- Import `NewSubscriptionEvent`, `RenewalEvent` etc. from `shared.schemas`
- Import `TOPIC_MAP` from `shared.constants`
- Build realistic fake data with `faker` (names, UUIDs, dates, amounts)
- Call `event.to_kafka_payload()` for value, `event.kafka_key()` for key
- Set a deterministic `idempotency_key` (not the default uuid4)
- Use `confluent_kafka.Producer` with delivery callback for error logging
- Use `get_logger("producer")` from `shared.logger`

**docker-compose addition (uncomment):**
```yaml
producer:
  build:
    context: .
    dockerfile: src/producer/Dockerfile
  networks: [streamforge]
  depends_on:
    kafka:
      condition: service_healthy
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    LOG_LEVEL: INFO
    SIMULATE_INTERVAL_SEC: 1.5
  restart: unless-stopped
```

---

## 16. Prompt to resume in a new chat

> I am a Senior Data Engineer building StreamForge — a real-time subscription lifecycle pipeline at XYS (antivirus company), 300M subscription scale. Here is my full project context: [paste context.md]. Days 1–5 are complete (Docker, Postgres schema, Kafka, schema hardening, shared Python library). Day 6 is next: build the Kafka producer service in src/producer/. Follow the same lesson format: explain each design decision as you build, and give me an updated context.md at the end.
