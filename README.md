# StreamForge — Module 1: Docker Environment

Local development stack for the StreamForge subscription pipeline.

## Services

| Service      | Container       | Port  | Description                     |
|-------------|-----------------|-------|---------------------------------|
| Kafka        | sf_kafka        | 9092  | Message broker                  |
| Zookeeper    | sf_zookeeper    | 2181  | Kafka coordination              |
| Kafka UI     | sf_kafka_ui     | 8080  | Web UI to inspect topics/msgs   |
| PostgreSQL   | sf_postgres     | 5432  | Subscription state store        |
| Producer     | sf_producer     | —     | Event simulation service        |

## Quick Start

```bash
# 1. Start everything
make up

# 2. Check all services are healthy
make ps

# 3. Watch producer logs
make logs-producer

# 4. Open Kafka UI in browser
open http://localhost:8080

# 5. Connect to Postgres
make db-shell
```

## Kafka Topics

| Topic                      | Purpose                              |
|---------------------------|--------------------------------------|
| sub.new_subscription       | New purchases from UI/API            |
| sub.renewal                | Auto and manual renewals             |
| sub.cancellation           | Customer or CS cancellations         |
| sub.refund                 | Refund requests                      |
| sub.expiry                 | Expired subscription notifications   |
| sub.extension              | CS-initiated extensions              |
| sub.*.dlq                  | Dead letter queues (one per topic)   |
| sub.audit_log              | Full audit trail                     |

## Producer Modes

```bash
# Simulate continuous traffic (default)
make produce-once        # emit one of each event type

# Emit a single event type
make produce-single EVENT=new_subscription
make produce-single EVENT=cancellation

# Consume and inspect messages
make consume TOPIC=sub.new_subscription
make consume TOPIC=sub.renewal
```

## Database Access

```bash
make db-shell
```

```sql
-- Check subscriptions
SELECT * FROM v_subscription_summary;

-- Recent lifecycle events
SELECT * FROM lifecycle_events ORDER BY created_at DESC LIMIT 20;

-- DLQ inspection
SELECT * FROM dlq_events WHERE resolved = FALSE;
```

## Project Structure

```
streamforge/
├── docker-compose.yml       # All services
├── Makefile                 # Dev shortcuts
├── .env.example             # Config reference
├── scripts/
│   ├── create_topics.sh     # Kafka topic setup
│   └── init_db.sql          # Postgres schema + seed data
└── producer/
    ├── Dockerfile
    ├── requirements.txt
    ├── schemas.py           # Pydantic event models
    └── producer.py          # Kafka producer (3 modes)
```

## Next Module

Once this is running, proceed to **Module 2: Kafka Consumer + ETL pipeline**
which reads from these topics and processes events into PostgreSQL.

