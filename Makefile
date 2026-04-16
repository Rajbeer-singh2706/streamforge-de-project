.PHONY: up down restart logs ps topics db-shell kafka-shell clean

# ── Cluster lifecycle ─────────────────────────────────────────
up:
	docker-compose up -d
	@echo ""
	@echo "  StreamForge is starting up..."
	@echo "  Kafka UI  → http://localhost:8080"
	@echo "  Postgres  → localhost:5432  (sfuser / sfpassword)"
	@echo "  Kafka     → localhost:9092"
	@echo ""
	@echo "  Run 'make logs' to follow all service logs"

down:
	docker-compose down

restart:
	docker-compose restart

clean:
	docker-compose down -v --remove-orphans
	@echo "All containers and volumes removed."

# ── Logs ─────────────────────────────────────────────────────
logs:
	docker-compose logs -f

logs-producer:
	docker-compose logs -f producer

logs-kafka:
	docker-compose logs -f kafka

# ── Status ───────────────────────────────────────────────────
ps:
	docker-compose ps

topics:
	docker exec sf_kafka kafka-topics \
	  --bootstrap-server localhost:29092 --list

topic-describe:
	docker exec sf_kafka kafka-topics \
	  --bootstrap-server localhost:29092 \
	  --describe --topic $(TOPIC)

# ── Shells ───────────────────────────────────────────────────
db-shell:
	docker exec -it sf_postgres psql -U sfuser -d streamforge

kafka-shell:
	docker exec -it sf_kafka bash

producer-shell:
	docker exec -it sf_producer bash

# ── Consume from a topic (for debugging) ─────────────────────
consume:
	docker exec sf_kafka kafka-console-consumer \
	  --bootstrap-server localhost:29092 \
	  --topic $(TOPIC) \
	  --from-beginning \
	  --max-messages 10

# ── Run producer manually ────────────────────────────────────
produce-once:
	docker exec sf_producer python producer.py --mode once

produce-single:
	docker exec sf_producer python producer.py --mode single --event $(EVENT)
