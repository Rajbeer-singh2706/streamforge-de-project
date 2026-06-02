.PHONY: start down restart logs ps clean db-shell

start:
	docker-compose up -d
	@echo "Kafka UI → http://localhost:8080"

stop:
	docker-compose stop
	
clean:
	docker-compose down -v --remove-orphans

logs:
	docker-compose logs -f

logs-producer:
	docker-compose logs -f producer

logs-consumer:
	docker-compose logs -f consumer

ps:
	docker-compose ps

db-shell:
	docker exec -it sf_postgres psql -U sfuser -d streamforge

kafka-shell:
	docker exec -it sf_kafka bash

topics:
	docker exec sf_kafka kafka-topics \
	  --bootstrap-server localhost:29092 --list