.PHONY: start stop restart logs ps clean db-shell produce-once produce-single logs-producer

start:
	docker-compose up -d
	@echo "Kafka UI → http://localhost:8080"

stop:
	docker-compose stop
	
clean:
	docker-compose down -v --remove-orphans

logs:
	docker-compose logs -f

ps:
	docker-compose ps

db-shell:
	docker exec -it sf_postgres psql -U sfuser -d streamforge

kafka-shell:
	docker exec -it sf_kafka bash

topics:
	docker exec sf_kafka kafka-topics \
	  --bootstrap-server localhost:29092 --list

## Emit one of each event type and exit (smoke test helper)
produce-once:
	docker compose run --rm producer \
		python producer/producer.py --mode once
 
## Emit one specific event type and exit
## Usage: make produce-single EVENT=renewal
produce-single:
ifndef EVENT
	$(error EVENT is required. Usage: make produce-single EVENT=renewal)
endif
	docker compose run --rm producer \
		python producer/producer.py --mode single --event $(EVENT)
 
## Tail producer logs only
logs-producer:
	docker compose logs -f producer