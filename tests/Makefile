PHONY: test

default: test

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --quiet-pull -d databend

start: up

test: up
	docker compose run --quiet-pull --rm test

run: test

test-flight-sql: up
	docker compose run --quiet-pull --rm test-flight-sql

down:
	docker compose down

stop: down
