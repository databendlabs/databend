PHONY: test

default: test

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up -d databend

test: up
	docker compose run test

down:
	docker compose down
