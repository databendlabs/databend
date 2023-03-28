PHONY: test

default: test

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --quiet-pull -d databend

test: up
	docker compose run --quiet-pull test

down:
	docker compose down
