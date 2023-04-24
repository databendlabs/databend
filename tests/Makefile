PHONY: test

default: test

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --quiet-pull -d databend --wait

start: up

test: up
	cargo test --test '*'

run: test

test-flight-sql: up
	TEST_DATABEND_DSN=databend+flight://root:@localhost:8900/default?sslmode=disable cargo test --features flight-sql --test driver

test-bendsql: up
	cd .. && ./cli/test.sh http
	cd .. && ./cli/test.sh flight

down:
	docker compose down

stop: down
