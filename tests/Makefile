default: run

run: test-core test-driver test-bendsql down

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --quiet-pull -d databend --wait

start: up

test-core: up
	cargo test --test core

test-driver: up
	cargo test --test driver
	TEST_DATABEND_DSN=databend+flight://root:@localhost:8900/default?sslmode=disable cargo test --features flight-sql --test driver

test-bendsql: up
	cd .. && ./cli/test.sh http
	cd .. && ./cli/test.sh flight

test-bindings-python: up
	cd ../bindings/python && pipenv run behave tests/*

test-bindings-nodejs: up
	cd ../bindings/nodejs && yarn test

down:
	docker compose down

stop: down
