.PHONY: check integration-tests integration-tests-flight-sql

default: check

check:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings
	cargo audit

build:
	cargo build

test:
	cargo test --all --all-features --lib -- --nocapture

integration-down:
	make -C tests down

integration-tests:
	make -C tests

integration-tests-flight-sql:
	make -C tests test-flight-sql

integration-bendsql:
	make -C tests test-bendsql
