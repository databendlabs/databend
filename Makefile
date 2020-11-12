fmt:
	cargo fmt

test:
	cargo +nightly test -- --nocapture 

bench:
	cargo +nightly bench -- --nocapture

lint:
	cargo fmt
	cargo +nightly clippy -- -D warnings

clean:
	cargo clean

.PHONY: test lint clean
