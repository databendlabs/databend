fmt:
	cargo fmt

test:
	cargo +nightly test -- --nocapture 

lint:
	cargo fmt
	cargo +nightly clippy -- -D warnings

clean:
	cargo clean

.PHONY: test lint clean
