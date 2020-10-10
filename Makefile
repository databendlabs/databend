fmt:
	cargo fmt

test:
	cargo +nightly test -- --nocapture 

lint:
	cargo fmt
	cargo +nightly clippy -- -D warnings

clean:
	rm -rf target/

.PHONY: test lint clean
