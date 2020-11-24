test:
	cargo +nightly test -- --nocapture 

bench:
	cargo +nightly bench -- --nocapture

run:
	cargo +nightly run -- --nocapture

lint:
	cargo fmt
	cargo +nightly clippy -- -D warnings

clean:
	cargo clean

.PHONY: test bench run lint clean
