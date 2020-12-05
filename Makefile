test:
	cargo +nightly test -- --nocapture 

bench:
	cargo +nightly bench -- --nocapture

run:
	cargo +nightly run -- --nocapture

build:
	cargo build --release 

lint:
	cargo fmt
	cargo +nightly clippy -- -D warnings

clean:
	cargo clean

.PHONY: test bench run build lint clean
