test:
	cargo +nightly test -- --nocapture 

bench:
	cargo +nightly bench -- --nocapture

run:
	cargo +nightly run --release -- --nocapture

build:
	cargo +nightly build --release

lint:
	cargo fmt
	cargo +nightly clippy -- -D warnings

docker:
	docker build -f docker/Dockerfile .

clean:
	cargo clean

.PHONY: test bench run build lint docker clean
