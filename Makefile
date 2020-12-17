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
	docker build --network host -f docker/Dockerfile -t datafusedev/fuse-query .

clean:
	cargo clean

.PHONY: test bench run build lint docker clean
