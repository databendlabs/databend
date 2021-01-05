test:
	cargo +nightly test -- --nocapture 

bench:
	RUSTFLAGS="-C target-cpu=native" cargo +nightly bench -- --nocapture

run:
	RUSTFLAGS="-C target-cpu=native" cargo +nightly run --release -- --nocapture

build:
	RUSTFLAGS="-C target-cpu=native" cargo +nightly build --release

lint:
	cargo fmt
	cargo +nightly clippy -- -D warnings

docker:
	docker build --network host -f docker/Dockerfile -t datafusedev/fuse-query .

clean:
	cargo clean

.PHONY: test bench run build lint docker clean
