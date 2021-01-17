test:
	cargo test -- --nocapture

bench:
	cargo bench -- --nocapture

run:
	cargo run --release -- --nocapture

build:
	cargo build --release

profile:
	RUSTFLAGS="-g" cargo flamegraph --bin=fuse-query

lint:
	cargo fmt
	cargo clippy -- -D warnings

docker:
	docker build --network host -f docker/Dockerfile -t datafusedev/fuse-query .

clean:
	cargo clean

.PHONY: test bench run build lint docker clean
