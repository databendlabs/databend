# Setup dev toolchain
setup:
	bash ./scripts/dev_setup.sh

test:
	cargo test -- --nocapture

bench:
	cargo bench -- --nocapture

run:
	RUSTFLAGS="-C target-cpu=native" cargo run --release

build:
	RUSTFLAGS="-C target-cpu=native" cargo build --release

profile:
	RUSTFLAGS="-g" cargo flamegraph --bin=fuse-query

fmt:
	cargo fmt

lint:
	cargo fmt
	cargo clippy -- -D warnings

docker:
	docker build --network host -f docker/Dockerfile -t datafusedev/fuse-query .

coverage:
	bash ./scripts/dev_codecov.sh

clean:
	cargo clean

.PHONY: setup test bench run build fmt lint docker coverage clean
