HUB ?= datafuselabs
TAG ?= latest
# Setup dev toolchain
setup:
	bash ./scripts/dev_setup.sh

test:
	cargo test

bench:
	cargo bench

run:
	RUST_BACKTRACE=full RUSTFLAGS="-C target-cpu=native" cargo run --bin=fuse-query --release

runquery:
	RUST_BACKTRACE=full RUSTFLAGS="-C target-cpu=native" cargo run --bin=fuse-query --release

runstore:
	RUST_BACKTRACE=full RUSTFLAGS="-C target-cpu=native" cargo run --bin=fuse-store --release

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
	docker build --network host -f docker/Dockerfile -t ${HUB}/fuse-query:${TAG} .

coverage:
	bash ./scripts/dev_codecov.sh

clean:
	cargo clean

.PHONY: setup test bench run runquery runstore build fmt lint docker coverage clean
