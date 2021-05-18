HUB ?= datafusedev
TAG ?= latest
# Setup dev toolchain
setup:
	bash ./scripts/setup/dev_setup.sh

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

runhelm:
	helm upgrade --install datafuse ./charts/datafuse \
		--set image.repository=${HUB}/fuse-query --set image.tag=${TAG} --set configs.mysqlPort=3308

clean:
	cargo clean

.PHONY: setup test bench run runquery runstore runhelm build fmt lint docker coverage clean
