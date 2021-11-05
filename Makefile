HUB ?= datafuselabs
TAG ?= latest
PLATFORM ?= linux/amd64
VERSION ?= latest
ADD_NODES ?= 0
NUM_CPUS ?= 2
# Setup dev toolchain
setup:
	bash ./scripts/setup/dev_setup.sh

fmt:
	cargo fmt

lint:
	cargo fmt
	cargo clippy --tests -- -D warnings

miri:
	cargo miri setup
	MIRIFLAGS="-Zmiri-disable-isolation" cargo miri test

cluster: build
	mkdir -p ./.databend/local/bin && ./target/release/bendctl cluster stop --databend_dir ./.databend --group local
	cp ./target/release/databend-query ./.databend/local/bin/test/databend-query
	cp ./target/release/databend-meta ./.databend/local/bin/test/databend-meta
	./target/release/bendctl cluster create --databend_dir ./.databend --group local --version test --num-cpus ${NUM_CPUS} --force
	for i in `seq 1 ${ADD_NODES}`; do make cluster_add; done;
	make cluster_view
cluster_add:
	./target/release/bendctl cluster add --databend_dir ./.databend --group local --num-cpus ${NUM_CPUS}
cluster_view:
	./target/release/bendctl cluster view --databend_dir ./.databend --group local
run: build
	bash ./scripts/deploy/databend-query-standalone.sh release

run-debug: build-debug
	bash ./scripts/deploy/databend-query-standalone.sh

build:
	bash ./scripts/build/build-release.sh

build-native:
	bash ./scripts/build/build-native.sh

build-debug:
	bash ./scripts/build/build-debug.sh

build-tool:
	docker build . -t ${HUB}/build-tool:arm-unknown-linux-gnueabi  --file ./docker/build-tool/armv6/Dockerfile
	docker build . -t ${HUB}/build-tool:aarch64-unknown-linux-gnu  --file ./docker/build-tool/arm64/Dockerfile
	docker build . -t ${HUB}/build-tool:armv7-unknown-linux-gnueabihf  --file ./docker/build-tool/armv7/Dockerfile
	docker push ${HUB}/build-tool:arm-unknown-linux-gnueabi
	docker push ${HUB}/build-tool:aarch64-unknown-linux-gnu
	docker push ${HUB}/build-tool:armv7-unknown-linux-gnueabihf

cross-compile-debug:
	cross build --target aarch64-unknown-linux-gnu
	cross build --target arm-unknown-linux-gnueabi
	cross build --target armv7-unknown-linux-gnueabihf

cross-compile-release:
	cross build --target aarch64-unknown-linux-gnu --release
	cross build --target arm-unknown-linux-gnueabi --release
	cross build --target armv7-unknown-linux-gnueabihf --release

cli-build:
	bash ./scripts/build/build-cli.sh build-cli

cli-install:
	bash ./scripts/build/build-cli.sh install-cli

cli-test:
	bash ./scripts/ci/ci-run-cli-unit-tests.sh

unit-test:
	ulimit -n 10000; bash ./scripts/ci/ci-run-unit-tests.sh

embedded-meta-test: build-debug
	rm -rf ./_meta_embedded
	bash ./scripts/ci/ci-run-tests-embedded-meta.sh

stateless-test: build-debug
	rm -rf ./_meta/
	ulimit -n 10000; bash ./scripts/ci/ci-run-stateless-tests-standalone.sh

stateless-cluster-test: build-debug
	rm -rf ./_meta/
	bash ./scripts/ci/ci-run-stateless-tests-cluster.sh

stateless-cluster-test-tls: build-debug
	rm -rf ./_meta/
	bash ./scripts/ci/ci-run-stateless-tests-cluster-tls.sh

test: unit-test stateless-test

docker:
	docker build --network host -f docker/Dockerfile -t ${HUB}/databend-query:${TAG} .

docker_release:
	docker buildx build . -f ./docker/release/Dockerfile  --platform ${PLATFORM} --allow network.host --builder host -t ${HUB}/databend:${TAG} --build-arg VERSION=${VERSION}--push

# experiment feature: take a look at docker/README.md for detailed multi architecture image build support
dockerx:
	docker buildx build . -f ./docker/Dockerfile  --platform ${PLATFORM} --allow network.host --builder host -t ${HUB}/databend-query:${TAG} --build-arg VERSION=${VERSION} --push

build-perf-tool:
	cargo build --target x86_64-unknown-linux-gnu --bin databend-benchmark
	mkdir -p ./distro
	mv ./target/x86_64-unknown-linux-gnu/debug/databend-benchmark  ./distro

perf-tool: build-perf-tool
	docker buildx build . -f ./docker/perf-tool/Dockerfile  --platform linux/amd64 --allow network.host --builder host -t ${HUB}/perf-tool:${TAG} --push

profile:
	bash ./scripts/ci/ci-run-profile.sh

clean:
	cargo clean
	rm -f ./nohup.out ./tests/suites/0_stateless/*.stdout-e
	rm -rf ./_meta/ ./_logs/ ./query/_logs/ ./metasrv/_logs/
	rm -rf ./common/base/_logs/ ./common/meta/raft-store/_logs/ ./common/meta/sled-store/_logs/

.PHONY: setup test run build fmt lint docker clean
