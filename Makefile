HUB ?= datafuselabs
TAG ?= latest
PLATFORM ?= linux/amd64,linux/arm64
VERSION ?= latest
ADD_NODES ?= 0
NUM_CPUS ?= 2
TENANT_ID ?= "tenant"
CLUSTER_ID ?= "test"

CARGO_TARGET_DIR ?= $(CURDIR)/target

# Setup dev toolchain
setup:
	bash ./scripts/setup/dev_setup.sh

fmt:
	cargo fmt --all

lint:
	cargo fmt --all
	# Cargo.toml file formatter(make setup to install)
	taplo fmt
	# Python file formatter(make setup to install)
	yapf -ri tests/
	# Bash file formatter(make setup to install)
	shfmt -l -w scripts/*
	cargo clippy --workspace --all-targets -- -D warnings

lint-yaml:
	yamllint -f auto .

check-license:
	license-eye -v info -c .licenserc.yaml header check

run: build-release
	BUILD_PROFILE=release bash ./scripts/ci/deploy/databend-query-standalone.sh

run-debug: build
	bash ./scripts/ci/deploy/databend-query-standalone.sh

run-debug-management: build
	bash ./scripts/ci/deploy/databend-query-management-mode.sh

build:
	bash ./scripts/build/build-debug.sh

build-release:
	bash ./scripts/build/build-release.sh
ifeq ($(shell uname),Linux) # Macs don't have objcopy
	# Reduce binary size by compressing binaries.
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/databend-query
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/databend-meta
	objcopy --compress-debug-sections=zlib-gnu ${CARGO_TARGET_DIR}/release/databend-metactl
endif

build-native:
	bash ./scripts/build/build-native.sh

unit-test:
	ulimit -n 10000;ulimit -s 16384; RUST_LOG="ERROR" bash ./scripts/ci/ci-run-unit-tests.sh

embedded-meta-test: build
	rm -rf ./_meta_embedded*
	bash ./scripts/ci/ci-run-tests-embedded-meta.sh

stateless-test: build
	rm -rf ./_meta*/
	rm -rf .databend
	ulimit -n 10000;ulimit -s 16384; bash ./scripts/ci/ci-run-tests-embedded-meta.sh

sqllogic-test: build
	rm -rf ./_meta*/
	ulimit -n 10000;ulimit -s 16384; bash ./scripts/ci/ci-run-sqllogic-tests.sh

stateless-cluster-test: build
	rm -rf ./_meta*/
	bash ./scripts/ci/ci-run-stateless-tests-cluster.sh

stateless-cluster-test-tls: build
	rm -rf ./_meta*/
	bash ./scripts/ci/ci-run-stateless-tests-cluster-tls.sh

metactl-test:
	bash ./tests/metactl/test-metactl.sh
	bash ./tests/metactl/test-metactl-restore-new-cluster.sh

meta-kvapi-test:
	bash ./tests/meta-kvapi/test-meta-kvapi.sh

meta-bench: build-release
	bash ./scripts/benchmark/run-meta-benchmark.sh 10 1000

test: unit-test stateless-test sqllogic-test metactl-test

docker:
	docker build --network host -f docker/Dockerfile -t ${HUB}/databend-query:${TAG} .

# experiment feature: take a look at docker/README.md for detailed multi architecture image build support
dockerx:
	docker buildx build . -f ./docker/Dockerfile  --platform ${PLATFORM} --allow network.host --builder host -t ${HUB}/databend-query:${TAG} --build-arg VERSION=${VERSION} --push

build-tool:
	bash ./scripts/build/build-tool-runner.sh

# used for the build of dev container
dev-container:
	cp ./rust-toolchain.toml ./docker/build-tool
	docker build ./docker/build-tool -t ${HUB}/dev-container:${TAG} -f ./docker/build-tool/Dockerfile\

profile:
	bash ./scripts/ci/ci-run-profile.sh

clean:
	cargo clean
	rm -f ./nohup.out ./tests/suites/0_stateless/*.stdout-e
	rm -rf ./_meta*/ ./_logs*/ ./query/_logs*/ ./metasrv/_logs*/ ./stateless_test_data/
	rm -rf ./common/base/_logs*/ ./common/meta/raft-store/_logs*/ ./common/meta/sled-store/_logs*/
	rm -rf ./.databend ./query/.databend ./meta/.databend

.PHONY: setup test run build fmt lint docker clean
