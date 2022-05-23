HUB ?= datafuselabs
TAG ?= latest
PLATFORM ?= linux/amd64,linux/arm64
VERSION ?= latest
ADD_NODES ?= 0
NUM_CPUS ?= 2
TENANT_ID ?= "tenant"
CLUSTER_ID ?= "test"

# yamllint vars
YAML_FILES ?= $(shell find . -path ./target -prune -type f -name '*.yaml' -or -name '*.yml')
YAML_DOCKER_ARGS ?= run --rm --user "$$(id -u)" -v "$${PWD}:/component" --workdir /component
YAMLLINT_ARGS ?= --no-warnings
YAMLLINT_CONFIG ?= .yamllint.yml
YAMLLINT_IMAGE ?= docker.io/cytopia/yamllint:latest
YAMLLINT_DOCKER ?= docker $(YAML_DOCKER_ARGS) $(YAMLLINT_IMAGE)

CARGO_TARGET_DIR ?= $(CURDIR)/target

# Setup dev toolchain
setup:
	bash ./scripts/setup/dev_setup.sh

fmt:
	cargo fmt

lint:
	cargo fmt
	cargo clippy --all -- -D warnings
	# Cargo.toml file formatter(make setup to install)
	taplo fmt
	# Python file formatter(make setup to install)
	yapf -ri tests/
	# Bash file formatter(make setup to install)
	shfmt -l -w scripts/*

lint-yaml: $(YAML_FILES)
	$(YAMLLINT_DOCKER) -f parsable -c $(YAMLLINT_CONFIG) $(YAMLLINT_ARGS) -- $?

miri:
	cargo miri setup
	MIRIFLAGS="-Zmiri-disable-isolation" cargo miri test

run: build-release
	bash ./scripts/ci/deploy/databend-query-standalone.sh release

run-debug: build-debug
	bash ./scripts/ci/deploy/databend-query-standalone.sh

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

build-debug:
	echo "Please use 'make build' instead"

cross-compile-debug:
	cross build --target aarch64-unknown-linux-gnu

cross-compile-release:
	RUSTFLAGS="-C link-arg=-Wl,--compress-debug-sections=zlib-gabi" cross build --target aarch64-unknown-linux-gnu --release

unit-test:
	ulimit -n 10000;ulimit -s 16384; RUST_LOG="ERROR" bash ./scripts/ci/ci-run-unit-tests.sh

embedded-meta-test: build-debug
	rm -rf ./_meta_embedded*
	bash ./scripts/ci/ci-run-tests-embedded-meta.sh

stateless-test: build-debug
	rm -rf ./_meta*/
	rm -rf .databend
	ulimit -n 10000;ulimit -s 16384; bash ./scripts/ci/ci-run-tests-embedded-meta.sh

sqllogic-test: build-debug
	rm -rf ./_meta*/
	ulimit -n 10000;ulimit -s 16384; bash ./scripts/ci/ci-run-sqllogic-tests.sh

management-test: build-debug
	rm -rf ./_meta*/
	ulimit -n 10000;ulimit -s 16384; bash ./scripts/ci/ci-run-stateless-tests-management-mode.sh

stateless-cluster-test: build-debug
	rm -rf ./_meta*/
	bash ./scripts/ci/ci-run-stateless-tests-cluster.sh

stateless-cluster-test-tls: build-debug
	rm -rf ./_meta*/
	bash ./scripts/ci/ci-run-stateless-tests-cluster-tls.sh

metactl-test: build-debug
	bash ./tests/metactl/test-metactl.sh

test: unit-test stateless-test sqllogic-test metactl-test

docker:
	docker build --network host -f docker/Dockerfile -t ${HUB}/databend-query:${TAG} .

k8s-docker:
	bash ./scripts/build/build-k8s-runner.sh

# experiment feature: take a look at docker/README.md for detailed multi architecture image build support
dockerx:
	docker buildx build . -f ./docker/Dockerfile  --platform ${PLATFORM} --allow network.host --builder host -t ${HUB}/databend-query:${TAG} --build-arg VERSION=${VERSION} --push

build-tool:
	bash ./scripts/build/build-tool-runner.sh

# generate common/functions/src/scalars/arithmetics/result_type.rs
run-codegen:
	cargo run --manifest-path=common/codegen/Cargo.toml

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
