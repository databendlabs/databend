HUB ?= datafusedev
TAG ?= latest
PLATFORM ?= linux/amd64,linux/arm64,linux/arm/v7,linux/arm/v6,linux/ppc64le

# Setup dev toolchain
setup:
	bash ./scripts/setup/dev_setup.sh

run: build
	bash ./scripts/deploy/fusequery-standalone.sh release

run-debug: build-debug
	bash ./scripts/deploy/fusequery-standalone.sh

run-release:
	bash ./scripts/deploy/fusequery-standalone-from-release.sh

build:
	bash ./scripts/build/build-native.sh

build-debug:
	bash ./scripts/build/build-debug.sh

build-cli:
	bash ./scripts/build/build-cli.sh

unit-test:
	bash ./scripts/ci/ci-run-unit-tests.sh

stateless-test:
	bash ./scripts/build/build-debug.sh
	bash ./scripts/ci/ci-run-stateless-tests.sh

stateless-cluster-test:
	bash ./scripts/build/build-debug.sh
	bash ./scripts/ci/ci-run-stateless-tests-cluster.sh

test: unit-test stateless-test

fmt:
	cargo fmt

lint:
	cargo fmt
	cargo clippy -- -D warnings

docker:
	docker build --network host -f docker/Dockerfile -t ${HUB}/fuse-query:${TAG} .

# experiment feature: take a look at docker/README.md for detailed multi architecture image build support
dockerx:
	docker buildx build . -f ./docker/Dockerfile  --platform ${PLATFORM} --allow network.host --builder host -t ${HUB}/fuse-query:${TAG} --push

perf-tool:
	docker build --network host -f docker/perf-tool/Dockerfile -t ${HUB}/perf-tool:${TAG} .

run-helm:
	helm upgrade --install datafuse ./charts/datafuse \
		--set image.repository=${HUB}/fuse-query --set image.tag=${TAG} --set configs.mysqlPort=3308
profile:
	bash ./scripts/ci/ci-run-profile.sh

clean:
	cargo clean

.PHONY: setup test run build fmt lint docker clean
