HUB ?= datafusedev
TAG ?= latest

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

unit-test:
	bash ./scripts/ci/ci-run-unit-tests.sh

stateless-test:
	bash ./scripts/build/build-debug.sh
	bash ./scripts/ci/ci-run-stateless-tests.sh

test: unit-test stateless-test

fmt:
	cargo fmt

lint:
	cargo fmt
	cargo clippy -- -D warnings

docker:
	docker build --network host -f docker/Dockerfile -t ${HUB}/fuse-query:${TAG} .

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
