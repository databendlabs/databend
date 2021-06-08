HUB ?= datafusedev
TAG ?= latest

# Setup dev toolchain
setup:
	bash ./scripts/setup/dev_setup.sh

run:
	bash ./scripts/build/build-debug.sh
	bash ./scripts/deploy/fusequery-standalone.sh

build:
	bash ./scripts/build/build-native.sh

profile:
	bash ./scripts/ci/ci-run-profile.sh

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

runhelm:
	helm upgrade --install datafuse ./charts/datafuse \
		--set image.repository=${HUB}/fuse-query --set image.tag=${TAG} --set configs.mysqlPort=3308

clean:
	cargo clean

.PHONY: setup test bench run runhelm build fmt lint docker coverage clean
