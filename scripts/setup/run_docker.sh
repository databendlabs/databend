#!/bin/bash

set -e

DOCKER_OPTIONS+=(-u root:root)
DOCKER_OPTIONS+=(-v /var/run/docker.sock:/var/run/docker.sock)
DOCKER_OPTIONS+=(--cap-add SYS_PTRACE --cap-add NET_RAW --cap-add NET_ADMIN)
DOCKER_OPTIONS+=("-it")
SOURCE_DIR="${PWD}"
HUB="${HUB:-datafuselabs}"
TAG="${TAG:-latest}"
SOURCE_DIR_MOUNT_DEST=/source
# how does it work?
# 1. docker build under root user with cargo installed on /opt/rust/cargo/bin
# 2. to handle with permission issue on target release, we grant a user with same uid/gid to the host user for target dir access
START_COMMAND=("/bin/bash" "-lc" "groupadd --gid $(id -g) -f databendgroup \
  && useradd -o --uid $(id -u) --gid $(id -g) --no-create-home --home-dir /source databendbuild \
  && chown -R databendbuild:databendgroup /source \
  && chown -R databendbuild:databendgroup /opt/rust/cargo \
  && chown -R databendbuild:databendgroup /tmp \
  && sudo -EHs -u databendbuild bash -c 'cd /source && export LC_NUMERIC='en_US.UTF-8' && export LC_ALL=en_US.UTF-8 && DATABEND_DEV_CONTAINER=TRUE RUST_BACKTRACE=full env CARGO_HOME=/opt/rust/cargo PATH=/home/rust/.cargo/bin:/opt/rust/cargo/bin:/usr/local/musl/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin $*'")

[[ -f .git ]] && [[ ! -d .git ]] && DOCKER_OPTIONS+=(-v "$(git rev-parse --git-common-dir):$(git rev-parse --git-common-dir)")
[[ -n "${SSH_AUTH_SOCK}" ]] && DOCKER_OPTIONS+=(-v "${SSH_AUTH_SOCK}:${SSH_AUTH_SOCK}" -e SSH_AUTH_SOCK)

# to get rid of confliction between host cargo directory and docker registry, we should use tmp directory instead of $HOME/.cargo directory
mkdir -p /tmp/dev/.cargo/git
mkdir -p /tmp/dev/.cargo/registry
CARGO_GIT_DIR=/tmp/dev/.cargo/git
CARGO_REGISTRY_DIR=/tmp/dev/.cargo/registry

docker run --rm \
	"${DOCKER_OPTIONS[@]}" \
	-v "${SOURCE_DIR}":"${SOURCE_DIR_MOUNT_DEST}" \
	-v "${CARGO_GIT_DIR}":"/opt/rust/cargo/git" \
	-v "${CARGO_REGISTRY_DIR}":"/opt/rust/cargo/registry" \
	"${HUB}"/dev-container:"${TAG}" \
	"${START_COMMAND[@]}"
