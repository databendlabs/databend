#!/usr/bin/env bash
# from: https://github.com/cross-rs/cross/blob/main/docker/musl.sh

set -x
set -euo pipefail

hide_output() {
    set +x
    trap "
      echo 'ERROR: An error was encountered with the build.'
      cat /tmp/build.log
      exit 1
    " ERR
    bash -c 'while true; do sleep 30; echo $(date) - building ...; done' &
    PING_LOOP_PID=$!
    "${@}" &> /tmp/build.log
    trap - ERR
    kill "${PING_LOOP_PID}"
    set -x
}

main() {
    local version=0.9.9

    local dependencies=(
        ca-certificates
        curl
        build-essential
    )

    apt-get update
    local purge_list=()
    for dep in "${dependencies[@]}"; do
        if ! dpkg -L "${dep}"; then
            apt-get install --assume-yes --no-install-recommends "${dep}"
            purge_list+=( "${dep}" )
        fi
    done

    local td
    td="$(mktemp -d)"

    pushd "${td}"
    curl --retry 3 -sSfL "https://github.com/richfelker/musl-cross-make/archive/v${version}.tar.gz" -O
    tar --strip-components=1 -xzf "v${version}.tar.gz"

    # Don't depend on the mirrors of sabotage linux that musl-cross-make uses.
    local linux_headers_site=https://ci-mirrors.rust-lang.org/rustc/sabotage-linux-tarballs

    hide_output make install "-j$(nproc)" \
        GCC_VER=9.2.0 \
        MUSL_VER=1.2.0 \
        BINUTILS_VER=2.33.1 \
        DL_CMD='curl --retry 3 -sSfL -C - -o' \
        LINUX_HEADERS_SITE=$linux_headers_site \
        OUTPUT=/usr/local/ \
        "${@}"

    if (( ${#purge_list[@]} )); then
      apt-get purge --assume-yes --auto-remove "${purge_list[@]}"
    fi

    popd

    rm -rf "${td}"
    rm "${0}"
}

main "${@}"
