#!/bin/bash

set -e

ARCH=x86_64
glibc=$(getconf GNU_LIBC_VERSION | awk '{print $2}')
TARGET=${ARCH}-linux-glib$glibc
MUSL_RUST_TARGET=${ARCH}-unknown-linux-musl
BENSQL_VERSION=0.26.1

RUSTFLAGS="-C target-feature=+sse4.2 -C link-arg=-Wl,--compress-debug-sections=zlib"

echo "==> https_proxy=${https_proxy}"

mkdir -p distro
rm -rf distro/{bin,configs,systemd,scripts}
mkdir -p distro/{pkg,bin,configs,systemd,scripts}

BENSQL_PKG=bendsql-${MUSL_RUST_TARGET}.tar.gz

if [ ! -f distro/pkg/${BENSQL_PKG} ]; then
	echo "==> downloading ${BENSQL_PKG} ..."
	curl -L https://github.com/databendlabs/bendsql/releases/download/v${BENSQL_VERSION}/${BENSQL_PKG} -o distro/pkg/${BENSQL_PKG}
fi

tar -xzvf distro/pkg/${BENSQL_PKG} -C distro/bin
distro/bin/bendsql --version

rustup show

DATABEND_VERSION=$(git describe --tags --abbrev=0)
PKG=databend-${DATABEND_VERSION}-${TARGET}.tar.gz

echo "==> building databend ${DATABEND_VERSION} for ${TARGET} ..."
#cargo clean

cargo build --release \
	--bin=databend-query \
	--bin=databend-meta \
	--bin=databend-metactl \
	--bin=databend-metabench

#  --bin=table-meta-inspector

QUERY_BIN="./target/release/databend-query"
readelf -p .comment ${QUERY_BIN}
ldd ${QUERY_BIN}
${QUERY_BIN} --version

mv ./target/release/databend-query distro/bin/
mv ./target/release/databend-meta distro/bin/
mv ./target/release/databend-metactl distro/bin/
mv ./target/release/databend-metabench distro/bin/
cp ./scripts/distribution/systemd/databend-* distro/systemd/
cp ./scripts/distribution/configs/databend-* distro/configs/
cp ./scripts/distribution/release-readme.txt distro/readme.txt
#cp -r ./scripts/distribution/local-scripts/* distro/scripts/
cp -r ./scripts/distribution/package-scripts/* distro/scripts/

cd distro
tar -czvf ${PKG} bin configs systemd scripts readme.txt
ls -lh ${PKG}
