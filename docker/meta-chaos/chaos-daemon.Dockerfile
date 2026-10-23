# Copyright 2020-2026 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

FROM curlimages/curl:7.88.1 AS download
USER root
RUN curl --fail --silent --show-error --location --retry 3 \
      https://github.com/chaos-mesh/toda/releases/download/v0.3.0/toda-linux-amd64.tar.gz \
      --output /tmp/toda.tar.gz && \
    echo '391001e13b32dfd2eb59999b6ebddac475057c8655dc106a652d7b8208f10efe  /tmp/toda.tar.gz' | sha256sum -c - && \
    mkdir /tmp/toda && tar -xzf /tmp/toda.tar.gz -C /tmp/toda

# The released toda binary needs glibc >= 2.34; the daemon uses Debian bullseye.
# Give only toda a newer runtime, leaving the daemon and other helpers unchanged.
FROM debian:bookworm-slim AS runtime

FROM ghcr.io/chaos-mesh/chaos-daemon:v2.6.3
COPY --from=download /tmp/toda/toda /opt/toda/toda
COPY --from=runtime /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 /opt/toda/lib/
COPY --from=runtime /lib/x86_64-linux-gnu/libc.so.6 /opt/toda/lib/
COPY --from=runtime /lib/x86_64-linux-gnu/libm.so.6 /opt/toda/lib/
COPY --from=runtime /lib/x86_64-linux-gnu/libgcc_s.so.1 /opt/toda/lib/
# toda 0.2.x stops its FUSE session on STATX, causing EEXIST and ENOTCONN.
# Keep the executable path expected by chaos-daemon and preserve its arguments.
RUN printf '%s\n' '#!/bin/sh' \
      'exec /opt/toda/lib/ld-linux-x86-64.so.2 --library-path /opt/toda/lib /opt/toda/toda "$@"' \
      > /usr/local/bin/toda && chmod 755 /usr/local/bin/toda
