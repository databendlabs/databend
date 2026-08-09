FROM debian:bookworm AS prepare

RUN apt-get update \
    && apt-get install -y --no-install-recommends binutils

COPY databend-query /databend-query
COPY databend-meta /databend-meta

RUN strip /databend-query /databend-meta

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=prepare /databend-query /databend-query
COPY --from=prepare /databend-meta /databend-meta

RUN chmod 755 /databend-query /databend-meta
