FROM rust:1.48.0-buster AS builder

COPY ./ /app
WORKDIR /app
RUN make setup
RUN make build

FROM python:3
COPY --from=builder /app/target/release/fuse-benchmark /fuse-benchmark
COPY ./tests/perfs/perfs.py /perfs.py
COPY ./tests/perfs/perfs.yaml /perfs.yaml
RUN pip install --no-cache-dir pyyaml
ENV BIN_LOCATION "/fuse-benchmark"
ENV SERVER_HOST "127.0.0.1"
ENV SERVER_PORT "9090"
ENV ITERATION "3"
ENV CONCURRENCY "1"
CMD ["python", "./perfs.py", "-b", "${BIN_LOCATION}", "--host", "${SERVER_HOST}", "-p", "${SERVER_PORT}", "-c", "${CONCURRENCY}}", "-i", "${ITERATION}"]