ARG BASE_IMAGE=zhihanz/build-tool:latest

FROM $BASE_IMAGE AS planner
WORKDIR app
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json
RUN cp ./recipe.json /tmp/recipe.json

FROM $BASE_IMAGE AS cacher
WORKDIR app
COPY --from=planner /tmp/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
RUN mkdir -p /tmp/target
RUN cp -r ./target /tmp/target

FROM $BASE_IMAGE AS builder
WORKDIR app
COPY . .
# Copy over the cached dependencies
COPY --from=cacher /tmp/target target
COPY --from=cacher $CARGO_HOME $CARGO_HOME
RUN rustup component add rustfmt --toolchain nightly-2021-06-01-x86_64-unknown-linux-gnu
RUN make build
RUN cp ./target/release/fuse-query /tmp/fuse-query \
	&& mv ./target/release/fuse-store /tmp/fuse-store \
	&& mv ./docker/fusequery-docker.toml /tmp/fuse-query.toml \
	&& mv ./docker/bootstrap.sh /tmp/bootstrap.sh

FROM debian:buster
COPY --from=builder /tmp/fuse-query /fuse-query
COPY --from=builder /tmp/fuse-store /fuse-store
COPY --from=builder /tmp/fuse-query.toml /fuse-query.toml
COPY --from=builder /tmp/bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]
