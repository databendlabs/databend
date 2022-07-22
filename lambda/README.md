# Databend Lambda

This is a demo to run databend inside lambda.

## Get Started

### Environment Setup

- Install `cargo-lambda`: `cargo install cargo-lambda`
- Install `zig`: https://github.com/ziglang/zig/wiki/Install-Zig-from-a-Package-Manager

`cargo-lambda` will use `cargo-zigbuild` to handle cross compile.

### Start build

- To make our binary works on aws runtime, we need to build as musl.
- Our binary is too large that debug build can't be uploaded to lambda
- Package as zip so that we can upload to lambda directly

```shell
cargo lambda build --release --output-format zip --target x86_64-unknown-linux-musl
```

### Deploy

Upload packaged zip to lambda, and create a `Function URL` via `Configuration` -> `Function URL`.

Setup environments to make `databend-query` work as expected via `Configuration` -> `Environment variables`:

- `LOG_DIR=/tmp/log`
