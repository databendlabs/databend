---
title: Getting and Building Databend From Source
sidebar_label: Building From Source
description:
  Getting and Building Databend From Source
---

:::note

Note that at least 16GB of RAM is required to build from source and run tests.

:::

### Install prerequisites

Databend is written in Rust, to build Databend from scratch you will need to install the following tools:
* **Git**
* **Rust** Install with [rustup](https://rustup.rs/)

### Get the Databend code

```shell
git clone https://github.com/datafuselabs/databend
cd databend
```
### Run make

```shell
make build-release
```

`databend-query` and `databend-meta` will be placed in the `target/release` directory.


### Build with Docker Image

We provide a docker image with full development requirements, currently only on amd64.

Making sure docker is installed and running, just run `INTERACTIVE=true scripts/setup/run_build_tool.sh` to get into the build/test environment.
