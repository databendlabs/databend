---
title: Building Databend
sidebar_label: Building Databend
description:
  Getting and building Databend from Source
---

As an open-source platform, Databend provides the flexibility for users to modify, distribute and enhance the software according to their specific needs. Additionally, users have the freedom to build Databend from the source code, allowing them to fully understand how the software works and potentially contribute to its development.

:::tip
Databend offers a Docker image that includes all the necessary tools for development, but it's currently only available for the amd64 architecture. To use it, ensure that Docker is installed and running, and then run `INTERACTIVE=true scripts/setup/run_build_tool.sh`. This will launch an environment for building and testing, and the `INTERACTIVE=true` flag enables interactive mode.
:::

## Prerequisites

Before you build Databend, make sure the following requirements have been met:

- At least 16 GB of RAM will be required to build Databend from the source code.
- You have installed the following required tools:
  - Git
  - cmake
  - [rustup](https://rustup.rs/)

## Building Databend

Follow the steps below to build Databend:

1. Download the source code.

```shell
git clone https://github.com/datafuselabs/databend.git
```

2. Install dependencies and compile the source code.

```shell
cd databend
make setup -d
export PATH=$PATH:~/.cargo/bin
```

3. Build Databend.

  - To build Databend with debug information for debugging purposes, run `make build`. The resulting files will be located in the "target/debug/" directory.

```shell
make build 
```
  - To build Databend for production, optimized for your local CPU, run `make build-release`. The resulting files will be located in the "target/release/" directory.

```shell
make build-release
```

## Start Databend for Debugging

```shell
# 1. Start databend-meta first:
nohup target/debug/databend-meta --single --log-level=ERROR & 

# 2. Start databend-query and connect it to databend-meta:
nohup target/debug/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml &
```
:::tip
To stop databend-meta and databend-query, run the following commands:

```shell
killall -9 databend-meta
killall -9 databend-query
```
:::

## Common Errors

1. protoc failed: Unknown flag: --experimental_allow_proto3_optional\n

```bash
  --- stderr
  Error: Custom { kind: Other, error: "protoc failed: Unknown flag: --experimental_allow_proto3_optional\n" }
warning: build failed, waiting for other jobs to finish...
All done...
# Reduce binary size by compressing binaries.
objcopy --compress-debug-sections=zlib-gnu /home/aucker/mldb/databend/target/release/databend-query
objcopy: '/home/aucker/mldb/databend/target/release/databend-query': No such file
make: *** [Makefile:51: build-release] Error 1
```

The error message indicates that there is an issue with building Databend due to an unknown flag (--experimental_allow_proto3_optional) being used with protoc, which is a protocol buffer compiler. This flag is only available in protoc version 3.12 or higher, and the current version being used does not support it.

The recommended solution is to upgrade to a version of protoc that supports that flag. You can do this by downloading the latest version of protoc from the official release page (https://github.com/protocolbuffers/protobuf/releases) and installing it on your system.

```bash
PB_REL="https://github.com/protocolbuffers/protobuf/releases"
curl -LO $PB_REL/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
unzip protoc-3.15.8-linux-x86_64.zip
$sudo cp bin/protoc /usr/bin/
$protoc --version 
libprotoc 3.15.6
```

2. Couldn't connect to databend-meta in a forked Databend project.

This issue is likely caused by a version check implemented in Databend-meta that is not being met by the forked project. 

One possible solution is to fetch the latest tags from the official Databend repository using the command `git fetch https://github.com/datafuselabs/databend.git --tags`. This should ensure that the project is using the latest version of Databend-meta and will pass the version check.