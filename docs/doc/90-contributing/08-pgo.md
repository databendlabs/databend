---
title: Profile Guided Optimization (PGO)
sidebar_label: Profile Guided Optimization (PGO)
description:
  Building an optimized Databend with Profile Guided Optimization.
---

Profile-guided Optimization is a compiler optimization technique that involves collecting typical execution data, including possible branches, during program execution. This collected data is then used to optimize various aspects of the code, such as inlining, conditional branches, machine code layout, and register allocation.

According to the [tests](https://github.com/datafuselabs/databend/issues/9387#issuecomment-1566210063), PGO boosts Databend performance by up to 10%. The performance benefits depend on your typical workload - you can get better or worse results.

More information about PGO in Databend you can read in [Issue #9387 | Feature: add PGO support](https://github.com/datafuselabs/databend/issues/9387).

## Prerequisites

Before you build Databend with PGO, make sure the following requirements have been met:

- Install the PGO helper binary by adding the `llvm-tools-preview` component to your toolchain with rustup:

```bash
$ rustup component add llvm-tools-preview
```

- Install [`cargo-pgo`](https://crates.io/crates/cargo-pgo) that makes it easier to use [PGO](https://doc.rust-lang.org/rustc/profile-guided-optimization.html) to optimize Rust binaries.

```bash
$ cargo install cargo-pgo
```

## Building Databend with PGO

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

3. Build Databend with `cargo-pgo`. Due to a [known issue](https://github.com/PyO3/pyo3/issues/1084) with PyO3, we need to skip `bendpy` during the build.

```shell
cargo pgo build -- --workspace --exclude bendpy
```

4. Import the dataset and run a typical query workload. 

```shell
# Run Databend in standalone mode, or you can try running it in cluster mode.
BUILD_PROFILE=<target-tuple>/release ./scripts/ci/deploy/databend-query-standalone.sh
# Here Databend's SQL logic tests is used as a demonstration, but you need to adjust it for the target workload
ulimit -n 10000;ulimit -s 16384; cargo run -p databend-qllogictest --release -- --enable_sandbox --parallel 16 --no-fail-fast
# Stop the running databend instance to dump profile data.
killall databend-query
killall databend-meta
```

:::tip
- You need to check the platform triple corresponding to your current build and replace `<target-tuple>` above. For example: `x86_64-unknown-linux-gnu`.
- For more precise profiles, run it with the following environment variable: `LLVM_PROFILE_FILE=./target/pgo-profiles/%m_%p.profraw`.
:::

5. Build an optimized Databend

```shell
cargo pgo optimize -- --workspace --exclude bendpy
```
