---
title: Databend routine maintenance
---

## What is Databend routine maintenance?

Routine maintenance of Databend is currently performed roughly every 30 days. This is usually done at the beginning of the month.

The main tasks are to **update the toolchain** and **upgrade the dependencies**.

## Update the toolchain

Databend will choose a relatively new nightly version for the build/test/bench.

The steps to update toolchain are as follows:

- Step 1: edit `rust-toolchain.toml` to update the `channel` field.
  - Usually updated to the current date.
- Step 2: Run `make lint` to make sure clippy is happy.
  - clippy is not entirely correct. If necessary, you can use `#[allow(clippy::xxx)]` to skip some of the rules.
  - If possible, please add a comment to clarify.
- Step 3: Run `make test` to check the correctness.

## Upgrade the dependencies

Databend involves nearly a thousand third-party dependencies from sources such as [github](https://github.com/) and [crates.io](https://crates.io/).

Currently, the dependency upgrade process is divided into the following steps:

- Step 1: Run `cargo upgrade --workspace` to upgrade dependencies in your Cargo.toml to their latest versions.
  - [cargo-edit](https://crates.io/crates/cargo-edit) crate needs to be installed.
- Step 2: Check the dependencies from github and update the value of the `rev` field if it can be upgraded.
  - Please avoid introducing `version` and `branch` fields.
- Step 3: Update dependencies that need to be updated individually.
  - If upgrading this dependency requires a large scope of changes, please update it separately at the end of routine maintenance.
- Step 4: Run `make lint` and `make test` to make sure the update works.
  - If you determine that a dependency needs to be fixed separately, restore that dependency to the previous version. Redo this step.
  - Please make sure all check passed after each step.

:::note Notes
If you hacked `Cargo.lock` during the process, make sure everything is working smoothly.
:::

