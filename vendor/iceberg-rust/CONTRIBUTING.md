<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Contributing

First, thank you for contributing to Iceberg Rust! The goal of this document is to provide everything you need to start contributing to iceberg-rust. The following TOC is sorted progressively, starting with the basics and expanding into more specifics.

## Your First Contribution

1. [Fork the iceberg-rust repository](https://github.com/apache/iceberg-rust/fork) into your own GitHub account.
1. [Create a new Git branch](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-and-deleting-branches-within-your-repository).
1. Make your changes.
1. [Submit the branch as a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to the main iceberg-rust repo. An iceberg-rust team member should comment and/or review your pull request within a few days. Although, depending on the circumstances, it may take longer.

## Workflow

### Git Branches

*All* changes must be made in a branch and submitted as [pull requests](#github-pull-requests). iceberg-rust does not adopt any type of branch naming style, but please use something descriptive of your changes.

### GitHub Pull Requests

Once your changes are ready you must submit your branch as a [pull request](https://github.com/apache/iceberg-rust/pulls).

#### Title

The pull request title must follow the format outlined in the [conventional commits spec](https://www.conventionalcommits.org). [Conventional commits](https://www.conventionalcommits.org) is a standardized format for commit messages. iceberg-rust only requires this format for commits on the `main` branch. And because iceberg-rust squashes commits before merging branches, this means that only the pull request title must conform to this format.

The following are all good examples of pull request titles:

```text
feat(schema): Add last_updated_ms in schema
docs: add hdfs classpath related troubleshoot
ci: Mark job as skipped if owner is not apache
fix(schema): Ignore prefix if it's empty
refactor: Polish the implementation of read parquet
```

#### Reviews & Approvals

All pull requests should be reviewed by at least one iceberg-rust committer.

#### Merge Style

All pull requests are squash merged. We generally discourage large pull requests that are over 300-500 lines of diff. If you would like to propose a change that is larger we suggest coming onto [Iceberg's DEV mailing list](mailto:dev@iceberg.apache.org) or [Slack #rust Channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A) and discuss it with us. This way we can talk through the solution and discuss if a change that large is even needed! This will produce a quicker response to the change and likely produce code that aligns better with our process.

When a pull request is under review, please avoid using force push as it makes it difficult for reviewer to track changes. If you need to keep the branch up to date with the main branch, consider using `git merge` instead. 

### CI

Currently, iceberg-rust uses GitHub Actions to run tests. The workflows are defined in `.github/workflows`.

## Setup

For small or first-time contributions, we recommend the dev container method. Prefer to do it yourself? That's fine too!

### Using a dev container environment

iceberg-rust provides a pre-configured [dev container](https://containers.dev/) that could be used in [Github Codespaces](https://github.com/features/codespaces), [VSCode](https://code.visualstudio.com/), [JetBrains](https://www.jetbrains.com/remote-development/gateway/), [JupyterLab](https://jupyterlab.readthedocs.io/en/stable/). Please pick up your favourite runtime environment.

The fastest way is:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/apache/iceberg-rust?quickstart=1&machine=standardLinux32gb)

### Bring your own toolbox

#### Install rust

iceberg-rust is primarily a Rust project. To build iceberg-rust, you will need to set up Rust development first. We highly recommend using [rustup](https://rustup.rs/) for the setup process.

For Linux or MacOS, use the following command:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

For Windows, download `rustup-init.exe` from [here](https://win.rustup.rs/x86_64) instead.

Rustup will read iceberg-rust's `rust-toolchain.toml` and set up everything else automatically. To ensure that everything works correctly, run `cargo version` under iceberg-rust's root directory:

```shell
$ cargo version
cargo 1.69.0 (6e9a83356 2023-04-12)
```

#### Install Docker or Podman

Currently, iceberg-rust uses Docker to set up environment for integration tests. Native Docker has some limitations, please check (https://github.com/apache/iceberg-rust/pull/748). Please use Orbstack or Podman.

For MacOS users, you can install [OrbStack as a docker alternative](website/src/reference/orbstack.md).
For Podman users, refer to [Using Podman instead of Docker](website/src/reference/podman.md)

## Build

* To compile the project: `make build`
* To check code styles: `make check`
* To run unit tests only: `make unit-test`
* To run all tests: `make test`

## Dependencies

`Cargo.lock` is committed, and regularly updated by dependabot to make sure the latest dependency versions are
tested in CI and developers have reproducible builds.

In `Cargo.toml`, we specify the minimum version required to use iceberg-rust. This allows users to choose their
dependency versions without always upgrading to the latest.

## Code of Conduct

We expect all community members to follow our [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
