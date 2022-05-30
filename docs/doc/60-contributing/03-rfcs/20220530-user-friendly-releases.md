---
title: User-friendly Releases
description: RFC for user-friendly releases
---

# Summary

A simple user-friendly release solution.

# Motivation

In the past, only specific binaries were included in the Databend release. Understanding how to operate and maintain Databend often requires extensive discussion.

This proposal aims to improve this process by providing some simple tips and default configs to help make the process painless for users.

# Detailed design

The release directory structure is roughly as follows:

```rust
.
├── bin
│   ├── databend-query
│   ├── databend-meta
│   └── databend-metactl (?)
├── configs
│   ├── databend-query.toml
│   └── databend-meta.toml
├── docs 
│   ├── basic-guide.md
│   ├── faq.md
│   └── mini-tutorial.md (?)
├── data/ (?)
├── scripts/ (?)
│   ├── bootstrap.sh
│   └── benchmark.sh
├── Makefile (?)
└── README.md

- `(?)` means optional.
```

## Required

- `README.md`, basic information and important tips, useful links.
- `bin`, of course, our binaries.
- `configs`, the basic configs, to prompt for configuration usage and start the Databend service.
- `docs`, helpful documents, frequently asked questions and basic user guide.

## Optional

- `databend-metactl`, perhaps we no longer need to release databend-tools.
- `docs/mini-tutorial.md`, like a worksheet, it will help to learn how to use Databend.
- `data`, if we have a `mini-tutorial` in release, here is a copy of the data.
- `scripts`, for easy installation and deployment.
- `Makefile`, one-line commands, `install`, `run` and more.

# Rationale and alternatives

## Why not use a directory structure closer to a package like `/etc`, `/usr` ?

Just for a quick glance, users can find the content they care about very intuitively.

It would also be easy to switch to a more package-like directory structure, but we don't have enough motivation to do so at the moment.

## What are other good practices?

The releases of most open source databases are no different with us. However, some good examples can still be found.

- [`clickhouse`](https://github.com/ClickHouse/ClickHouse/releases), package-like directory structure, includes configs and utilities, the documentation is mainly a collection of useful links.
- [`cayley`](https://github.com/cayleygraph/cayley/releases/), a notable highlight is the inclusion of data and queries to help users quickly explore linked data.
