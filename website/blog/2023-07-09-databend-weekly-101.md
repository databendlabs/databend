---
title: "This Week in Databend #101"
date: 2023-07-09
slug: 2023-07-09-databend-weekly
cover_url: 'weekly/weekly-101.jpg'
image: 'weekly/weekly-101.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: dacozai
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: JackTan25
  - name: leiysky
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: ZhiHanZ
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Tutorial | Create External Stage with AWS IAM User

AWS Identity and Access Management (IAM) allows you to control access to AWS services and resources by specifying authorized users or entities. It also enables centralized management of detailed permissions, as well as analysis of access patterns for refining permissions across the entire AWS platform.

Databend supports creating External Stage using AWS IAM User, making access to AWS S3 object storage more secure and reliable.

If you are interested in learning more, please check out the resources listed below.

- [Docs | Create External Stage with AWS IAM User](/doc/sql-commands/ddl/stage/ddl-create-stage#example-3-create-external-stage-with-aws-iam-user)

### Tutorial | Create External Stage on Cloudflare R2

[Cloudflare R2](https://www.cloudflare.com/products/r2/) gives you the freedom to create the multi-cloud architectures you desire with an S3-compatible object storage.

You can use Databend to create external storage on Cloudflare R2 and further efficiently mine the value of data.

If you are interested in learning more, please check out the resources listed below.

- [Docs | Create External Stage on Cloudflare R2](/doc/sql-commands/ddl/stage/ddl-create-stage#example-4-create-external-stage-on-cloudflare-r2)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### New System Table: `system.backtrace`

[async-backtrace](https://github.com/tokio-rs/async-backtrace) provides efficient and logical "stack" traces for asynchronous functions.

Databend has built the ability of `async-backtrace` into the system table. Developers can obtain trace information by querying the `system.backtrace` system table through `SELECT` statement, which will help further debugging and troubleshooting.

Furthermore, if you are in cluster mode, executing a query against `system.backtrace` on any node will return the call stack of the entire cluster.

If you are interested in learning more, please check out the resources listed below:

- [feat: add system.backtrace table](https://github.com/datafuselabs/databend/pull/12024)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added `bitmap_not_count`, `bitmap_union` and `bitmap_intersect` aggregate functions.
- Added `cume_dist` window function.
- Added support for `ATTACH TABLE`.
- Added `system.metrics` table and metrics for measuring spill and transformation operations.
- Added support for converting stored computed column into a regular column.
- Implemented task suggestion for **Serverless Background Service**.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Add Random Seed for Random Engine

Databend previously implemented a random engine that supports generating random tables using random data. In order to obtain deterministic and reproducible test results, we plan to introduce random seed.

```SQL
CREATE table(t int, time timestamp) ENGINE=RANDOM(1000)
```

[Issue #11863 | Feature: Support to add random seed on random engine](https://github.com/datafuselabs/databend/issues/11863)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@dacozai](https://github.com/dacozai) made their first contribution in [#11956](https://github.com/datafuselabs/databend/pull/11956). Updated dsn in develop/Rust docs.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.4-nightly...v1.2.14-nightly>
