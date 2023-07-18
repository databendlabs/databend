---
title: "This Week in Databend #98"
date: 2023-06-18
slug: 2023-06-18-databend-weekly
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: andylokandy
  - name: b41sh
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: JackTan25
  - name: jonahgao
  - name: leiysky
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: Xuanwo
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

### Background Service

Databend Internal Storage FuseTable is similar to Apache Iceberg, a Log Structured Table that requires regular table compaction, re-clustering, and vacuuming to merge small data chunks. The process involves sorting the data by the cluster key or vacuuming unneeded branches.

Previously, different drivers were used for these implementations which added complexity to the infrastructure. Additional services had to be deployed and maintained to trigger driver events. To simplify this process, we have introduced a Background Service that allows Databend to run as a background one-shot job or daemon for running cron jobs. These jobs can trigger table maintenance tasks such as automatic compaction/vacuum/reclustering based on criteria without additional maintenance needed.

This implementation includes:

1. Complete metasrv schema definition and background_job and background_tasks.
2. APIs for updating and maintaining background_job and background_task state on meta-service.
3. Simplified job scheduler implementation which support `one_shot`, `interval`, `cron` job type.

`Background Service` requires **Enterprise Edition**. To inquire about upgrading, please contact [Databend Support](https://www.databend.com/contact-us).

If you are interested in learning more, please check out the resources listed below:

- [PR | feat: implement Background service for databend](https://github.com/datafuselabs/databend/pull/11751)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### IceLake - Pure Rust Iceberg Implementation 

[Iceberg](https://iceberg.apache.org/), an open table format for analytics, lacks a mature Rust binding, making integration with databases like Databend difficult. IceLake aims to fill this gap and build an open ecosystem that:

- Users can read/write iceberg tables from **ANY** storage services like s3, gcs, azblob, hdfs and so on.
- **ANY** Databases can integrate with `icelake` to facilitate reading and writing of iceberg tables.
- Provides **NATIVE** support transmute between `arrow`s.
- Provides bindings so that other languages can work with iceberg tables powered by Rust core.

If you are interested in learning more, please check out the resources listed below:

- [GitHub - icelake-io/icelake](https://github.com/icelake-io/icelake/)
- [PR | feat: Integrate with icelake for iceberg support](https://github.com/datafuselabs/databend/pull/11785)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for MERGE JOIN.
- Add support for column position to CSV format.
- Read *[Docs | Computed Columns](https://databend.rs/doc/sql-commands/ddl/table/ddl-create-table#computed-columns)* to understand how to use computed columns and the trade-offs when choosing which type to adopt.
- Read *[Docs | Subquery-Based Deletions](https://databend.rs/doc/sql-commands/dml/dml-delete-from)* to learn how to use subquery operators and comparison operators to achieve the desired deletion.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Support `VALIDATION_MODE` for **COPY INTO**

We hope to support the `VALIDATION_MODE` for Databend's `COPY INTO` statement, which can be used to validate the data that needs to be loaded (but not actually loaded) and return results based on specified validation options.

- `RETURN_ERRORS`: This mode validates the data and returns all errors.
- `RETURN_<number>_ROWS`: This mode validates `<number>` rows of data. If there are no errors, it returns the loaded information. If there are any errors encountered, it will throw errors.

[Issue #11582 | Feature: copy support VALIDATION_MODE](https://github.com/datafuselabs/databend/issues/11582)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@jonahgao](https://github.com/jonahgao) made their first contribution in [#11718](https://github.com/datafuselabs/databend/pull/11718). Fixed column types of MySQLClient.
* [@akoshchiy](https://github.com/akoshchiy) made their first contribution in [#11783](https://github.com/datafuselabs/databend/pull/11783). Updated `MACOSX_DEPLOYMENT_TARGET` value.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.56-nightly...v1.1.64-nightly>
