---
title: "This Week in Databend #105"
date: 2023-08-06
slug: 2023-08-06-databend-weekly
cover_url: 'weekly/weekly-105.png'
image: 'weekly/weekly-105.png'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: Chasen-Zhang
  - name: dantengsky
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: lichuang
  - name: nange
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Loading Data with Debezium

[Debezium](https://debezium.io/) is a set of distributed services to capture changes in your databases. [debezium-server-databend](https://github.com/databendcloud/debezium-server-databend) is a lightweight CDC tool developed by Databend, based on Debezium Engine.

This tool provides a simple way to monitor and capture database changes, transforming them into consumable events without the need for large data infrastructures like Flink, Kafka, or Spark.

If you are interested in learning more, please check out the resources listed below.

- [Docs | Loading Data with Tools - Debezium](https://databend.rs/doc/load-data/load-db/debezium)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Getting started with Iceberg using Databend

[Apache Iceberg](https://iceberg.apache.org/) is a high-performance open table format designed for large-scale analytics workloads. It is known for its simplicity and reliability.

With Databend's Multiple Catalog capability and [IceLake](https://github.com/icelake-io/icelake)'s Iceberg Rust implementation, Databend now supports mounting and analyzing data stored in Iceberg table format in the form of a Catalog.

```sql
CREATE CATALOG iceberg_ctl
TYPE=ICEBERG
CONNECTION=(
    URL='s3://warehouse/path/to/db'
    AWS_KEY_ID='admin'
    AWS_SECRET_KEY='password'
    ENDPOINT_URL='your-endpoint-url'
);
```

If you are interested in learning more, please check out the resources listed below:

- [Blog | Feature Preview: Iceberg Integration with Databend](https://databend.rs/blog/2023-08-01-iceberg-integration)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added Transport Layer Security (TLS) support for MySQL handler.
- Added Geo functions: `h3_to_string`, `string_to_h3`, `h3_is_res_class_iii`, `h3_is_pentagon`, `h3_get_faces`, `h3_cell_area_m2`, and `h3_cell_area_rads2`.
- Read document [Docs | Network Policy](https://databend.rs/doc/sql-commands/ddl/network-policy/) to learn how to manage network policies in Databend.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Native `async fn` in Trait

Since the MVP of native [async-fn-in-trait](https://github.com/rust-lang/rust/issues/91611) in Rust was launched in November 2022, it has been available for testing on Rust nightly channel. It may be time to evaluate the status of this feature and consider using it instead of `async_trait`.

```rust
#![feature(async_fn_in_trait)]

trait Database {
    async fn fetch_data(&self) -> String;
}

impl Database for MyDb {
    async fn fetch_data(&self) -> String { ... }
}
```

[Issue #12201 | Refactor: use native async fn in trait syntax](https://github.com/datafuselabs/databend/issues/12201)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.42-nightly...v1.2.51-nightly>
