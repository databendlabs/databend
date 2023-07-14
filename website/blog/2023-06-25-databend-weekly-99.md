---
title: "This Week in Databend #99"
date: 2023-06-25
slug: 2023-06-25-databend-weekly
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: ariesdevil
  - name: b41sh
  - name: dantengsky
  - name: Dousir9
  - name: flaneur2020
  - name: JackTan25
  - name: leiysky
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: ZhiHanZ
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Flink CDC

Databend now supports Flink CDC (Change Data Capture), which allows you to capture and process real-time data changes from various sources using SQL-based queries. With Flink CDC, you can monitor and capture data modifications (inserts, updates, and deletes) happening in a database or streaming system and react to those changes in real-time.

Databend's Flink SQL connector offers a connector that integrates Flink's stream processing capabilities with Databend. By configuring this connector, you can capture data changes from various databases as streams and load them into Databend for processing and analysis in real-time.

If you are interested in learning more, please check out the resources listed below:

- [Docs | Loading Data with Tools - Flink CDC](https://databend.rs/doc/load-data/load-db/flink-cdc)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Register External Tables with Databend's Python Binding

Databend's Python binding now allows the registration of external tables. You can use the following functions to register external tables in Databend:

- `register_parquet`
- `register_ndjson`
- `register_csv`
- `register_tsv`

Here is an example of how to use the `register_parquet` function to register an external table named "ontime" in Databend:

```python
from databend import SessionContext

ctx = SessionContext()

ctx.register_parquet("ontime", "./ontime/", pattern = ".*.parquet")

df = ctx.sql("select * from ontime limit 10").collect()

print(df)
```

This code registers Parquet files located in the `./ontime/` directory with the pattern `.*.parquet` . You can then use the registered table name `ontime` in SQL queries to access the data.

If you are interested in learning more, please check out the resources listed below:

- [PR | feat(python): support register table functions](https://github.com/datafuselabs/databend/pull/11841)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for renaming columns with `ALTER TABLE [ IF EXISTS ] <name> RENAME COLUMN <col_name> TO <new_col_name>`.
- Added support for using column position when querying CSV and TSV files.
- Added `system.background_jobs` and `system.background_tasks` tables.
- Added http query deduplication via `X-DATABEND-DEDUPLICATE-LABEL` header.
- Added support for distributed deletion.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Implement Read-Only Mode for Databend Clusters

Databend plans to introduce a Read-Only Mode for clusters. This will provide users with more control over resource allocation.

Introducing Read-Only Mode allows separate clusters for read and write operations, preventing accidental writes in the read cluster, avoiding data loss, and enhancing performance.

[Issue #11836 | feat: read-only mode for Databend clusters](https://github.com/datafuselabs/databend/issues/11836)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.64-nightly...v1.1.72-nightly>
