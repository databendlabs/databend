---
title: "This Week in Databend #113"
date: 2023-10-01
slug: 2023-10-01-databend-weekly
cover_url: 'weekly/weekly-113.jpg'
image: 'weekly/weekly-113.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTang
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zenus
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

### Loading to Table with Extra Columns

By default, COPY INTO loads data into a table by matching the order of fields in the file to the corresponding columns in the table. It's essential to ensure that the data aligns correctly between the file and the table.

![load extra](https://databend.rs/assets/images/load-extra-0b76668838a386aace5463d65547a40f.png)

If your table has more columns than the file, you can specify the columns into which you want to load data.

When working with CSV format, if your table has more columns than the file and the additional columns are at the end of the table, you can load data using the `FILE_FORMAT` option `ERROR_ON_COLUMN_COUNT_MISMATCH`.

If you are interested in learning more, please check out the resources below:

- [Docs | Example 5: Loading to Table with Extra Columns](https://databend.rs/doc/sql-commands/dml/dml-copy-into-table#example-5-loading-to-table-with-extra-columns)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Introducing Read Policies to Parquet Reader

There is a drawback of using the `arrow-rs` APIs: When we try to prefetch data for prewhere and topk push downs, we can't reuse the deserialized blocks.

In order to improve the logic of row group reading and reuse the prefetched data blocks at the final output stage, we have done a lot of refactoring and introduced some read policies.

**NoPrefetchPoliy** 

No prefetch stage at all. Databend reads, deserializes, and outputs data blocks you need directly.

**PredicateAndTopkPolicy**

Databend prefetches columns needed by prewhere and topk at the prefetch stage. It deserializes them into a DataBlock and evaluates them into RowSelection. It then slices the DataBlock by batch size and stores the resulting VecDeque in memory.

Databend reads the remaining columns specified by RowSelection at the final stage, and it outputs DataBlocks in batches. Then, it merges the prefetched blocks and projects the resulting blocks according to the output_schema.

**TopkOnlyPolicy**

It's similar to the `PredicateAndTopkPolicy`, but Databend only evaluates the topk at the prefech stage.

If you are interested in learning more, please check out the resources below:

- [PR #13020 | refactor: introduce ReadPolicy to parquet reader](https://github.com/datafuselabs/databend/pull/13020)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added spill info to query log.
- Added support for unloading data into a compressed file with COPY INTO.
- Introduced the `GET /v1/background/:tenant/background_tasks` HTTP API for querying background tasks.
- Read [Example 4: Filtering Files with Pattern](https://databend.rs/doc/sql-commands/dml/dml-copy-into-table#example-4-filtering-files-with-pattern) to understand how to use Pattern to filter files.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Fixing issues detected by SQLsmith

In the last month, SQLsmith has discovered around 40 bugs in Databend. Databend Labs is actively working to fix these issues and improve system stability, even in uncommon scenarios. Your involvement in this effort, which may include tasks like type conversion or handling special values, is encouraged and can be facilitated by referring to past fixes.


[Issues | Found by SQLsmith](https://github.com/datafuselabs/databend/issues?q=is%3Aissue+is%3Aopen+label%3Afound-by-sqlsmith)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@zenus](https://github.com/zenus) fixed an issue where schema mismatch was not detected during the execution of `COPY INTO` in [#13010](https://github.com/datafuselabs/databend/pull/13010).

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.128-nightly...v1.2.137-nightly>
