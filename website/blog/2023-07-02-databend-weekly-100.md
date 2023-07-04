---
title: "This Week in Databend #100"
date: 2023-07-02
slug: 2023-07-02-databend-weekly
cover_url: 'weekly/weekly-100.png'
image: 'weekly/weekly-100.png'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: JackTan25
  - name: leiysky
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
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

> For security reasons, the Root user is no longer available out of the box. You must configure it before use. Learn more at <https://databend.rs/doc/sql-clients/admin-users> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Announcing Databend v1.2! Data + AI

Databend v1.2 was officially released on June 29, 2023! Thanks to all the community partners who participated and to everyone who contributed to making Databend better!


- New Data Type: `BITMAP`
- Direct Query of CSV/TSV/NDJSON Files Using Column Position
- New Hash Table: Improved Hash Join Performance
- AI Functions
- Computed Columns
- `VACUUM TABLE`
- Serverless Background Service
- Bind `databend` into Python
- BendSQL - Databend Native Command Line Tool
- Integration with Apache DolphinScheduler, Apache Flink CDC and Tableau

If you are interested in learning more, please check out the resources listed below.

- [What's Fresh in Databend v1.2 | Blog | Databend](/blog/databend-changelog-1-2)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Databend Long Run Tests

Databend's long run tests the correctness and performance of the system under heavy load and concurrency. This includes concurrent large-scale data ingestion, table maintenance (optimization, re-clustering, and vacuuming), as well as querying.

The test will run a series of SQL and validation commands to verify the results. It will begin by executing the pre-test scripts (`_before.sh`), followed by repeatedly running concurrent test scripts, and finally executing post-test scripts (`_after.sh`). All event logs will be stored in a table on Databend for further analysis.

Databend conducts long run tests to verify the correctness and performance of the system under heavy load and concurrency. These tests involve concurrent ingestion of large-scale data, table maintenance (optimization, re-clustering, and vacuuming), as well as querying.

During the testing process, a series of SQL commands and validation checks will be performed to ensure accurate results. The testing process will start by running pre-test scripts (`_before.sh`), followed by repeated execution of concurrent test scripts, and finally executing post-test scripts (`_after.sh`). All event logs will be stored in a Databend table for further analysis.

```lua
                      +-------------------+
                      |     Long Run      |
                      +-------------------+
                               |
                               |
                               v
                  +-----------------------+
                  |  Before Test Scripts  |
                  +-----------------------+
                               |
                               |
                               v
        +----------------------------------+
        |     Concurrent Test Scripts      |
        +----------------------------------+
        |              |                   |
        |              |                   |
        v              v                   v
+----------------+ +----------------+ +----------------+
|  Test Script 1 | |  Test Script 2 | |  Test Script 3 |
+----------------+ +----------------+ +----------------+
                               |
                               |
                               v
              +-----------------------+
              |   After Test Scripts  |
              +-----------------------+

```

If you are interested in learning more, please check out the resources listed below:

- [Databend Long Run Tests](https://github.com/datafuselabs/databend/tree/main/tests/longrun)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added more execution information in `system.query_profile`, which makes it easier than ever to profile your queries.
- Added the basic read support for Iceberg table.
- Added support for `ntile` window function.
- Added support for distributed copy into (first version).
- Read documents [Docs | Loading Data with Tools - Addax](https://databend.rs/doc/load-data/load-db/addax) and [Docs | Loading Data with Tools - DataX](https://databend.rs/doc/load-data/load-db/datax) to learn how to import data efficiently and conveniently.
- Read document [Docs | Working with Stages - Staging Files](https://databend.rs/doc/load-data/stage/stage-files) to learn how to use the Presigned URL method for uploading files to the stage.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Release Proposal: Nightly v1.3

Databend v1.3 is scheduled for release on August 1st and will primarily focus on enhancing stability.

| Task                                                                                         | Status      |
| -------------------------------------------------------------------------------------------- | ----------- |
| (Query) [JSON indexing#6994](https://github.com/datafuselabs/databend/issues/6994)           | IN PROGRESS |
| (Query+Storage) Create index feature                                                         | IN PROGRESS |
| (Query+Storage)[Distributed COPY#8594](https://github.com/datafuselabs/databend/issues/8594) | IN PROGRESS |
| (Query+Storage) Distributed REPLACE                                                          | PLAN        |
| [COPY returns more status](https://github.com/datafuselabs/databend/issues/7730)             | PLAN        |
| (Query+Storage) Query apache/iceberg                                                         | IN PROGRESS |
| (Processor) OrderBy Spill                                                                    | IN PROGRESS |
| (Stability) Fast update/delete with fuse engine                                              | IN PROGRESS |
| (Stability) Query profiling                                                                  | IN PROGRESS |
| (Test) Longrun framework:BendRun                                                             | IN PROGRESS |

[Issue #11868 | Release proposal: Nightly v1.3](https://github.com/datafuselabs/databend/issues/11868)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.72-nightly...v1.2.4-nightly>
