---
title: "This Week in Databend #108"
date: 2023-08-27
slug: 2023-08-27-databend-weekly
cover_url: 'weekly/weekly-108.jpg'
image: 'weekly/weekly-108.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: Chasen-Zhang
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: JackTan25
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
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

### Multiple Catalogs

Catalog is a fundamental organizational concept in Databend and assists in efficiently managing and accessing your data sources. 

Databend allows Multiple Catalogs and currently supports two types of external catalogs: Apache Iceberg and Apache Hive.

With external catalogs, there's no longer a need to load your external data into Databend before querying

```sql
-- Create a Hive catalog
CREATE CATALOG hive_ctl 
TYPE = HIVE 
CONNECTION =(
    METASTORE_ADDRESS = '127.0.0.1:9083' 
    URL = 's3://databend-toronto/' 
    AWS_KEY_ID = '<your_key_id>' 
    AWS_SECRET_KEY = '<your_secret_key>' 
);
```

If you are interested in learning more, please check out the resources listed below.

- [Docs | Catalog](https://databend.rs/doc/sql-commands/ddl/catalog/)
- [RFCs | Multiple Catalog](https://databend.rs/doc/contributing/rfcs/multiple-catalog)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Using `cargo-pgo` to Practice PGO

Profile-guided optimization (PGO) is a common compilation optimization technique that utilizes profiling information collected during program runtime to guide the compiler in making optimizations, thereby improving program performance.

According to the [tests](https://github.com/datafuselabs/databend/issues/9387#issuecomment-1566210063), PGO boosts Databend performance by up to 10%. The actual performance depends on your workloads. Try PGO for your Databend cluster.

If you are interested in learning more, please check out the resources listed below:

- [Docs | Profile Guided Optimization (PGO)](https://databend.rs/doc/contributing/pgo)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Introduced the `json_pretty` function.
- Improved performance of Inner Join.
- Added more metrics to the HTTP query interface.
- Implemented support for `SHOW DATABASES` and `SHOW TABLES` in Hive Catalog.
- Read [Blog | Revolutionizing Data Archival and Query Performance for Pharmaceutical Group](https://databend.rs/blog/2023-08-24-cdh) to check out how Databend played a pivotal role in transforming data archiving and query performance for a Pharmaceutical Group.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Using SQLsmith for Fuzz Testing

[SQLsmith](https://github.com/anse1/sqlsmith) focuses on generating random, type-aware and column-aware SQL queries that can often pass semantic checks, thereby further testing the execution logic of databases.

In the past, Databend added support for SQLancer and traditional fuzz testing with [LibAFL](https://github.com/AFLplusplus/LibAFL). The Databend team now plans to introduce SQLsmith for domain-aware fuzz testing, which will provide more comprehensive and targeted test results to increase the likelihood of discovering vulnerabilities.

[Issue #12576 | Feature: Using sqlsmith to support sql fuzzy testing](https://github.com/datafuselabs/databend/issues/12576)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.74-nightly...v1.2.87-nightly>
