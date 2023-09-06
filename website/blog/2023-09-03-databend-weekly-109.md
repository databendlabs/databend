---
title: "This Week in Databend #109"
date: 2023-09-03
slug: 2023-09-03-databend-weekly
cover_url: 'weekly/weekly-109.jpg'
image: 'weekly/weekly-109.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: flaneur2020
  - name: JackTan25
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
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

### Understanding Cluster Key

By defining a Cluster Key, you can enhance query performance by clustering tables. In this case, data will be organized and grouped based on the Cluster Key, rather than relying solely on the order in which data is ingested. This optimizes the data retrieval logic for large tables and accelerates queries.

When using the `COPY INTO` or `REPLACE INTO` command to write data into a table that includes a cluster key, Databend will automatically initiate a re-clustering process, as well as a segment and block compact process.

Clustering or re-clustering a table takes time. Databend suggests defining cluster keys primarily for sizable tables that experience slow query performance.

If you are interested in learning more, please check out the resources listed below.

- [Docs | Understanding Cluster Key](https://databend.rs/doc/sql-commands/ddl/clusterkey/)
- [Docs | Databend Data Storage: Snapshot, Segment, and Block](https://databend.rs/doc/sql-commands/ddl/clusterkey/)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Exploring Databend Local Mode

**Databend Local Mode** aims to provide a simple Databend environment where users can directly interact with Databend using SQL without the need to perform a full Databend deployment. This makes it convenient for developers to perform basic data processing using Databend.

`databend-query local` starts a temporary databend-query process which functions as both client and server.The storage is located in a temporary directory and its lifecycle follows the process. Once the process ends, the resources are also destroyed. You can start multiple local processes on one server, and their resources are isolated from each other.

```sql!
❯ alias databend-local="databend-query local"
❯ echo " select sum(a) from range(1, 100000) as t(a)" | databend-local
4999950000

❯ databend-local

databend-local:) select number %3 n, number %4 m, sum(number) from numbers(1000000000) group by n,m limit 3 ;

┌───────────────────────────────────┐
│   n   │   m   │    sum(number)    │
│ UInt8 │ UInt8 │    UInt64 NULL    │
├───────┼───────┼───────────────────┤
│     0 │     0 │ 41666666833333332 │
│     1 │     0 │ 41666666166666668 │
│     2 │     0 │ 41666666500000000 │
└───────────────────────────────────┘
0 row result in 1.669 sec. Processed 1 billion rows, 953.67 MiB (599.02 million rows/s, 4.46 GiB/s)
```

If you need to use Databend in a production environment, we recommend deploying a Databend cluster according to the official documentation or using Databend Cloud. databend-local is a good choice for developers to quickly leverage Databend's capabilities without the need to deploy a full Databend instance.

If you are interested in learning more, please check out the resources listed below:

- [PR | chore(query): improve databend local](https://github.com/datafuselabs/databend/pull/12659)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added initial support for `MERGE INTO`.
- Implemented SQLsmith testing framework to support more accurate fuzzy testing.
- Read the document [Docs | Setting Environment Variables](https://databend.rs/doc/deploy/node-config/environment-variables) for how to change Databend configurations through environment variables.
- Implemented `json_strip_nulls` and `json_typeof` functions. You can also read [Docs | Semi-Structured Functions](https://databend.rs/doc/reference/functions/variant-functions) to check out Databend functions for semi-structured data processing.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Optimizing the implementation of MERGE INTO

In [PR #12350 | feat: support Merge-Into V1](https://github.com/datafuselabs/databend/pull/12350), Databend has initially supported the `MERGE INTO` syntax.

![](/img/blog/merge-into.png)

Based on this, there are many optimizations worth considering, such as providing parallel and distributed implementations, reducing I/O, and simplifying data block splitting.

[Issue #12595 | Feature: Merge Into Optimizations](https://github.com/datafuselabs/databend/issues/12595)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.87-nightly...v1.2.96-nightly>
