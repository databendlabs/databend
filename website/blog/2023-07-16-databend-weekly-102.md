---
title: "This Week in Databend #102"
date: 2023-07-16
slug: 2023-07-16-databend-weekly
cover_url: 'weekly/weekly-102.jpg'
image: 'weekly/weekly-102.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: Dousir9
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
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

### Creating Bloom Indexes for Specified Columns

Creating bloom indexes consumes a significant amount of CPU resources. For wide tables, where only a few columns may require point queries or data ingestion performance is more important, creating bloom indexes for all columns may not be a good idea.

The `bloom_index_columns` option was introduced to the Databend [Fuse Engine](https://databend.rs/doc/sql-reference/table-engines/fuse), allowing you to specify which columns you want to create bloom indexes for.

To create a table with bloom indexes:

```SQL
CREATE TABLE table_name (
  column_name1 column_type1,
  column_name2 column_type2,
  ...
) ... bloom_index_columns='columnName1[, ...]'.
```

To create or modify bloom indexes for an existing table:

<small><i>After modifying the Bloom index options, Databend will not create indexes for existing data. The changes will only affect the subsequent data.</i></small>

```SQL
ALTER TABLE <db.table_name> SET OPTIONS(bloom_index_columns='columnName1[, ...]');
```

To disable the bloom indexing:

```SQL
ALTER TABLE <db.table_name> SET OPTIONS(bloom_index_columns='');
```

If you are interested in learning more, please check out the resources listed below.

- [PR #12048 | feat: support specify bloom index columns](https://github.com/datafuselabs/databend/pull/12048)

### Databend SQL Conformance

Databend aims to conform to the SQL standard, with particular support for ISO/IEC 9075:2011, also known as SQL:2011. Databend incorporates many features required by the SQL standard, often with slight differences in syntax or function.

We have summarized the level of conformity of Databend to the SQL:2011 standard, hoping it can help you further understand Databend's SQL Conformance.

If you are interested in learning more, please check out the resources listed below.

- [Docs | SQL Conformance](/doc/sql-reference/ansi-sql)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Understanding Databend Recluster Pipeline

A well-clustered table may become chaotic in some storage blocks negatively affecting the query performance. For example, the table continues to have DML operations (INSERT / UPDATE / DELETE).

The re-clustering operation does not cluster the table from the ground up. It selects and reorganizes the most chaotic existing storage blocks by calculating based on the clustering algorithm. 

The recluster pipeline is as follows:

```text
┌──────────┐     ┌───────────────┐     ┌─────────┐
│FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┐
└──────────┘     └───────────────┘     └─────────┘    │
┌──────────┐     ┌───────────────┐     ┌─────────┐    │     ┌──────────────┐     ┌─────────┐
│FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┤────►│MultiSortMerge├────►│Resize(N)├───┐
└──────────┘     └───────────────┘     └─────────┘    │     └──────────────┘     └─────────┘   │
┌──────────┐     ┌───────────────┐     ┌─────────┐    │                                        │
│FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┘                                        │
└──────────┘     └───────────────┘     └─────────┘                                             │
┌──────────────────────────────────────────────────────────────────────────────────────────────┘
│         ┌──────────────┐
│    ┌───►│SerializeBlock├───┐
│    │    └──────────────┘   │
│    │    ┌──────────────┐   │    ┌─────────┐    ┌────────────────┐     ┌─────────────────┐     ┌──────────┐
└───►│───►│SerializeBlock├───┤───►│Resize(1)├───►│SerializeSegment├────►│TableMutationAggr├────►│CommitSink│
     │    └──────────────┘   │    └─────────┘    └────────────────┘     └─────────────────┘     └──────────┘
     │    ┌──────────────┐   │
     └───►│SerializeBlock├───┘
```

If you are interested in learning more, please check out the resources listed below:

- [feat(storage): improve optimize and recluster](https://github.com/datafuselabs/databend/pull/11850)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for distributed COPY INTO. 
- Read document [Docs | ATTACH TABLE](https://databend.rs/doc/sql-commands/ddl/table/attach-table) to learn how to attach an existing table to another one.
- Read documents [Docs | Deepnote](/doc/integrations/deepnote) and [Docs | MindsDB](/doc/integrations/mindsdb) to learn how Databend can better collaborate with your data science projects.
- Read documents [Docs | Window Functions](/doc/sql-functions/window-functions/) and [Docs | Bitmap Functions](/doc/sql-functions/bitmap-functions/) to fully understand the BITMAP and window functions supported by Databend.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Accelerate CTE through Materialization

Inlining Common Table Expression (CTE) is a good idea, but if the CTE is particularly heavy, such as in TPCH Q15, the cost can be prohibitively expensive. In this case, it's better to introduce materialization for the expensive CTE.

```sql
--- TPCH Q15
WITH revenue AS
  (SELECT l_suppkey AS supplier_no,
          sum(l_extendedprice * (1 - l_discount)) AS total_revenue
   FROM lineitem
   WHERE l_shipdate >= TO_DATE ('1996-01-01')
     AND l_shipdate < TO_DATE ('1996-04-01')
   GROUP BY l_suppkey)
SELECT s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM supplier,
     revenue
WHERE s_suppkey = supplier_no
  AND total_revenue =
    (SELECT max(total_revenue)
     FROM revenue)
ORDER BY s_suppkey;
```

[Issue #12067 | Feature: speed up CTE by materialization](https://github.com/datafuselabs/databend/issues/12067)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.14-nightly...v1.2.25-nightly>
