---
title: "This Week in Databend #115"
date: 2023-10-15
slug: 2023-10-15-databend-weekly
cover_url: 'weekly/weekly-115.jpg'
image: 'weekly/weekly-115.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTang
  - name: Chasen-Zhang
  - name: ct20000901
  - name: dantengsky
  - name: dependabot[bot]
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
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

## What's New

Stay informed about the latest features of Databend.

### AGGREGATING INDEX

Databend has recently introduced **AGGREGATING INDEX** to improve query performance, especially for aggregation queries involving `MIN`, `MAX`, and `SUM`. Aggregating Index leverage techniques like precomputing and storing query results separately to eliminate the need to scan the entire table, thus speeding up data retrieval.

In addition, this feature includes a refresh mechanism that allows you to update and persist the latest query results on demand, ensuring data accuracy and reliability by refreshing the results when needed. Databend recommends manually refreshing the aggregating index before executing relevant queries to retrieve the most up-to-date data; Databend Cloud supports auto-refreshing of aggregating index.

```sql
-- Create an aggregating index
CREATE AGGREGATING INDEX my_agg_index AS SELECT MIN(a), MAX(c) FROM agg;

-- Refresh the aggregating index
REFRESH AGGREGATING INDEX my_agg_index;
```

The **AGGREGATING INDEX** requires Databend Enterprise Edition. Please [contact the Databend team](https://www.databend.com/contact-us) for upgrade information.

If you are interested in learning more, please check out the resources below:

- [Docs | AGGREGATING INDEX](https://databend.rs/doc/sql-commands/ddl/aggregating-index/)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Visualizing the MERGE INTO Pipeline

Databend recently implemented the `MERGE INTO` statement to provide more comprehensive data management capabilities. For those interested in how it works under the hood, check out the pipeline visualization of `MERGE INTO` below.

```
                                                                                                                                               +-------------------+
                                                                                        +-----------------------------+    output_port_row_id  |                   |
                                           +-----------------------+     Matched        |                             +------------------------>-ResizeProcessor(1)+---------------+
                                           |                       +---+--------------->|    MatchedSplitProcessor    |                        |                   |               |
                                           |                       |   |                |                             +----------+             +-------------------+               |
        +----------------------+           |                       +---+                +-----------------------------+          |                                                 |
        |   MergeIntoSource    +---------->|MergeIntoSplitProcessor|                                                       output_port_updated                                     |
        +----------------------+           |                       +---+                +-----------------------------+          |             +-------------------+               |
                                           |                       |   | NotMatched     |                             |          |             |                   |               |
                                           |                       +---+--------------->| MergeIntoNotMatchedProcessor+----------+------------->-ResizeProcessor(1)+-----------+   |
                                           +-----------------------+                    |                             |                        |                   |           |   |
                                                                                        +-----------------------------+                        +-------------------+           |   |
                                                                                                                                                                               |   |
                                                                                                                                                                               |   |
                                                                                                                                                                               |   |
                                                                                                                                                                               |   |
                                                                                                                                                                               |   |
                                                                              +-------------------------------------------------+                                              |   |
                                                                              |                                                 |                                              |   |
                                                                              |                                                 |                                              |   |
          +--------------------------+        +-------------------------+     |         ++---------------------------+          |     +--------------------------------------+ |   |
+---------+ TransformSerializeSegment<--------+ TransformSerializeBlock <-----+---------+|TransformAddComputedColumns|<---------+-----+TransformResortAddOnWithoutSourceSchema<-+   |
|         +--------------------------+        +-------------------------+     |         ++---------------------------+          |     +--------------------------------------+     |
|                                                                             |                                                 |                                                  |
|                                                                             |                                                 |                                                  |
|                                                                             |                                                 |                                                  |
|                                                                             |                                                 |                                                  |
|          +---------------+                 +------------------------------+ |               ++---------------+                |               +---------------+                  |
+----------+ TransformDummy|<----------------+ AsyncAccumulatingTransformer <-+---------------+|TransformDummy |<---------------+---------------+TransformDummy <------------------+
|          +---------------+                 +------------------------------+ |               ++---------------+                |               +---------------+
|                                                                             |                                                 |
|                                                                             |  If it includes 'computed', this section        |
|                                                                             |  of code will be executed, otherwise it won't   |
|                                                                             |                                                 |
|                                                                            -+-------------------------------------------------+
|
|
|
|        +------------------+            +-----------------------+        +-----------+
+------->|ResizeProcessor(1)+----------->|TableMutationAggregator+------->|CommitSink |
         +------------------+            +-----------------------+        +-----------+
```

If you are interested in learning more, please check out the resources below:

- [PR #13036 | chore: draw merge into pipeline](https://github.com/datafuselabs/databend/pull/13036)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- MERGE INTO now supports for automatic recluster and compaction.
- SQLsmith now covers `DELETE`, `UPDATE`, `ALTER TABLE`, and `CAST`.
- Added semi-structured data processing functions `json_each` and `json_array_elements`.
- Added time and date functions `to_week_of_year` and `date_part`. See [Docs | Date & Time Functions](https://databend.rs/doc/sql-functions/datetime-functions) for details.
- Read [Sending IoT Stream Data to Databend with LF Edge eKuiper](https://databend.rs/blog/2023-10-09-databend-and-ekuiper) to learn how Databend integrates with eKuiper to meet growing IoT data analytics demands.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Enhancing Role-Based Access Control

Currently, Databend's access control system consists of Role-Based Access Control (RBAC) and Discretionary Access Control (DAC). However, there is still room for improvement to make it more comprehensive.

We plan to support more privilege checks on uncovered resources and provide privilege definition guidance in 2023 Q4.

[Issue #13207 | Tracking: RBAC improvement plan in 2023 Q4](https://github.com/datafuselabs/databend/issues/13207)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.147-nightly...v1.2.160-nightly>
