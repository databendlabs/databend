---
title: "This Week in Databend #91"
date: 2023-04-30
slug: 2023-04-30-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy 
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: dependabot[bot]
  - name: Dousir9
  - name: everpcpc
  - name: flaneur2020
  - name: jun0315
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: tisonkun
  - name: wubx
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

### New datatype: BITMAP

Databend has added support for the bitmap datatype.

`BITMAP` is a type of compressed data structure that can be used to efficiently store and manipulate sets of boolean values. It is often used to accelerate count distinct.

```sql
> CREATE TABLE IF NOT EXISTS t1(id Int, v Bitmap) Engine = Fuse;
> INSERT INTO t1 (id, v) VALUES(1, to_bitmap('0, 1')),(2, to_bitmap('1, 2')),(3, to_bitmap('3, 4'));
> SELECT id, to_string(v) FROM t1;

┌──────────────────────┐
│   id  │ to_string(v) │
│ Int32 │    String    │
├───────┼──────────────┤
│     1 │ 0,1          │
│     2 │ 1,2          │
│     3 │ 3,4          │
└──────────────────────┘
```

Our implementation of the BITMAP data type utilizes `RoaringTreemap`, a compressed bitmap with u64 values. Using this data structure brought us improved performance and decreased memory usage in comparison to alternative bitmap implementations.

If you are interested in learning more, please check out the resources listed below.

- [PR #11097 | feat: add bitmap data type](https://github.com/datafuselabs/databend/pull/11097)
- [Website | Roaring Bitmaps](https://roaringbitmap.org/)
- [Paper | Consistently faster and smaller compressed bitmaps with Roaring](https://arxiv.org/pdf/1603.06549.pdf)

### Improving Hash Join Performance with New Hash Table Design

We optimized our previous hash table implementation for aggregation functions, but it significantly limited hash join operation performance. To improve hash join performance, we implemented a dedicated hash table optimized for it. We allocated a fixed-size hash table based on the number of rows in the build stage and replaced the value type with a pointer that supports CAS operations, ensuring memory control without the need for Vec growth. The new implementation significantly improved performance. Check out the resources below for more information:

- [PR #11140 | feat(query): new hash table and parallel finalize for hash join](https://github.com/datafuselabs/databend/pull/11140)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Rust Compilation Challenges and Solutions

Compiling a medium to large Rust program is not a breeze due to the accumulation of complex project dependencies and boilerplate code.

To address these challenges, Databend team implemented several measures, including observability tools, configuration adjustments, caching, linker optimization, compile-related profiles, and refactoring.

If you are interested in learning more, please check out the resources listed below.

- [Optimizing Compilation for Databend](https://databend.rs/blog/2023/04/20/optimizing-compilation-for-databend)

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- Databend will participate in OSPP 2023 projects: [OSPP2023 - Databend](https://summer-ospp.ac.cn/org/orgdetail/646b9834-3923-4e74-b98b-90afec341705?lang=en).
- Check out [Docs | Developing with Databend using Rust](https://databend.rs/doc/develop/rust) for Rust application development with `databend-driver`.
- Learn to manage and query databases with ease using BendSQL, a powerful command-line tool for Databend. Check out [Docs | BendSQL](https://databend.rs/doc/integrations/access-tool/bendsql) now!
- Check out [Docs | Loading from a Stage](https://databend.rs/doc/load-data/stage) and [Docs | Loading from a Bucket](https://databend.rs/doc/load-data/s3) to learn more about loading data from stages and object storage buckets.
- Introduced `table-meta-inspector`, a command-line tool for decoding new table metadata in Databend.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Contributors Wanted for Function Development

We are currently working on improving our functions, and we need your help!

We have identified four areas that require attention, and we would be extremely grateful for any assistance that you can provide.

- [Bitmap Functions](https://github.com/datafuselabs/databend/issues/11219)
- [Window Functions](https://github.com/datafuselabs/databend/issues/11148)
- [Geo Functions](https://github.com/datafuselabs/databend/issues/6390)
- [JSON Functions and Operators](https://github.com/datafuselabs/databend/issues/11270)

If you are interested in contributing to any of these areas, please refer to the following resources to learn more about how to write scalar and aggregate functions:

- [How to Write Scalar Functions](https://databend.rs/doc/contributing/how-to-write-scalar-functions)
- [How to Write Aggregate Functions](https://databend.rs/doc/contributing/how-to-write-aggregate-functions)

We appreciate any help that you can provide, and we look forward to working with you.

[Issue #11220 | Tracking: functions](https://github.com/datafuselabs/databend/issues/11220)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.14-nightly...v1.1.23-nightly>
