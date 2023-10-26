---
title: "This Week in Databend #116"
date: 2023-10-22
slug: 2023-10-22-databend-weekly
cover_url: 'weekly/weekly-116.jpg'
image: 'weekly/weekly-116.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTang
  - name: dantengsky
  - name: dependabot[bot]
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: lichuang
  - name: PsiACE
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
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

### Feature Preview: Managing Background Tasks with SQL

Previously, Databend introduced **Background Service** to allow Databend to run one-off background jobs or cron jobs in daemon mode. This simplifies the complexity of managing data maintenance tasks.

To make it easy to create, manage and maintain background tasks, Databend recently added support for a series of SQL statements including `CREATE TASK`, `ALTER TASK`, `SHOW TASK`, etc.

For example, the following SQL statement instructs Databend to execute a task called `MyTask1` every day at `6am PST` to insert `(1, 2)` and `(3, 4)` into table `t`:

```sql
CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 6 * * *' 'America/Los_Angeles' COMMENT = 'serverless + cron' AS insert into t (c1, c2) values (1, 2), (3, 4) 
```

The **Background Service** requires Databend Enterprise Edition. Please [contact the Databend team](https://www.databend.com/contact-us) for upgrade information.

If you are interested in learning more, please check out the resources below:

- [PR #13316 | feat: impl create task sql parser and planner](https://github.com/datafuselabs/databend/pull/13316)
- [PR #13344 | feat: add SQL syntax support for alter, execute, describe, show, drop task](https://github.com/datafuselabs/databend/pull/13344)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Understanding Configuration Mapping in Databend

For a complex database service like Databend, supporting many configurable options helps developers manage and tune the system. A recently published blog explains the mappings between command line options, environment variables, and config files by reading the Databend code.

Databend currently supports three configuration methods in decreasing order of priority:

1. Command line options: Override configurations set elsewhere.
2. Environment Variables: Provide configuration flexibility for Kubernetes clusters.
3. Config files: A recommended approach for recording and managing configurations.

Please note that Databend also supports common environment variables from storage services such as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` through the rollback mechanism in [opendal](https://github.com/apache/incubator-opendal). You can directly utilize these variables when working with Databend.

If you are interested in learning more, please check out the resources below:

- [Blog | Navigating Databend's Configuration Maze: A Guide for Developers and Operators](https://databend.rs/blog/2023-10-18-databend-configuration-guide)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added a new table function `fuse_encoding` .
- Added new string functions `split` and `split_part`.
- SQLsmith now supports for `MERGE INTO`.
- `databend-metactl` supports viewing cluster status.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Support More Type Comparisons in z3 Solver

Previously, Databend introduced [z3.rs](https://github.com/prove-rs/z3.rs) to solve constraints by finding solutions that satisfy given conditions. [Z3](https://github.com/Z3Prover/z3) from Microsoft Research is a theorem prover commonly used to solve SMT problems. However, currently Databend's z3 solving only supports integer comparisons and needs to cover more types.

For example, after string comparison is supported, `t3.f >= '2000-08-23'` in `select * from t1 left join t3 on t3.e = t1.a where t3.f >= '2000-08-23';` can be pushed down to table `t3`.

[Issue #13236 | Feature: z3 supports more type comparison](https://github.com/datafuselabs/databend/issues/13236)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.160-nightly...v1.2.174-nightly>
