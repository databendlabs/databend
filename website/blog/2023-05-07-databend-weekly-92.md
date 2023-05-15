---
title: "This Week in Databend #92"
date: 2023-05-07
slug: 2023-05-07-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: b41sh
  - name: BohuTANG
  - name: dantengsky
  - name: drmingdrmer
  - name: everpcpc
  - name: JackTan25
  - name: leiysky
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: Xuanwo
  - name: xudong963
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

### Using Column Aliases in a Where Clause

A column alias provides a way to create a clean or more descriptive header for a results set.

Databend now supports column aliases in WHERE clause. Please note that if the alias and the column name are the same, the WHERE clause will recognize the alias as the column name:

```sql
> select number * 2 as number from numbers(3) where (number + 1) % 3 = 0;

┌────────┐
│ number │
│ UInt64 │
├────────┤
│      4 │
└────────┘
```

If you are interested in learning more, please check out the resources listed below.

- [PR #11272 | feat: support alias in where clause](https://github.com/datafuselabs/databend/pull/11272)

### `databend-metactl` Is Included in the Databend Release

`databend-metactl` is a command-line tool that allows users to manage the Databend Meta Service cluster. It can be used to back up and restore the metadata.

Now, databend-metactl will be released together with Databend and no longer requires building it manually.

If you are interested in learning more, please check out the resources listed below.

- [Docs | Back Up and Restore Databend Meta Service Cluster](https://databend.rs/doc/deploy/metasrv/metasrv-backup-restore)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Troubleshooting `databend-meta` Connection Issues with Invalid Semver Error

When compiling and running Databend, you may encounter connection issues with databend-meta and receive an error message containing `Invalid semver`. This is due to Databend's protocol compatibility check using semantic versions.

To resolve this issue, you can fetch the latest tags from the official Databend repository using the command `git fetch https://github.com/datafuselabs/databend.git --tags`. This ensures that your project is using the latest version of databend-meta and passes the version check.

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- Optimizer now supports constant folding. If interested, please check [PR #11216](https://github.com/datafuselabs/databend/pull/11216).
- Learn how to [Transform Data During Load](https://databend.rs/doc/load-data/data-load-transform) through three simple tutorials.
- Added bitmap functions: `bitmap_count` and `build_bitmap`.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Introduce Timeout Mechanism to Control the Query Optimization Time

Optimizing complex queries with numerous joins can be a time-consuming task. To avoid performing an exhaustive search of the entire query plan space, we can set a timeout for optimization.

The timeout can be based on a logical time, such as the number of applied transform rules, rather than wall time. Once the timeout is reached, a greedy search can be performed instead of generating new transform rules.

[Issue #11133 | Introduce timeout mechanism to control the query optimization time](https://github.com/datafuselabs/databend/issues/11133)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@JackTan25](https://github.com/JackTan25) made their first contribution in [#11290](https://github.com/datafuselabs/databend/pull/11290). The PR fixes table options validation.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.23-nightly...v1.1.30-nightly>
