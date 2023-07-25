---
title: "This Week in Databend #103"
date: 2023-07-23
slug: 2023-07-23-databend-weekly
cover_url: 'weekly/weekly-103.jpg'
image: 'weekly/weekly-103.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: ariesdevil
  - name: b41sh
  - name: ben1009
  - name: BohuTANG
  - name: dantengsky
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: leiysky
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
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

### Creating Network Policies

**Network Policies** are used to manage access to Databend services. They can also be utilized to restrict access to a SQL user account based on users' IP addresses.

To create a set of network policies:

```SQL
CREATE NETWORK POLICY <policy-name> ALLOWED_IP_LIST=(<allowed-ip>) BLOCKED_IP_LIST=(<blocked-ip>) COMMENT=<policy-comment>
```

To restrict access to a SQL user account:

```SQL
CREATE USER <user-name> IDENTIFIED BY <user-password> WITH SET NETWORK POLICY=<policy-name>

--- OR ---

ALTER USER <user-name> WITH SET NETWORK POLICY=<policy-name>
```

If you are interested in learning more, please check out the resources listed below.

- [PR #11988 | feat(query): Support create network policy](https://github.com/datafuselabs/databend/pull/11988)
- [PR #12137 | feat(query): Alter user support set/unset network policy](https://github.com/datafuselabs/databend/pull/12137)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Profiling Table Scan

Databend now supports `wait_time` for profiling table scans. This feature offers insights into time spent reading data from storage, assisting in determining whether a query is I/O or CPU bound.

![](/img/blog/profiling-table-scan.jpg)

If you are interested in learning more, please check out the resources listed below:

- [PR #12158 | feat(planner): Support profile table scan](https://github.com/datafuselabs/databend/pull/12158)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added `intersect_count` and `to_yyyymmddhh` functions.
- Added `fuse_column` system function.
- Read document [Docs | RECLUSTER TABLE](https://databend.rs/doc/sql-commands/ddl/clusterkey/dml-recluster-table) to understand the updated reclustering mechanism.
- Read documents [Docs | Fuse Engine](https://databend.rs/doc/sql-reference/table-engines/fuse#options) and [Docs | ALTER TABLE OPTION](https://databend.rs/doc/sql-commands/ddl/table/alter-table-option) to learn the options available in Fuse Engine and how to modify them.
- Read document [Docs | Vacuum Dropped Tables](https://databend.rs/doc/sql-commands/ddl/table/vacuum-drop-table) to learn about our latest enterprise feature and how it can help you save storage space.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Renaming TSV file format to Text

Databend supports a file format called TSV, which may mislead users into thinking that its only difference from CSV is the field delimiter.

In reality, TSV corresponds to the text format of MySQL/Hive/PostgreSQL and uses escaping instead of quoting to handle delimiters in values (MySQL text supports quoting but not by default). ClickHouse also uses it for transferring data to/from MySQL.

Therefore, we suggest renaming TSV to TEXT.

[Issue #11987 | Rename TSV file format to Text](https://github.com/datafuselabs/databend/issues/11987)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@ben1009](https://github.com/ben1009) made their first contribution in [#12144](https://github.com/datafuselabs/databend/pull/12144). Fixed the typo in the error message.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.25-nightly...v1.2.32-nightly>
