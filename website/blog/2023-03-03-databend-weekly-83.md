---
title: "This Week in Databend #83"
date: 2023-03-03
slug: 2023-03-03-databend-weekly
tags: [databend, weekly]
description: ""
contributors:
- name: andylokandy
- name: ariesdevil
- name: b41sh
- name: BohuTANG
- name: Carlosfengv
- name: Chasen-Zhang
- name: ClSlaid
- name: drmingdrmer
- name: dusx1981
- name: everpcpc
- name: johnhaxx7
- name: mergify[bot]
- name: PsiACE
- name: RinChanNOWWW
- name: soyeric128
- name: sandflee
- name: sundy-li
- name: TCeason
- name: wubx
- name: Xuanwo
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

> :loudspeaker: Databend's official release of version 1.0 is just around the corner, and a preview version, v1.0.0-nightly, is already available as a candidate. Check it out at https://github.com/datafuselabs/databend/releases/tag/v1.0.0-nightly

## What's On In Databend

Stay connected with the latest news about Databend.

### Support for WebHDFS

HDFS is a popular distributed file system for managing big data and is one of the storage backends supported by Databend. However, incorporating HDFS into Databend required a Java environment and specific jars, which were not suitable in some circumstances.

Databend now offers support for WebHDFS as a storage backend, allowing users to interact with existing HDFS storage without requiring Java environment. WebHDFS is a REST API that exposes HDFS operations through HTTP, making it more accessible to users.

```sql
#> CREATE STAGE IF NOT EXISTS whdfs URL='webhdfs://127.0.0.1:9870/data-files/' CONNECTION=(HTTPS='false');
Query OK, 0 rows affected (0.020 sec)

#> CREATE TABLE IF NOT EXISTS books (     title VARCHAR,     author VARCHAR,     date VARCHAR );
Query OK, 0 rows affected (0.030 sec)

#> COPY INTO books FROM @whdfs FILES=('books.csv') file_format=(type=CSV field_delimiter=','  record_delimiter='\n' skip_header=0);
Query OK, 2 rows affected (0.615 sec)

#> SELECT * FROM books;
+------------------------------+---------------------+------+
| title                        | author              | date |
+------------------------------+---------------------+------+
| Transaction Processing       | Jim Gray            | 1992 |
| Readings in Database Systems | Michael Stonebraker | 2004 |
+------------------------------+---------------------+------+
2 rows in set (0.044 sec)
```

Check out these pull requests if you're interested in how it works and improving your analytical experience:

- [PR | feat: backend webhdfs](https://github.com/datafuselabs/databend/pull/10285)
- [PR | feat: Add support for copying from webhdfs](https://github.com/datafuselabs/databend/pull/10156)

### Support for Aggregation Spilling to Object Storage

One of the challenges of processing large data volumes is how to perform GroupBy and OrderBy operations efficiently and reliably. To address this challenge, the Databend community is working on a new feature that involves spilling intermediate results to cloud-based object storage like Amazon S3.

This feature will enable Databend to handle GroupBy and OrderBy queries with unlimited data size, without running out of memory or compromising performance. If you want to learn more about how this feature works and how it can benefit your analytics needs, please check out this pull request:

- [PR | feat(query): support aggregate spill to object storage](https://github.com/datafuselabs/databend/pull/10273)

### Decimal Data Types

Databend supports various data types for analytics. One of the data types that Databend has recently improved is decimal, which can store exact numeric values with a fixed precision and scale. Decimal types are useful for applications that require high accuracy, such as financial calculations.

```sql
-- Create a table with decimal data type.
create table decimal(value decimal(36, 18));

-- Insert two values.
insert into decimal values(0.152587668674722117), (0.017820781941443176);

select * from decimal;
+----------------------+
| value                |
+----------------------+
| 0.152587668674722117 |
| 0.017820781941443176 |
+----------------------+
```
If you're interested in how Databend handles decimal types efficiently and accurately, please read on:

- [Docs | Data Types - Decimal](https://databend.rs/doc/sql-reference/data-types/data-type-decimal-types).

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Initialize `regex` at Compile Time

If you read the code in [federated_helper.rs](https://github.com/datafuselabs/databend/blob/main/src/query/service/src/servers/federated_helper.rs), you may notice that numerous regular expressions are employed to match queries.

```rust
use regex::bytes::RegexSet;

let regex_set = RegexSet::new(regex_rules).unwrap();
let matches = regex_set.matches(query.as_ref());
```

This can be optimized by initializing the [`regex`](https://crates.io/crates/regex) at compile time.

[Issue 10286: Feature: make regexp initialized at compile time](https://github.com/datafuselabs/databend/issues/10286)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@xinlifoobar](https://github.com/xinlifoobar) made their first contribution in [#10164](https://github.com/datafuselabs/databend/pull/10164)
- [@wangjili8417](https://github.com/wangjili8417) made their first contribution in [#10255](https://github.com/datafuselabs/databend/pull/10255)
- [@dusx1981](https://github.com/dusx1981) made their first contribution in [#10024](https://github.com/datafuselabs/databend/pull/10024)

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v0.9.48-nightly...v1.0.0-nightly>
