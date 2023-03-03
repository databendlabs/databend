---
title: "This Week in Databend #83"
date: 2023-03-03
slug: 2023-03-03-databend-weekly
tags: [databend, weekly]
contributors:
- name: andylokandy
  url: https://github.com/andylokandy
  image_url: https://github.com/andylokandy.png
- name: ariesdevil
  url: https://github.com/ariesdevil
  image_url: https://github.com/ariesdevil.png
- name: b41sh
  url: https://github.com/b41sh
  image_url: https://github.com/b41sh.png
- name: BohuTANG
  url: https://github.com/BohuTANG
  image_url: https://github.com/BohuTANG.png
- name: Carlosfengv
  url: https://github.com/Carlosfengv
  image_url: https://github.com/Carlosfengv.png
- name: Chasen-Zhang
  url: https://github.com/Chasen-Zhang
  image_url: https://github.com/Chasen-Zhang.png
- name: ClSlaid
  url: https://github.com/ClSlaid
  image_url: https://github.com/ClSlaid.png
- name: drmingdrmer
  url: https://github.com/drmingdrmer
  image_url: https://github.com/drmingdrmer.png
- name: dusx1981
  url: https://github.com/dusx1981
  image_url: https://github.com/dusx1981.png
- name: everpcpc
  url: https://github.com/everpcpc
  image_url: https://github.com/everpcpc.png
- name: johnhaxx7
  url: https://github.com/johnhaxx7
  image_url: https://github.com/johnhaxx7.png
- name: mergify[bot]
  url: https://github.com/apps/mergify
  image_url: https://avatars.githubusercontent.com/in/10562
- name: PsiACE
  url: https://github.com/PsiACE
  image_url: https://github.com/PsiACE.png
- name: RinChanNOWWW
  url: https://github.com/RinChanNOWWW
  image_url: https://github.com/RinChanNOWWW.png
- name: soyeric128
  url: https://github.com/soyeric128
  image_url: https://github.com/soyeric128.png
- name: sandflee
  url: https://github.com/sandflee
  image_url: https://github.com/sandflee.png
- name: sundy-li
  url: https://github.com/sundy-li
  image_url: https://github.com/sundy-li.png
- name: TCeason
  url: https://github.com/TCeason
  image_url: https://github.com/TCeason.png
- name: wubx
  url: https://github.com/wubx
  image_url: https://github.com/wubx.png
- name: Xuanwo
  url: https://github.com/Xuanwo
  image_url: https://github.com/Xuanwo.png
- name: xudong963
  url: https://github.com/xudong963
  image_url: https://github.com/xudong963.png
- name: youngsofun
  url: https://github.com/youngsofun
  image_url: https://github.com/youngsofun.png
- name: zhang2014
  url: https://github.com/zhang2014
  image_url: https://github.com/zhang2014.png
- name: zhyass
  url: https://github.com/zhyass
  image_url: https://github.com/zhyass.png
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

> :loudspeaker: Databend v1.0 is about to be released, and v1.0.0-nightly is already available as a candidate version. https://github.com/datafuselabs/databend/releases/tag/v1.0.0-nightly

## What's On In Databend

Stay connected with the latest news about Databend.

### Support WebHDFS Backend

HDFS is a widely used distributed file system for big data, and one of the storage backends supported by Databend. However, in previous implementations, using HDFS depended on Java environment and some specific jars, which might be inconvenient for some users.

To solve this problem, Databend now supports WebHDFS as a storage backend, which facilitates users to interact with existing HDFS storage without Java environment. WebHDFS is a REST API that exposes HDFS operations through HTTP.

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

If you are interested in learning more about how this is achieved, and how it can improve your analytical experience, please refer to these pull requests:

- [PR | feat: backend webhdfs](https://github.com/datafuselabs/databend/pull/10285)
- [PR | feat: Add support for copying from webhdfs](https://github.com/datafuselabs/databend/pull/10156)

### Support Aggregate Spill to Object Storage

One of the challenges of processing large data volumes is how to perform GroupBy and OrderBy operations efficiently and reliably. To address this challenge, Databend community is working on a new feature that involves spilling intermediate results to cloud-based object storage like Amazon S3.

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

If you are interested in learning more about how Databend handles decimal types efficiently and accurately, please read on:

- [Docs | Data Types - Decimal](https://databend.rs/doc/sql-reference/data-types/data-type-decimal-types).

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Initialize `regex` at Compile Time

In `https://github.com/datafuselabs/databend/blob/main/src/query/service/src/servers/federated_helper.rs`, many regular expressions will be used to match queries. We can improve this by initializing [`regex`](https://crates.io/crates/regex) at compile time.

[Issue 10286: Feature: make regexp initialized at compile time](https://github.com/datafuselabs/databend/issues/10286)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## What's New

Check out what make Databend even better for you.

### New Contributors :sparkler:

- [@xinlifoobar](https://github.com/xinlifoobar) made their first contribution in [#10164](https://github.com/datafuselabs/databend/pull/10164)
- [@wangjili8417](https://github.com/wangjili8417) made their first contribution in [#10025](https://github.com/datafuselabs/databend/pull/10255)
- [@dusx1981](https://github.com/dusx1981) made their first contribution in [#10024](https://github.com/datafuselabs/databend/pull/10024)

### Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v0.9.48-nightly...v1.0.0-nightly>
