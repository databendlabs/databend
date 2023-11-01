---
title: "This Week in Databend #107"
date: 2023-08-20
slug: 2023-08-20-databend-weekly
cover_url: 'weekly/weekly-107.jpg'
image: 'weekly/weekly-107.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: ariesdevil
  - name: b41sh
  - name: dantengsky
  - name: Dousir9
  - name: everpcpc
  - name: flaneur2020
  - name: JackTan25
  - name: lichuang
  - name: nange
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
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

## What's On In Databend

Stay connected with the latest news about Databend.

### Understanding Connection Parameters

The connection parameters refer to a set of essential connection details required for establishing a secure link to supported external storage services, like Amazon S3. These parameters are enclosed within parentheses and consists of key-value pairs separated by commas or spaces. It is commonly utilized in operations such as creating a stage, copying data into Databend, and querying staged files from external sources. 

For example, the following statement creates an external stage on Amazon S3 with the connection parameters:

```sql
CREATE STAGE my_s3_stage
's3://load/files/'
CONNECTION = (
    ACCESS_KEY_ID = '<your-access-key-id>',
    SECRET_ACCESS_KEY = '<your-secret-access-key>'
);
```

If you are interested in learning more, please check out the resources listed below.

- [Docs | SQL Reference - Connection Parameters](https://databend.rs/doc/sql-reference/connect-parameters)

### Adding Storage Parameters for Hive Catalog

Over the past week, Databend introduced storage parameters for the Hive Catalog, allowing the configuration of specific storage services. This means that the catalog no longer relies on the storage backend of the default catalog.

The following example shows how to create a Hive Catalog using MinIO as the underlying storage service:

```sql
CREATE CATALOG hive_ctl 
TYPE = HIVE 
CONNECTION =(
    ADDRESS = '127.0.0.1:9083' 
    URL = 's3://warehouse/' 
    AWS_KEY_ID = 'admin' 
    AWS_SECRET_KEY = 'password' 
    ENDPOINT_URL = 'http://localhost:9000/'
)
```

If you are interested in learning more, please check out the resources listed below.

- [Issue #12407 | Feature: Add storage support for Hive catalog](https://github.com/datafuselabs/databend/issues/12407)
- [PR #12469 | feat: Add storage params in hive catalog](https://github.com/datafuselabs/databend/pull/12469)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Using `gitoxide` to Speed Up Git Dependency Downloads

`gitoxide` is a high-performance, modern Git implementation written in Rust. Utilizing the `gitoxide` feature of cargo (Unstable), the `gitoxide` crate can replace `git2` to perform various Git operations, thereby achieving several times performance improvement when downloading crates-index and git dependencies.

Databend has recently enabled this feature for `cargo {build | clippy | test}`` in CI. You can also try to add the -Zgitoxide option to speed up the build process during local development:

```bash
cargo -Zgitoxide=fetch,shallow-index,shallow-deps build
```

If you are interested in learning more, please check out the resources listed below:

- [chore(ci): cargo build with -Zgitoxide](https://github.com/datafuselabs/databend/pull/12504)
- [The Cargo Book | Unstable - gitoxide](https://doc.rust-lang.org/nightly/cargo/reference/unstable.html#gitoxide)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- [`VALUES` clause](https://databend.rs/doc/sql-commands/query-syntax/values) can be used without being combined with `SELECT`.
- You can now set a default value when modifying the type of a column. See [Docs | ALTER TABLE COLUMN](https://databend.rs/doc/sql-commands/ddl/table/alter-table-column) for details.
- Databend can now automatically recluster a table after write operations such as `COPY INTO` and `REPLACE INTO`.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Enhancing `infer_schema` for All File Locations

Currently, it is possible to query files using file locations or from stages in Databend.

```sql
select * from 'fs:///home/...';
select * from 's3://bucket/...';
select * from @stage;
```

However, the `infer_schema` function only works with staged files. For example:

```sql
select * from infer_schema(location=>'@stage/...');
```

When attempting to use `infer_schema` with other file locations, it leads to a panic:

```sql
select * from infer_schema(location =>'fs:///home/...'); -- this will panic.
```

So, the improvement involves extending the `infer_schema` capability to encompass all types of file paths, not limited to staged files. This will enhance system consistency and the usefulness of the `infer_schema` function.

[Issue #12458 | Feature: `infer_schema` support normal file path](https://github.com/datafuselabs/databend/issues/12458)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.62-nightly...v1.2.74-nightly>
