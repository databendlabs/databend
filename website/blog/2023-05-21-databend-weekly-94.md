---
title: "This Week in Databend #94"
date: 2023-05-21
slug: 2023-05-21-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: gitccl
  - name: hantmac
  - name: Jake-00
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: silver-ymz
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
  - name: youngsofun
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

### Computed Columns

Computed columns are generated from other columns by a scalar expression. There are two types of computed columns: stored and virtual.

A stored computed column computes and stores the result value when a row is inserted. Use this SQL syntax to create one:

  ```sql
  column_name <type> AS (<expr>) STORED
  ```

While a virtual computed column is calculated at query time and does not store the result value. To create one, use this SQL syntax:

  ```sql
  column_name <type> AS (<expr>) VIRTUAL
  ```

- [PR | feat(query): Support Computed columns](https://github.com/datafuselabs/databend/pull/11391)

### VACUUM TABLE

The `VACUUM TABLE` command helps to optimize the system performance by freeing up storage space through the permanent removal of historical data files from a table. This includes:

- Snapshots associated with the table, as well as their relevant segments and blocks.
- Orphan files. Orphan files in Databend refer to snapshots, segments, and blocks that are no longer associated with the table. Orphan files might be generated from various operations and errors, such as during data backups and restores, and can take up valuable disk space and degrade the system performance over time.

`VACUUM TABLE` requires **Enterprise Edition**. To inquire about upgrading, please contact [Databend Support](https://www.databend.com/contact-us).

If you are interested in learning more, please check out the resources listed below:

- [Docs | VACUUM TABLE](https://databend.rs/doc/sql-commands/ddl/table/vacuum-table)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Enable Cache in Python Binding

Databend supports data caching and query result caching, which can effectively accelerate queries. The Python bindings of Databend also support these features, albeit with slight differences.

For query result caching, SQL statements can be used to set it up, which is very convenient.

```sql
>>> from databend import SessionContext 
>>> ctx = SessionContext()
>>> ctx.sql("set enable_query_result_cache = 1")
```

For data caching, it can be enabled through environment variables.

```python
>>> import os 
>>> os.environ["CACHE_DATA_CACHE_STORAGE"] = "disk"
>>> from databend import SessionContext 
>>> ctx = SessionContext()
>>> ctx.sql("select * from system.configs where name like '%data_cache%'")
┌────────────────────────────────────────────────────────────────────────────┐
│  group  │                   name                   │  value  │ description │
│  String │                  String                  │  String │    String   │
├─────────┼──────────────────────────────────────────┼─────────┼─────────────┤
│ 'cache' │ 'data_cache_storage'                     │ 'disk'  │ ''          │
│ 'cache' │ 'table_data_cache_population_queue_size' │ '65536' │ ''          │
└────────────────────────────────────────────────────────────────────────────┘
```

Feel free to use it in your data science workflow:

- [databend · PyPI](https://pypi.org/project/databend/)

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- Read [Docs | Date & Time - Formatting Date and Time](https://databend.rs/doc/sql-reference/data-types/data-type-time-date-types#formatting-date-and-time) to learn how to precisely control the format of time and date.
- Added support for transforming data when loading it from a URI.
- Added support for replacing with stage attachment.
- Added bitmap-related functions: `bitmap_contains`, `bitmap_has_all`, `bitmap_has_any`, `bitmap_or`, `bitmap_and`, `bitmap_xor`, etc.
- Supported `intdiv` operator `//`.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Remove `if_not_exists` from the Meta Request

In `CreateIndexReq`/`CreateTableReq`, we use `if_not_existed` to indicate whether an index/table exists.

```rust
pub struct CreateIndexReq {
    pub if_not_exists: bool,
    pub name_ident: IndexNameIdent,
    pub meta: IndexMeta,
}
```

The `if_not_exists` clause only affects the outcome that is presented to the user, and does not alter the behavior of the meta-service operation.

Therefore, it will be more effective for `SchemaApi` to provide either a **Created** or an **Exist** status code, allowing the caller to determine whether to generate an error message.

[Issue #11456 | Moving if_not_exists out of meta request body](https://github.com/datafuselabs/databend/issues/11456)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@silver-ymz](https://github.com/silver-ymz) made their first contribution in [#11487](https://github.com/datafuselabs/databend/pull/11487). Added five bitmap-related functions.
* [@Jake-00](https://github.com/Jake-00) made their first contribution in [#11503](https://github.com/datafuselabs/databend/pull/11503). Modified duplicate test case for `SOUNDS LIKE` syntax.
* [@gitccl](https://github.com/gitccl) made their first contribution in [#11507](https://github.com/datafuselabs/databend/pull/11507). Added five bitmap-related functions and fixed panic when calling with empty bitmap.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.38-nightly...v1.1.43-nightly>
