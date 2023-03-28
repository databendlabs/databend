---
title: "This Week in Databend #84"
date: 2023-03-10
slug: 2023-03-10-databend-weekly
tags: [databend, weekly]
description: "Get to know the latest updates on Databend this week!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Carlosfengv
  - name: Chasen-Zhang
  - name: dantengsky
  - name: drmingdrmer
  - name: dusx1981
  - name: everpcpc
  - name: jun0315
  - name: leiysky
  - name: mergify[bot]
  - name: PsiACE
  - name: quaxquax
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
  - name: xinlifoobar
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

> :loudspeaker: We announced the milestone release of Databend 1.0 this week. For what's new in the release, please read this post: <https://databend.rs/blog/databend-release-v1.0> .

## What's On In Databend

Stay connected with the latest news about Databend.

### SQL: `REPLACE INTO`

The `REPLACE INTO` statement is a powerful way to insert or update data in Databend. It allows you to specify a conflict key that determines whether a new row should be inserted or an existing row should be updated.

If a row with the same conflict key already exists in the table, Databend will update the row with the new data. Otherwise, the new data will be added to the table as a new row. You can use this statement to easily sync data from different sources or handle duplicate records.

```sql
#> CREATE TABLE employees(id INT, name VARCHAR, salary INT);
#> REPLACE INTO employees (id, name, salary) ON (id) VALUES (1, 'John Doe', 50000);
#> SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
|    1 | John Doe |  50000 |
+------+----------+--------+
```

If you want to learn more details about `REPLACE INTO` statement, please read the materials listed below.

- [Docs | DML Commands - REPLACE](https://databend.rs/doc/sql-commands/dml/dml-replace)
- [PR | feat: replace into statement](https://github.com/datafuselabs/databend/pull/10191)

### RFC: Add Incremental Update for Copy Into

Databend is currently capable of transforming and inserting data from a stage into a table. For example, you can run a SQL statement like this:

```sql
insert into table1 from (select c1， c2 from @stage1/path/to/dir);
```

The COPY INTO command needs a similar feature as well to support incremental data loading from a stage.

If you're interested, check the following RFCs:

- [Docs | RFCs - Transform During Load](https://databend.rs/doc/contributing/rfcs/transform-during-load)
- [Docs | RFCs - Stage With Schema](https://databend.rs/doc/contributing/rfcs/stage-with-schema)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Rust Toolchain: 🦀 v1.70.0-nightly (03-10)

Databend has upgraded its Rust toolchain to `nightly-2023-03-10`. Please remember to run `cargo clean` after pulling the latest code.

We also fixed some clippy warnings. We now use `#[default]` to annotate the default value of an `enum`, as shown below.

```rust
#[derive(Debug, Default)]
pub enum RecordDelimiter {
    #[default]
    Crlf,
    Any(u8),
}
```

If you are interested in this Rust trick, you can read this RFC: [`derive_default_enum`](https://rust-lang.github.io/rfcs/3107-derive-default-enum.html).

### Announcing OpenDAL v0.30

[OpenDAL](https://github.com/datafuselabs/opendal) brings some important changes in its newly released 0.30 version, including removing `Object` Abstraction and bindings for JavaScript and Python.

**Removing `Object` Abstraction**

The term `Object` was applied in various fields, so it used to be difficult to provide a concise definition of `opendal::Object`. We eliminated the intermediate API layer of `Object` so users can now directly utilize the `Operator`, for example:

```diff
# get metadata of a path
- op.object("path").stat().await;
+ op.stat("path").await;
```

**Bindings for JavaScript and Python**

We have released OpenDAL's JavaScript and Python bindings, thanks to [@suyanhanx](https://github.com/suyanhanx), [@messense](https://github.com/messense), [@Xuanwo](https://github.com/Xuanwo), and others who were involved, Now, users of these languages can access data from various services using OpenDAL. This is especially useful for data scientists and data analysts who only require OpenDAL and do not need any other SDKs.

Here is a Python example:

```python
>>> import opendal
>>> op = opendal.Operator('memory')
>>> op.write("test", b"Hello, World!")
>>> op.read("test")
b'Hello, World!'
```

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Support More Mathematical Operators like PostgreSQL

More mathematical operators in SQL expressions are required to perform complex calculations instead of writing them in another language or using external tools.

| Operator | Description         | Example    | Result |
| -------- | ------------------- | ---------- | ------ |
| ^        | exponentiation      | 2.0 ^ 3.0  | 8      |
| \|/      | square root         | \|/ 25.0   | 5      |
| \|\|/    | cube root           | \|\|/ 27.0 | 3      |
| !        | factorial           | 5 !        | 120    |
| @        | absolute value      | @ -5.0     | 5      |
| &        | bitwise AND         | 91 & 15    | 11     |
| \|       | bitwise OR          | 32 \| 3    | 35     |
| #        | bitwise XOR         | 17 # 5     | 20     |
| ~        | bitwise NOT         | ~1         | -2     |
| <<       | bitwise shift left  | 1 << 4     | 16     |
| >>       | bitwise shift right | 8 >> 2     | 2      |

[@jun0315](https://github.com/jun0315) is working on it and has made some great progress. If you are interested in this feature, feel free to drop in and contribute.

[Issue 10233: Feature: support more pg functions](https://github.com/datafuselabs/databend/issues/10233)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@jun0315](https://github.com/jun0315) made their first contribution in [#10347](https://github.com/datafuselabs/databend/pull/10347), adding the `caret` function that performs exponentiation. For example, 2^3 means 2 raised to the power of 3, which equals 8.
- [@quaxquax](https://github.com/quaxquax) made their first contribution in [#10465](https://github.com/datafuselabs/databend/pull/10465), fixing a typo, thanks!

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.0.1-nightly...v1.0.10-nightly>
