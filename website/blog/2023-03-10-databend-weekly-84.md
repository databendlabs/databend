---
title: "This Week in Databend #84"
date: 2023-03-10
slug: 2023-03-10-databend-weekly
tags: [databend, weekly]
description: "Learn about the latest updates of Databend in a week!"
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

> :loudspeaker: This week we announced the official release of Databend v1.0. This is an important milestone, to learn more, please check out our blog: https://databend.rs/blog/databend-release-v1.0

## What's On In Databend

Stay connected with the latest news about Databend.

### SQL: `REPLACE INTO`

The `REPLACE INTO` statement is a powerful way to insert or update data in Databend. It allows you to specify a conflict key (not necessarily a primary key) that determines whether a new row should be inserted or an existing row should be updated.

If a row with the same conflict key already exists in the table, it will be replaced by the new data. Otherwise, a new row will be added with the new data. You can use this statement to easily sync data from different sources or handle duplicate records.

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

Also, we plan to gradually transition to the `MERGE INTO` statement based on this, and if you are interested in this topic, you can also follow this issue:

- [Issue | tracking: `MERGE` #10373](https://github.com/datafuselabs/databend/issues/10373)

### RFC: Transform During Load

Currently, we support stage table function, so we can transform when insert from stage into table.

```sql
insert into table1 from (select c1， c2 from @stage1/path/to/dir);
```

but this still missing one feature of `COPY INTO`: incremental load (remember files already loaded).

We plan to support transform when copying from stage to table, which enables a series of features such as incremental load and auto cast.

If you are interested in this topic, you can read the following two RFCs:

- [Docs | RFCs - Transform During Load](https://databend.rs/doc/contributing/rfcs/transform-during-load)
- [Docs | RFCs - Stage With Schema](https://databend.rs/doc/contributing/rfcs/stage-with-schema)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Rust Toolchain: 🦀 v1.70.0-nightly (03-10)

Databend has upgraded its Rust toolchain to `nightly-2023-03-10`. Please remember to run `cargo clean` after pulling the latest code.

We fixed some clippy warnings that we encountered. One notable thing is that we now use `#[default]` to annotate the default value of an `enum`, as shown below.

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

The term `Object` is applied in various fields, making it difficult to provide a concise definition of `opendal::Object`. We eliminated the intermediate API layer of `Object` and enabled users to directly utilize `Operator`. For example:

```diff
# get metadata of a path
- op.object("path").stat().await;
+ op.stat("path").await;
```

**Bindings for JavaScript and Python**

With [@suyanhanx](https://github.com/suyanhanx) [@messense](https://github.com/messense) [@Xuanwo](https://github.com/Xuanwo) and others' help, we released opendal's JavaScript and Python bindings. Now users of these languages can access data in various services with opendal. This is useful for Data Scientist and Data Analyst who **only** need opendal and **no** other SDKs.

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

More mathematical operators in SQL expressions are wanted to perform complex calculations on data without having to write them in another language or use external tools.

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

[@jun0315](https://github.com/jun0315) is working on this issue and has made some great progress. If you are interested in this feature, you are welcome to join the contribution.

[Issue 10233: Feature: support more pg functions](https://github.com/datafuselabs/databend/issues/10233)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@jun0315](https://github.com/jun0315) made their first contribution in [#10347](https://github.com/datafuselabs/databend/pull/10347), adding the `caret` function that performs exponentiation. For example, 2^3 means 2 raised to the power of 3, which equals 8.
- [@quaxquax](https://github.com/quaxquax) made their first contribution in [#10465](https://github.com/datafuselabs/databend/pull/10465), fixing a typo, thanks!

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.0.1-nightly...v1.0.10-nightly>
