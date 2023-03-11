---
title: Rewrite sqllogictest framework in rust
description: sqllogictest, rust
date: 2022-12-29
tags: [databend, sqllogictest, rust]
cover_url: rewrite-in-rust.png
authors:
- name: Xudong
  url: https://github.com/xudong963
  image_url: https://github.com/xudong963.png
---

**Rewriting sqllogictest Framework with Rust**

This post is about a big move we've made for Databend. We successfully switched the sqllogictest framework from Python to Rust using sqllogictest-rs, a robust implementation of the sqllogictest framework for the Rust ecosystem. Sqllogictest was designed with SQLite in mind. Benefiting from its neutrality towards database engines, we can use Sqllogictest to verify the accuracy of a SQL database engine as well. This is done by comparing query results from multiple SQL engines running the same query.

**Why sqllogictest-rs**?

The original sqllogictest framework ([RFC for sqllogictest](https://databend.rs/doc/contributing/rfcs/new_sql_logic_test_framework)) was written in Python. We planned a switch to [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) for the following reasons:

- The entire Databend team is proficient in Rust. Working with a unified codebase written in Rust would boost our productivity over the long term.
- The previous framework lacked a strict parser at the front end and resulted in errors going undetected.
- As the sqllogictest-rs crate is maturing, building a new sqllogictest framework based on it would save us a lot of effort in the long run.
- We expected a 10x performance boost from the switch to Rust. The Python sqllogictest had been experiencing suboptimal runtime that resulted in a slower CI.

**How We Nailed It**

Our first version of sqllogictest doesn't strictly follow the [sqllogictest wiki](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki). This is a little bit frustrating because we have to manually adjust the format of the test files, for example, in some cases like these:

- Extra blank lines between the query and  `----`.
- Non-identical comment formats.
- Confusing empty strings: It displays results from queries like `select ' '`  with ` `, rather than `(empty)`.

Databend supports three types of client handlers: MySQL, HTTP, and ClickHouse. Each type of them returns content in a different format. The HTTP handler returns content in JSON format and the ClickHouse handler returns it in TSV, both of which require the following substitutions:

- inf -> Infinity
- nan -> NaN
- \\N -> NULL

We introduced `sandbox tenant` to increase parallelism. Each test file now runs in parallel in its own sandbox environment that is separated from each other. The benefits of doing so include preventing a database or table from being dropped by mistake and significantly reducing test time.

**Unsolved Issues​**

We're still figuring out the most effective way to test a query that returns dynamic results. For example, the `Create_time` in the result returned from `SHOW TABLE STATUS`.

**After the Switch**

We're glad to see an efficiency improvement after going with sqllogictest-rs and this will benefit the entire Databend community. Our special thanks go to [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) for the great support, and everyone who has been involved. If you're also a fan of sqllogictest, stay tuned for more exciting news by visiting the following links:

- [README](https://github.com/datafuselabs/databend/blob/main/tests/sqllogictests/README.md)
- [sqllogictest tracking](https://github.com/datafuselabs/databend/issues/9174)