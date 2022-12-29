---
title: Rewrite sqllogictest framework in rust
description: sqllogictest, rust
date: 2022-12-29
tags: [databend, sqllogictest, rust]
authors:
- name: Xudong
  url: https://github.com/xudong963
  image_url: https://github.com/xudong963.png
---

Sqllogictest is a program designed to verify that an SQL database engine computes correct results by comparing the results to identical queries from other SQL database engines. Sqllogictest was originally designed to test [SQLite](http://www.sqlite.org/), but it is database engine neutral and can just as easily be used to test other database products.

In the rust ecosystem, [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) is a very good implementation of the sqllogictest framework, thanks to which databend can easily and quickly switch the sqllogictest framework from python to rust.

### Background

- For some historical reasons, databend's original sqllogictest framework was the python version, as seen in [rfc for sqllogictest](https://databend.rs/doc/contributing/rfcs/new_sql_logic_test_framework)
- We decided to rewrite it in rust for the following reasons:
  - Unified codebase, all with rust, long-term can improve the framework development and iteration speed, after all, the whole team is rustacean.
  - The old framework did not have a strict parser front end and some errors could not be caught.
  - [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) crate is maturing and building databend's new sqllogictest framework based on it can save a lot of labor.
  - Switching from python to rust, there is a potential performance gain. The current python version of sqllogictest has a suboptimal runtime, resulting in a slower CI, and a more desirable end result, **with about 10x improvement**.

### Implementation

- The first version of sqllogictest does not strictly follow the [sqllogictest wiki](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) implementation, so the format of the test file needs to be adjusted (purely physical work), for example:
  - query and `----` has extra blank lines in between.
  - The comment format is different.
  - Some queries with empty results, such as `select ' ', 1`, are displayed directly with ` `, which can easily cause confusion throughout the test file and needs to be displayed with `(empty)` instead.
  - ...
- Databend supports three client handlers: mysql, http, and clickhouse as described in [handlers](https://databend.rs/doc/reference/api), each of which returns a different format of content. Mysql is more normal, but http returns json and clickhouse returns tsv. In http and clickhouse, the following substitutions need to be made:
  - `inf` -> `Infinity`
  - `nan` -> `NaN`
  - `\\N` -> `NULL`
- Isolation between test files. In order to increase the parallelism as much as possible (multiple test files can run in parallel), we need to isolate different files to prevent misuse of database or table, avoid database or table being dropped by mistake, we introduced `sandbox tenant`, each test file a separate sandbox environment, so that the files can run in parallel, greatly reducing the test time.
- ...

### Unsolved Issues
What is the best way to effectively test a query that has dynamic results? 
- For example, `SHOW TABLE STATUS` results will include the creation time of the table, which is dynamic, and it is worth discussing how to test such sql.

### Conclusion
Overall, the benefits of switching from the python version to the rust version to solve the problems mentioned in the background are very good, and [RIIR](https://github.com/ansuz/RIIR) has its justification! For more information on using the databend sqllogictest framework, see [README](https://github.com/datafuselabs/databend/blob/main/tests/sqllogictests/README.md). Some future todos: [sqllogictest tracking](https://github.com/datafuselabs/databend/issues/9174). And finally, thanks to [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) for the support!