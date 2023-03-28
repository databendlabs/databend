---
title: 'This Week in Databend #79'
date: 2023-02-01
slug: 2023-02-01-databend-weekly
tags: [databend, weekly]
authors:
- name: PsiACE
  url: https://github.com/psiace
  image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a powerful cloud data warehouse. Built for elasticity and efficiency. Free and open. Also available in the cloud: <https://app.databend.com> .

## What's New

Check out what we've done this week to make Databend even better for you.

### Features & Improvements :sparkles:

**AST**

- add syntax about parsing presign options with content type ([#9771](https://github.com/datafuselabs/databend/pull/9771))

**Format**

- add TSV file format back ([#9732](https://github.com/datafuselabs/databend/pull/9732))

**Functions**

- support array functions prepend and append ([#9844](https://github.com/datafuselabs/databend/pull/9844))
- support array concat ([#9804](https://github.com/datafuselabs/databend/pull/9804))

**Query**

- add topn runtime filter in native storage format ([#9738](https://github.com/datafuselabs/databend/pull/9738))
- enable hashtable state pass from partial to final ([#9809](https://github.com/datafuselabs/databend/pull/9809))

**Storage**

- add pruning stats to EXPLAIN ([#9724](https://github.com/datafuselabs/databend/pull/9724))
- cache bloom index object ([#9712](https://github.com/datafuselabs/databend/pull/9712))

### Code Refactoring :tada:

- 'select from stage' use ParquetTable ([#9801](https://github.com/datafuselabs/databend/pull/9801))

**Meta**

- expose a single "kvapi" as public interface ([#9791](https://github.com/datafuselabs/databend/pull/9791))
- do not remove the last node from a cluster ([#9781](https://github.com/datafuselabs/databend/pull/9781))

**AST/Expression/Planner**

- unify Span and Result ([#9713](https://github.com/datafuselabs/databend/pull/9713))

**Executor**

- merge simple pipe and resize pipe ([#9782](https://github.com/datafuselabs/databend/pull/9782))

### Bug Fixes :wrench:

**Base**

- fix not linux and macos jemalloc fallback to std ([#9786](https://github.com/datafuselabs/databend/pull/9786))

**Config**

- fix table_meta_cache can't be disabled ([#9767](https://github.com/datafuselabs/databend/pull/9767))

**Meta**

- when import data to meta-service dir, the specified "id" has to be one of the "initial_cluster" ([#9755](https://github.com/datafuselabs/databend/pull/9755))

**Query**

- fix and refactor aggregator ([#9748](https://github.com/datafuselabs/databend/pull/9748))
- fix memory leak for data port ([#9762](https://github.com/datafuselabs/databend/pull/9762))
- fix panic when cast jsonb to string ([#9813](https://github.com/datafuselabs/databend/pull/9813))

**Storage**

- fix up max_file_size may oom ([#9740](https://github.com/datafuselabs/databend/pull/9740))

## What's On In Databend

Stay connected with the latest news about Databend.

### DML Command - UPDATE

Modifies rows in a table with new values.

> **Note:** Databend guarantees data integrity. In Databend, Insert, Update, and Delete operations are guaranteed to be atomic, which means that all data in the operation must succeed or all must fail.

**Syntax**

```sql
UPDATE <table_name>
SET <col_name> = <value> [ , <col_name> = <value> , ... ]
    [ FROM <table_name> ]
    [ WHERE <condition> ]
```

**Learn More**

- [Docs | DML Command - UPDATE](https://databend.rs/doc/sql-commands/dml/dml-update)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Support Arrow Flight SQL Protocol

Currently Databend supports the MySQL protocol, and it would be great if Databend could support the Arrow Flight SQL protocol as well.

Typically a lakehouse stores data in parquet files using the MySQL protocol while Databend has to do deserialization from parquet to arrow and then back to MySQL data types. Again on the caller end users use data frames or MySQL result iterators, which also requires serialization of types. With Arrow Flight SQL all of these back and forth serialization costs can be avoided.

[Issue 9832: Feature: Support Arrow Flight SQL protocol](https://github.com/datafuselabs/databend/issues/9832)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.9.21-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.21-nightly)
- [v0.9.20-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.20-nightly)
- [v0.9.19-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.19-nightly)
- [v0.9.18-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.18-nightly)
- [v0.9.17-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.17-nightly)
- [v0.9.16-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.16-nightly)
- [v0.9.15-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.15-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |[<img alt="dependabot[bot]" src="https://avatars.githubusercontent.com/in/29110?v=4&s=117" width="117" />](https://github.com/apps/dependabot) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[ariesdevil](https://github.com/ariesdevil) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[dependabot[bot]](https://github.com/apps/dependabot) |

[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="flaneur2020" src="https://avatars.githubusercontent.com/u/129800?v=4&s=117" width="117" />](https://github.com/flaneur2020) |[<img alt="johnhaxx7" src="https://avatars.githubusercontent.com/u/12479235?v=4&s=117" width="117" />](https://github.com/johnhaxx7) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |
:---: |:---: |:---: |:---: |:---: |:---: |
[drmingdrmer](https://github.com/drmingdrmer) |[everpcpc](https://github.com/everpcpc) |[flaneur2020](https://github.com/flaneur2020) |[johnhaxx7](https://github.com/johnhaxx7) |[leiysky](https://github.com/leiysky) |[mergify[bot]](https://github.com/apps/mergify) |

[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |
:---: |:---: |:---: |:---: |:---: |:---: |
[PsiACE](https://github.com/PsiACE) |[RinChanNOWWW](https://github.com/RinChanNOWWW) |[soyeric128](https://github.com/soyeric128) |[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |[Xuanwo](https://github.com/Xuanwo) |

[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="yufan022" src="https://avatars.githubusercontent.com/u/30121694?v=4&s=117" width="117" />](https://github.com/yufan022) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |
:---: |:---: |:---: |
[youngsofun](https://github.com/youngsofun) |[yufan022](https://github.com/yufan022) |[zhang2014](https://github.com/zhang2014) |
