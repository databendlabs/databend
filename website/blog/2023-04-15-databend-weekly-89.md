---
title: "This Week in Databend #89"
date: 2023-04-15
slug: 2023-04-15-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: dependabot[bot]
  - name: Dousir9
  - name: everpcpc
  - name: flaneur2020
  - name: leiysky
  - name: lichuang
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

### Announcing Databend v1.1

Databend v1.1.0 was officially released on April 14, 2023! This release marks the first major update of Databend since version 1.0.

The new version has many new features, including:

- `COPY INTO` now has ETL capabilities
- Full Support for TPC-DS Queries
- `REPLACE INTO`
- Window Functions
- Integration with Metabase, Redash and Grafana
- Rust driver
- AI Functions
- AskBend - knowledge base Q&A system

If you are interested in learning more, please check out the resources listed below.

- [What's Fresh in Databend v1.1 | Blog | Databend](https://databend.rs/blog/databend-release-v1.1)

### New AI-related Configurations

Databend has added new AI-related configurations that allow for the use of different endpoints and models.

This means that it can be easily integrated with other services including Azure OpenAI API. Additionally, the corresponding model can be flexibly configured as needed to balance performance and cost.

```text
| "query"   | "openai_api_base_url"                      | "https://api.openai.com/v1/"     | ""       |
| "query"   | "openai_api_completion_model"              | "text-embedding-ada-002"         | ""       |
| "query"   | "openai_api_embedding_model"               | "gpt-3.5-turbo"                  | ""       |
```

If you are interested in learning more, please check out the resources listed below.

- [PR | add openai api_base_url, completion_model and embedding_model to query config](https://github.com/datafuselabs/databend/pull/10993)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### bendsql - Databend Native Command Line Tool

We previously introduced bendsql, which is the Databend native client written in Go. Now it is being rewritten in Rust and is currently in the experimental stage.

The Rust version not only provides support for REST API but also supports Flight SQL.

```sql
bendsql> select avg(number) from numbers(10);

SELECT
  avg(number)
FROM
  numbers(10);

┌───────────────────┐
│    avg(number)    │
│ Nullable(Float64) │
├───────────────────┤
│ 4.5               │
└───────────────────┘

1 row in 0.259 sec. Processed 10 rows, 10B (38.59 rows/s, 308B/s)
```

We are excited about the progress of the Rust version and look forward to sharing more updates with you soon! Feel free to try it out and give us feedback. For more information, follow the resources listed below.

- [Crates.io - bendsql](https://crates.io/crates/bendsql)
- [GitHub - databendcloud/bendsql | Go](https://github.com/databendcloud/bendsql)
- [Github - datafuselabs/databend-client | Rust](https://github.com/datafuselabs/databend-client)

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- Databend leverages a RBAC model to secure your data. Read [Docs | Access Control Privileges](https://databend.rs/doc/sql-reference/access-control-privileges) to learn about Global privileges and Object-specific privileges in Databend.
- Add the `array_aggregate` function and support more array aggregate functions, including `stddev_samp`, `stddev_pop`, `stddev`, `std`, `median`, `approx_count_distinct`, `kurtosis`, and `skewness`.
- Add the system table `system.caches` to describe the cache status in Databend.
- Databend recently introduced [z3.rs](https://github.com/prove-rs/z3.rs) to solve constraints. [Z3](https://github.com/Z3Prover/z3) is a theorem prover from Microsoft Research that is usually used to solve SMT problems.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Release proposal: Nightly v1.2

Databend v1.2 is scheduled for release on May 15th and includes the following features:

| Task                                                                                             | Status      | Comments      |
| ------------------------------------------------------------------------------------------------ | ----------- | ------------- | --- |
| (Query) [JSON indexing#6994](https://github.com/datafuselabs/databend/issues/6994)               | IN PROGRESS | high-priority |
| (Query) [Fulltext index#3915](https://github.com/datafuselabs/databend/issues/3915)              | IN PROGRESS | high-priority |
| (Query+Storage) [Merge Into#10174](https://github.com/datafuselabs/databend/issues/10174)        | PLAN        | high-priority |
| (Query)[Window function#10810](https://github.com/datafuselabs/databend/issues/10810)            | IN PROGRESS |               |
| (Query)[Distributed COPY#8594](https://github.com/datafuselabs/databend/issues/8594)             | PLAN        |               |
| (Query)Lambda function and high-order functions                                                  | PLAN        |               |     |
| (Storage) Fuse engine re-clustering                                                              | PLAN        | high-priority |
| (Storage) Fuse engine segment tree                                                               | IN PROGRESS | high-priority |
| (Storage) Fuse engine orphan data cleanup                                                        | IN PROGRESS | high-priority |
| (Storage) [Compress Fuse meta data#10265](https://github.com/datafuselabs/databend/issues/10265) | IN PROGRESS | high-priority |
| [Embedding Model #10689](https://github.com/datafuselabs/databend/issues/10689)                  | IN PROGRESS | high-priority |
| Search optimization SQL framework                                                                 | IN PROGRESS |               |
| Distributed group by with dictionary                                                              | IN PROGRESS |               |

Please note that this is a release plan and may be subject to change. We encourage community members to participate in the discussion and provide feedback on the plan.

[Issue #11073 | Release proposal: Nightly v1.2](https://github.com/datafuselabs/databend/issues/11073)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.0.57-nightly...v1.1.2-nightly>
