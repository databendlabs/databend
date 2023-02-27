---
title: 'This Week in Databend #81'
date: 2023-02-15
slug: 2023-02-15-databend-weekly
tags: [databend, weekly]
authors:
- name: PsiACE
  url: https://github.com/psiace
  image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's New

Check out what we've done this week to make Databend even better for you.

### Accepted RFCs :airplane_departure:

- rfc: query result cache ([#10014](https://github.com/datafuselabs/databend/pull/10014))

### Features & Improvements :sparkles:

**Planner**

- support `EXPLAIN ANALYZE` statement to profile query execution ([#10023](https://github.com/datafuselabs/databend/pull/10023))
- derive new filter and push down ([#10021](https://github.com/datafuselabs/databend/pull/10021))

**Query**

- alter table add/drop column SQL support ([#9851](https://github.com/datafuselabs/databend/pull/9851))
- new table function `infer_schema` ([#9936](https://github.com/datafuselabs/databend/pull/9936))
- add privilege check for select ([#9924](https://github.com/datafuselabs/databend/pull/9924))
- improve signed numeric keys ([#9978](https://github.com/datafuselabs/databend/pull/9978))
- support to parse jwt metadata and add multiple identity issuer configuration ([#9971](https://github.com/datafuselabs/databend/pull/9971))
- support create file format ([#10009](https://github.com/datafuselabs/databend/pull/10009))

**Storage**

- adopt OpenDAL's native scan support ([#9985](https://github.com/datafuselabs/databend/pull/9985))
- add drop_table_by_id api ([#9990](https://github.com/datafuselabs/databend/pull/9990))

**Expression**

- add operation for decimal ([#9926](https://github.com/datafuselabs/databend/pull/9926))

**Functions**

- support `array_any` function ([#9953](https://github.com/datafuselabs/databend/pull/9953))
- support `array_sort` ([#9941](https://github.com/datafuselabs/databend/pull/9941))

**Sqllogictest**

- add time travel test for alter table ([#9939](https://github.com/datafuselabs/databend/pull/9939))

### Code Refactoring :tada:

**Meta**

- move application level types such as user/role/storage-config to crate common-meta/app ([#9944](https://github.com/datafuselabs/databend/pull/9944))
- fix abuse of ErrorCode ([#10056](https://github.com/datafuselabs/databend/pull/10056))

**Query**

- use `transform_sort_merge` use heap to sort blocks ([#10047](https://github.com/datafuselabs/databend/pull/10047))

**Storage**

- introduction of FieldIndex and ColumnId types for clear differentiation of use ([#10017](https://github.com/datafuselabs/databend/pull/10017))

### Build/Testing/CI Infra Changes :electric_plug:

- run benchmark for clickbench result format ([#10019](https://github.com/datafuselabs/databend/pull/10019))
- run benchmark both s3 & fs ([#10050](https://github.com/datafuselabs/databend/pull/10050))

### Bug Fixes :wrench:

**Privilege**

- add privileges on system.one to PUBLIC by default ([#10040](https://github.com/datafuselabs/databend/pull/10040))

**Catalog**

- parts was not distributed evenly ([#9951](https://github.com/datafuselabs/databend/pull/9951))

**Planner**

- type assertion failed on subquery ([#9937](https://github.com/datafuselabs/databend/pull/9937))
- enable outer join to inner join optimization ([#9943](https://github.com/datafuselabs/databend/pull/9943))
- fix limit pushdown outer join ([#10043](https://github.com/datafuselabs/databend/pull/10043))

**Query**

- fix add column update bug ([#10037](https://github.com/datafuselabs/databend/pull/10037))

**Storage**

- fix sub-column of added-tuple column return default 0 bug ([#9973](https://github.com/datafuselabs/databend/pull/9973))
- new bloom filter that bind index with Column Id instead of column name ([#10022](https://github.com/datafuselabs/databend/pull/10022))

## What's On In Databend

Stay connected with the latest news about Databend.

### RFC: Query Result Cache

Caching the results of queries against data that doesn't update frequently can greatly reduce response time. Once cached, the result will be returned in a much shorter time if you run the query again.

- [Docs | RFC: Query Result Cache](https://databend.rs/doc/contributing/rfcs/query-result-cache)
- [Tracking Issue | RFC: query result cache](https://github.com/datafuselabs/databend/issues/10011)

### How to Write a Scalar / Aggregate Function

Did you know that you can enhance the power of Databend by creating your own scalar or aggregate functions? Fortunately, it's not a difficult task!

The following guides are intended for Rust developers and Databend users who want to create their own workflows. The guides provide step-by-step instructions on how to create and register your own functions using Rust, along with code snippets and examples of various types of functions to walk you through the process.

- [Docs | How to Write a Scalar Function](https://databend.rs/doc/contributing/how-to-write-scalar-functions)
- [Docs | How to Write an Aggregate Function](https://databend.rs/doc/contributing/how-to-write-aggregate-functions)

### Profile-Guided Optimization

Profile-guided optimization (PGO) is a compiler optimization technique that collects execution data during the program runtime and allows for tailoring optimizations tailored to both hot and cold code paths.

In this blog, we'll guide you through the process of optimizing Databend binary builds using PGO. We'll use Databend's SQL logic tests as an example to illustrate the step-by-step procedure.

Please note that PGO always requires generating perf data using workloads that are statistically representative. However, there's no guarantee that performance will always improve. Decide whether to use it based on your actual needs.

**Learn More**

- [Blog | Optimizing Databend Binary Builds with Profile-guided Optimization](https://databend.rs/blog/profile-guided-optimization)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Restructure function-related documentation

To make our documentation clearer and easier to understand, we plan to restructure our function-related documentation to follow the same format as DuckDB's documentation. This involves breaking down the task into smaller sub-tasks based on function categories, so that anyone who wants to help improve Databend's documentation can easily get involved.

[Issue 10029: Tracking: re-org the functions doc](https://github.com/datafuselabs/databend/issues/10029)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.9.39-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.39-nightly)
- [v0.9.38-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.38-nightly)
- [v0.9.37-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.37-nightly)
- [v0.9.36-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.36-nightly)
- [v0.9.35-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.35-nightly)
- [v0.9.34-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.34-nightly)
- [v0.9.33-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.33-nightly)
- [v0.9.32-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.32-nightly)
- [v0.9.31-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.31-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="Big-Wuu" src="https://avatars.githubusercontent.com/u/52322009?v=4&s=117" width="117" />](https://github.com/Big-Wuu) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |[<img alt="dependabot[bot]" src="https://avatars.githubusercontent.com/in/29110?v=4&s=117" width="117" />](https://github.com/apps/dependabot) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[b41sh](https://github.com/b41sh) |[Big-Wuu](https://github.com/Big-Wuu) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[dependabot[bot]](https://github.com/apps/dependabot) |

[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="flaneur2020" src="https://avatars.githubusercontent.com/u/129800?v=4&s=117" width="117" />](https://github.com/flaneur2020) |[<img alt="johnhaxx7" src="https://avatars.githubusercontent.com/u/12479235?v=4&s=117" width="117" />](https://github.com/johnhaxx7) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="lichuang" src="https://avatars.githubusercontent.com/u/1998569?v=4&s=117" width="117" />](https://github.com/lichuang) |
:---: |:---: |:---: |:---: |:---: |:---: |
[drmingdrmer](https://github.com/drmingdrmer) |[everpcpc](https://github.com/everpcpc) |[flaneur2020](https://github.com/flaneur2020) |[johnhaxx7](https://github.com/johnhaxx7) |[leiysky](https://github.com/leiysky) |[lichuang](https://github.com/lichuang) |

[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) |
:---: |:---: |:---: |:---: |:---: |:---: |
[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |[RinChanNOWWW](https://github.com/RinChanNOWWW) |[soyeric128](https://github.com/soyeric128) |[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |

[<img alt="wubx" src="https://avatars.githubusercontent.com/u/320680?v=4&s=117" width="117" />](https://github.com/wubx) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="xxchan" src="https://avatars.githubusercontent.com/u/37948597?v=4&s=117" width="117" />](https://github.com/xxchan) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="yufan022" src="https://avatars.githubusercontent.com/u/30121694?v=4&s=117" width="117" />](https://github.com/yufan022) |
:---: |:---: |:---: |:---: |:---: |:---: |
[wubx](https://github.com/wubx) |[Xuanwo](https://github.com/Xuanwo) |[xudong963](https://github.com/xudong963) |[xxchan](https://github.com/xxchan) |[youngsofun](https://github.com/youngsofun) |[yufan022](https://github.com/yufan022) |

[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |[<img alt="ZhiHanZ" src="https://avatars.githubusercontent.com/u/25170437?v=4&s=117" width="117" />](https://github.com/ZhiHanZ) |[<img alt="zhyass" src="https://avatars.githubusercontent.com/u/34016424?v=4&s=117" width="117" />](https://github.com/zhyass) |
:---: |:---: |:---: |
[zhang2014](https://github.com/zhang2014) |[ZhiHanZ](https://github.com/ZhiHanZ) |[zhyass](https://github.com/zhyass) |

## Connect With Us

We'd love to hear from you. Feel free to run the code and see if Databend works for you. Submit an issue with your problem if you need help.

[DatafuseLabs Community](https://github.com/datafuselabs/) is open to everyone who loves data warehouses. Please join the community and share your thoughts.

- [Databend Official Website](https://databend.rs)
- [GitHub Discussions](https://github.com/datafuselabs/databend/discussions) (Feature requests, bug reports, and contributions)
- [Twitter](https://twitter.com/DatabendLabs) (Stay in the know)
- [Slack Channel](https://link.databend.rs/join-slack) (Chat with the community)
