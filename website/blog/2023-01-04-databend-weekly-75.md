---
title: 'This Week in Databend #75'
date: 2023-01-04
slug: 2023-01-04-databend-weekly
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

**Format**

- basic output format JSON ([#9447](https://github.com/datafuselabs/databend/pull/9447))

**Query**

- check connection params ([#9437](https://github.com/datafuselabs/databend/pull/9437))
- add `max_query_row_nums` ([#9406](https://github.com/datafuselabs/databend/pull/9406))

**Storage**

- support prewhere in hive ([#9427](https://github.com/datafuselabs/databend/pull/9427))
- add generic cache trait for different object reader ([#9436](https://github.com/datafuselabs/databend/pull/9436))
- add metrics for new cache ([#9445](https://github.com/datafuselabs/databend/pull/9445))

**New Expression**

- migrate hash func to func-v2 ([#9402](https://github.com/datafuselabs/databend/pull/9402))

**Sqllogictest**

- run all tests in parallel ([#9400](https://github.com/datafuselabs/databend/pull/9400))

### Code Refactoring :tada:

**Storage**

- add `to_bytes` and `from_bytes` for `CachedObject` ([#9439](https://github.com/datafuselabs/databend/pull/9439))
- better table-meta and parquet reader function ([#9434](https://github.com/datafuselabs/databend/pull/9434))
- convert fuse_snapshot unit tests to sqlloigc test ([#9428](https://github.com/datafuselabs/databend/pull/9428))

### Bug Fixes :wrench:

**Format**

- catch unwind when read split ([#9420](https://github.com/datafuselabs/databend/pull/9420))

**User**

- lazy load JWKS ([#9446](https://github.com/datafuselabs/databend/pull/9446))

**Planner**

- create Stage URL's path should ends with `/` ([#9450](https://github.com/datafuselabs/databend/pull/9450))

## What's On In Databend

Stay connected with the latest news about Databend.

#### Databend 2022 Recap

Let's look back and see how Databend did in 2022.

- Open source: got 2,000+ stars, merged 2,400+ PRs, and solved 1,900 issues.
- From data warehouse to lakehouse: Brand-new design with enhanced capabilities.
- Rigorous testing: SQL Logic Tests, SQLancer, and <https://perf.databend.rs>.
- Building the ecosystem: More customers chose, trusted, and grew with Databend, including Kuaishou and SAP.
- Databend Cloud: Built on top of Databend, the next big data analytics platform.

We wish everyone a Happy New Year and look forward to engaging with you.

**Learn More**

- [Blog | Databend 2022 Recap](https://databend.rs/blog/2022-12-31-databend-2022-recap)


### Databend 2023 Roadmap

As the new year approaches, Databend is also actively planning its roadmap for 2023.

We will continue to polish the Planner and work on data and query caching. Enhancing storage and query issues for PB-level data volumes is also on our list.

Try Databend and join the roadmap discussion.

**Learn More**

- [Issue | Roadmap 2023](https://github.com/datafuselabs/databend/issues/9448)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

#### Profile-Guided Optimization (PGO)

The basic concept of PGO is to collect data about the typical execution of a program (e.g. which branches it is likely to take) and then use this data to inform optimizations such as inlining, machine-code layout, register allocation, etc.

`rustc` supports doing profile-guided optimization (PGO). We expect to be able to use it to enhance the build.

[Issue 9387: Feature: Add PGO Support](https://github.com/datafuselabs/databend/issues/9387)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.8.176-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.176-nightly)
- [v0.8.175-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.175-nightly)
- [v0.8.174-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.174-nightly)
- [v0.8.173-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.173-nightly)
- [v0.8.172-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.171-nightly)
- [v0.8.171-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.171-nightly)
- [v0.8.170-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.170-nightly)
- [v0.8.169-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.169-nightly)
- [v0.8.168-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.168-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" />](https://github.com/ariesdevil) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" />](https://github.com/dantengsky) |[<img alt="dependabot[bot]" src="https://avatars.githubusercontent.com/in/29110?v=4&s=117" />](https://github.com/apps/dependabot) |[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" />](https://github.com/everpcpc) |[<img alt="flaneur2020" src="https://avatars.githubusercontent.com/u/129800?v=4&s=117" />](https://github.com/flaneur2020) |
:---: |:---: |:---: |:---: |:---: |:---: |
[ariesdevil](https://github.com/ariesdevil) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[dependabot[bot]](https://github.com/apps/dependabot) |[everpcpc](https://github.com/everpcpc) |[flaneur2020](https://github.com/flaneur2020) |

[<img alt="hantmac" src="https://avatars.githubusercontent.com/u/7600925?v=4&s=117" />](https://github.com/hantmac) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" />](https://github.com/leiysky) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" />](https://github.com/PsiACE) |[<img alt="sandflee" src="https://avatars.githubusercontent.com/u/5102100?v=4&s=117" />](https://github.com/sandflee) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" />](https://github.com/soyeric128) |
:---: |:---: |:---: |:---: |:---: |:---: |
[hantmac](https://github.com/hantmac) |[leiysky](https://github.com/leiysky) |[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |[sandflee](https://github.com/sandflee) |[soyeric128](https://github.com/soyeric128) |

[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" />](https://github.com/TCeason) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" />](https://github.com/Xuanwo) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" />](https://github.com/youngsofun) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" />](https://github.com/zhang2014) |
:---: |:---: |:---: |:---: |:---: |:---: |
[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |[Xuanwo](https://github.com/Xuanwo) |[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[zhang2014](https://github.com/zhang2014) |
