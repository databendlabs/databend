---
title: 'This Week in Databend #80'
date: 2023-02-08
slug: 2023-02-08-databend-weekly
tags: [databend, weekly]
authors:
- name: PsiACE
  url: https://github.com/psiace
  image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's New

Check out what we've done this week to make Databend even better for you.

### Features & Improvements :sparkles:

**Meta**

- add databend-meta config `grpc_api_advertise_host` ([#9835](https://github.com/datafuselabs/databend/pull/9835))

**AST**

- select from stage with files/pattern ([#9877](https://github.com/datafuselabs/databend/pull/9877))
- parse decimal type ([#9894](https://github.com/datafuselabs/databend/pull/9894))

**Expression**

- add `Decimal128` and `Decimal256` type ([#9856](https://github.com/datafuselabs/databend/pull/9856))

**Functions**

- support `array_indexof` ([#9840](https://github.com/datafuselabs/databend/pull/9840))
- support array function `array_unique`, `array_distinct` ([#9875](https://github.com/datafuselabs/databend/pull/9875))
- support array aggregate functions ([#9903](https://github.com/datafuselabs/databend/pull/9903))

**Query**

- add column id in TableSchema; use column id instead of index when read and write data ([#9623](https://github.com/datafuselabs/databend/pull/9623))
- support view in `system.columns` ([#9853](https://github.com/datafuselabs/databend/pull/9853))

**Storage**

- `ParquetTable` support topk optimization ([#9824](https://github.com/datafuselabs/databend/pull/9824))

**Sqllogictest**

- leverage sqllogictest to benchmark tpch ([#9887](https://github.com/datafuselabs/databend/pull/9887))

### Code Refactoring :tada:

**Meta**

- remove obsolete meta service api `read_msg()` and `write_msg()` ([#9891](https://github.com/datafuselabs/databend/pull/9891))
- simplify `UserAPI` and `RoleAPI` by introducing a method `update_xx_with(id, f: FnOnce)` ([#9921](https://github.com/datafuselabs/databend/pull/9921))

**Cluster**

- split exchange source to reader and deserializer ([#9805](https://github.com/datafuselabs/databend/pull/9805))
- split and eliminate the status for exchange transform and sink ([#9910](https://github.com/datafuselabs/databend/pull/9910))

**Functions**

- rename some array functions add `array_` prefix ([#9886](https://github.com/datafuselabs/databend/pull/9886))

**Query**

- `TableArgs` preserve info of positioned and named args ([#9917](https://github.com/datafuselabs/databend/pull/9917))

**Storage**

- `ParquetTable` list file in `read_partition` ([#9871](https://github.com/datafuselabs/databend/pull/9871))

### Build/Testing/CI Infra Changes :electric_plug:

- support for running benchmark on PRs ([#9788](https://github.com/datafuselabs/databend/pull/9788))

### Bug Fixes :wrench:

**Functions**

- fix nullable and or domain cal ([#9928](https://github.com/datafuselabs/databend/pull/9928))

**Planner**

- fix slow planner when ndv error backtrace ([#9876](https://github.com/datafuselabs/databend/pull/9876))
- fix order by contains aggregation function ([#9879](https://github.com/datafuselabs/databend/pull/9879))
- prevent panic when delete with subquery ([#9902](https://github.com/datafuselabs/databend/pull/9902))

**Query**

- fix insert default value datatype ([#9816](https://github.com/datafuselabs/databend/pull/9816))

## What's On In Databend

Stay connected with the latest news about Databend.

### Why You Should Try Sccache

[Sccache](https://github.com/mozilla/sccache) is a [ccache](https://ccache.dev/)-like project started by the Mozilla team, supporting C/CPP, Rust and other languages, and storing caches locally or in a cloud storage backend. The community first added native support for the Github Action Cache Service to Sccache in version 0.3.3, then improved the functionality in [v0.4.0-pre.6](https://github.com/mozilla/sccache/releases/tag/v0.4.0-pre.6) so that the production CI can now use it.

> Now, [opendal](https://github.com/datafuselabs/opendal), open-sourced by [Datafuse Labs](https://github.com/datafuselabs), acts as a storage access layer for sccache to interface with various storage services (s3/gcs/azlob etc.).

**Learn More**

- [Blog | Why You Should Try Sccache](https://databend.rs/blog/sccache)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Try using `build-info`

To get information about git commits, build options and credits, we now use `vergen` and `cargo-license`.

[build-info](https://github.com/danielschemmel/build-info) can collect build-information of your Rust crate. It might be possible to use it to refactor the relevant logic in `common-building`.

```rust
pub struct BuildInfo {
    pub timestamp: DateTime<Utc>,
    pub profile: String,
    pub optimization_level: OptimizationLevel,
    pub crate_info: CrateInfo,
    pub compiler: CompilerInfo,
    pub version_control: Option<VersionControl>,
}
```

[Issue 9874: Refactor: Try using build-info](https://github.com/datafuselabs/databend/issues/9874)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.9.30-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.30-nightly)
- [v0.9.29-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.29-nightly)
- [v0.9.28-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.28-nightly)
- [v0.9.27-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.27-nightly)
- [v0.9.26-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.26-nightly)
- [v0.9.25-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.25-nightly)
- [v0.9.24-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.24-nightly)
- [v0.9.23-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.23-nightly)
- [v0.9.22-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.22-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dependabot[bot]" src="https://avatars.githubusercontent.com/in/29110?v=4&s=117" width="117" />](https://github.com/apps/dependabot) |[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[ariesdevil](https://github.com/ariesdevil) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[dependabot[bot]](https://github.com/apps/dependabot) |[drmingdrmer](https://github.com/drmingdrmer) |

[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="flaneur2020" src="https://avatars.githubusercontent.com/u/129800?v=4&s=117" width="117" />](https://github.com/flaneur2020) |[<img alt="johnhaxx7" src="https://avatars.githubusercontent.com/u/12479235?v=4&s=117" width="117" />](https://github.com/johnhaxx7) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="lichuang" src="https://avatars.githubusercontent.com/u/1998569?v=4&s=117" width="117" />](https://github.com/lichuang) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |
:---: |:---: |:---: |:---: |:---: |:---: |
[everpcpc](https://github.com/everpcpc) |[flaneur2020](https://github.com/flaneur2020) |[johnhaxx7](https://github.com/johnhaxx7) |[leiysky](https://github.com/leiysky) |[lichuang](https://github.com/lichuang) |[mergify[bot]](https://github.com/apps/mergify) |

[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |
:---: |:---: |:---: |:---: |:---: |:---: |
[PsiACE](https://github.com/PsiACE) |[RinChanNOWWW](https://github.com/RinChanNOWWW) |[soyeric128](https://github.com/soyeric128) |[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |[Xuanwo](https://github.com/Xuanwo) |

[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |
:---: |:---: |:---: |
[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[zhang2014](https://github.com/zhang2014) |
