---
title: 'This Week in Databend #77'
date: 2023-01-18
slug: 2023-01-18-databend-weekly
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

**Meta**

- use `expression::TableSchema` to replace obsolete `datavalues::DataSchema` ([#9506](https://github.com/datafuselabs/databend/pull/9506))
- `iter()` iterate every tree and every records in theses trees ([#9621](https://github.com/datafuselabs/databend/pull/9621))

**Expression**

- add other base geo functions ([#9588](https://github.com/datafuselabs/databend/pull/9588))

**Optimizer**

- improve cardinality estimation for join based on histogram ([#9594](https://github.com/datafuselabs/databend/pull/9594))

**Planner**

- improve join reorder algorithm ([#9571](https://github.com/datafuselabs/databend/pull/9571))

**Query**

- support insert with placeholder ([#9575](https://github.com/datafuselabs/databend/pull/9575))
- set setting support expr ([#9574](https://github.com/datafuselabs/databend/pull/9574))
- add information_schema for sharding-jdbc ([#9583](https://github.com/datafuselabs/databend/pull/9583))
- support named params for table functions ([#9630](https://github.com/datafuselabs/databend/pull/9630))

**Storage**

- read_parquet page index ([#9563](https://github.com/datafuselabs/databend/pull/9563))
- update interpreter and storage support ([#9261](https://github.com/datafuselabs/databend/pull/9261))

### Code Refactoring :tada:

- refine on_error mode ([#9473](https://github.com/datafuselabs/databend/pull/9473))

**Meta**

- remove unused meta types and conversion util ([#9584](https://github.com/datafuselabs/databend/pull/9584))

**Parser**

- more strict parser for format_options ([#9635](https://github.com/datafuselabs/databend/pull/9635))

**Expression**

- rearrange common_expression and common_function ([#9585](https://github.com/datafuselabs/databend/pull/9585))

### Build/Testing/CI Infra Changes :electric_plug:

- run sqllogictests with binary ([#9603](https://github.com/datafuselabs/databend/pull/9603))

### Bug Fixes :wrench:

**Expression**

- constant folder should run repeatedly until stable ([#9572](https://github.com/datafuselabs/databend/pull/9572))
- `check_date()` and `to_string(boolean)` may panic ([#9561](https://github.com/datafuselabs/databend/pull/9561))

**Planner**

- fix stack overflow when applying RuleFilterPushDownJoin ([#9645](https://github.com/datafuselabs/databend/pull/9645))

**Storage**

- fix range filter read stat with index ([#9619](https://github.com/datafuselabs/databend/pull/9619))

**Sqllogictest**

- sqllogic test hangs (cluster mod + clickhouse handler) ([#9615](https://github.com/datafuselabs/databend/pull/9615))

## What's On In Databend

Stay connected with the latest news about Databend.

### Upgrade Databend Query from 0.8 to 0.9

Databend-query-0.9 introduces incompatible changes in metadata, these metadata has to be manually migrated.
Databend provides a program for this job: `databend-meta-upgrade-09`, which you can find in a [release package](https://github.com/datafuselabs/databend/releases) or can be built from source.

**Upgrade**

```bash
databend-meta-upgrade-09 --cmd upgrade --raft-dir "<./your/raft-dir/>"
```

**Learn More**

- [PR | feat(meta/upgrade): meta data upgrade program](https://github.com/datafuselabs/databend/pull/9489)
- [Doc | Upgrade Databend Query from 0.8 to 0.9](https://databend.rs/doc/deploy/upgrade/upgrade-0.8-to-0.9)

### Release Proposal: Nightly v1.0

The call for proposals for the release of v1.0 is now open.

The preliminary plan is to release in March, mainly focusing on `alter` table, `update`, and `group by spill`.

**Learn More**

- [Release proposal: Nightly v1.0](https://github.com/datafuselabs/databend/issues/9604)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Add Type Checker for Sqllogictest

We can check if each row's each element's type is correct.

**databend/tests/sqllogictests/src/client/mysql_client.rs**

```rust
 // Todo: add types to compare
 Ok(DBOutput::Rows {
     types,
     rows: parsed_rows,
```

[Issue 9647: Feature: Add type checker for sqllogictest](https://github.com/datafuselabs/databend/issues/9647)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.9.6-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.6-nightly)
- [v0.9.5-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.5-nightly)
- [v0.9.4-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.4-nightly)
- [v0.9.3-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.3-nightly)
- [v0.9.2-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.2-nightly)
- [v0.9.1-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.1-nightly)
- [v0.9.0-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.0-nightly)

We're gearing up for the v0.9 release of Databend. Stay tuned.

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[ariesdevil](https://github.com/ariesdevil) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[drmingdrmer](https://github.com/drmingdrmer) |

[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |
:---: |:---: |:---: |:---: |:---: |:---: |
[everpcpc](https://github.com/everpcpc) |[leiysky](https://github.com/leiysky) |[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |[RinChanNOWWW](https://github.com/RinChanNOWWW) |[soyeric128](https://github.com/soyeric128) |

[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="yufan022" src="https://avatars.githubusercontent.com/u/30121694?v=4&s=117" width="117" />](https://github.com/yufan022) |
:---: |:---: |:---: |:---: |:---: |:---: |
[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |[Xuanwo](https://github.com/Xuanwo) |[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[yufan022](https://github.com/yufan022) |

[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |[<img alt="zhyass" src="https://avatars.githubusercontent.com/u/34016424?v=4&s=117" width="117" />](https://github.com/zhyass) |
:---: |:---: |
[zhang2014](https://github.com/zhang2014) |[zhyass](https://github.com/zhyass) |
