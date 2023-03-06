---
title: "This Week in Databend #82"
date: 2023-02-22
slug: 2023-02-22-databend-weekly
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

**AST**

- select from stage support uri with connection options ([#10066](https://github.com/datafuselabs/databend/pull/10066))

**Catalog**

- Iceberg/create-catalog ([#9017](https://github.com/datafuselabs/databend/pull/9017))

**Expression**

- type decimal support agg func min/max ([#10085](https://github.com/datafuselabs/databend/pull/10085))
- add sum/avg for decimal types ([#10059](https://github.com/datafuselabs/databend/pull/10059))

**Pipeline**

- enrich core pipelines processors ([#10098](https://github.com/datafuselabs/databend/pull/10098))

**Query**

- create stage, select stage, copy, infer_schema support named file format ([#10084](https://github.com/datafuselabs/databend/pull/10084))
- query result cache ([#10042](https://github.com/datafuselabs/databend/pull/10042))

**Storage**

- table data cache ([#9772](https://github.com/datafuselabs/databend/pull/9772))
- use `drop_table_by_id` api in `drop all` ([#10054](https://github.com/datafuselabs/databend/pull/10054))
- native storage format support nested data types ([#9798](https://github.com/datafuselabs/databend/pull/9798))

### Code Refactoring :tada:

**Meta**

- add compatible layer for upgrade ([#10082](https://github.com/datafuselabs/databend/pull/10082))
- More elegant error handling ([#10112](https://github.com/datafuselabs/databend/pull/10112), [#10114](https://github.com/datafuselabs/databend/pull/10114), etc.)

**Cluster**

- support exchange sorting ([#10149](https://github.com/datafuselabs/databend/pull/10149))

**Executor**

- add check processor graph completed ([#10166](https://github.com/datafuselabs/databend/pull/10166))

**Planner**

- apply constant folder at physical plan builder ([#9889](https://github.com/datafuselabs/databend/pull/9889))

**Query**

- use accumulating to impl single state aggregator ([#10125](https://github.com/datafuselabs/databend/pull/10125))

**Storage**

- adopt OpenDAL's batch delete support ([#10150](https://github.com/datafuselabs/databend/pull/10150))
- adopt OpenDAL query based metadata cache ([#10162](https://github.com/datafuselabs/databend/pull/10162))

### Build/Testing/CI Infra Changes :electric_plug:

- release deb repository ([#10080](https://github.com/datafuselabs/databend/pull/10080))
- release with systemd units ([#10145](https://github.com/datafuselabs/databend/pull/10145))

### Bug Fixes :wrench:

**Expression**

- no longer return Variant as common super type ([#9961](https://github.com/datafuselabs/databend/pull/9961))
- allow auto cast from string and variant ([#10111](https://github.com/datafuselabs/databend/pull/10111))

**Cluster**

- fix limit query hang in cluster mode ([#10006](https://github.com/datafuselabs/databend/pull/10006))

**Storage**

- wrong column statistics when contain tuple type ([#10068](https://github.com/datafuselabs/databend/pull/10068))
- compact not work as expected with add column ([#10070](https://github.com/datafuselabs/databend/pull/10070))
- fix add column min/max stat bug ([#10137](https://github.com/datafuselabs/databend/pull/10137))

## What's On In Databend

Stay connected with the latest news about Databend.

### Query Result Cache

In the past week, Databend now supports caching of query results!

```
             ┌─────────┐ 1  ┌─────────┐ 1
             │         ├───►│         ├───►Dummy───►Downstream
Upstream────►│Duplicate│ 2  │         │ 3
             │         ├───►│         ├───►Dummy───►Downstream
             └─────────┘    │         │
                            │ Shuffle │
             ┌─────────┐ 3  │         │ 2  ┌─────────┐
             │         ├───►│         ├───►│  Write  │
Upstream────►│Duplicate│ 4  │         │ 4  │ Result  │
             │         ├───►│         ├───►│  Cache  │
             └─────────┘    └─────────┘    └─────────┘

```

**Learn More**

- [PR | feat(query): query result cache](https://github.com/datafuselabs/databend/pull/10042)
- [Docs | RFC: Query Result Cache](https://databend.rs/doc/contributing/rfcs/query-result-cache)
- [Tracking Issue | RFC: query result cache](https://github.com/datafuselabs/databend/issues/10011)

### Table Data Cache

Databend now supports table data cache:

- disk cache: raw column(compressed) data of the data block.
- in-memory cache(experimental): deserialized column objects of a data block.

For cache-friendly workloads, the performance gains are significant.

**Learn More**

- [PR | feat: table data cache](https://github.com/datafuselabs/databend/pull/9772)
- [Docs | Query Server Configuration - Cache](https://databend.rs/doc/deploy/query/query-config#5-cache)

### Deb Source & Systemd Support

Databend now offers the official Deb package source and supports the use of `systemd` to manage the service.

For DEB822 Source Format:

```bash
sudo curl -L -o /etc/apt/sources.list.d/datafuselabs.sources https://repo.databend.rs/deb/datafuselabs.sources
sudo apt update
sudo apt install databend
sudo systemctl start databend-meta
sudo systemctl start databend-query
```

**Learn More**

- [PR | chore(ci): release with systemd units](https://github.com/datafuselabs/databend/pull/10145)
- [Docs | Installing Databend - Package Manager](https://databend.rs/doc/deploy/installing-databend#package-manager)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Service Activation Progress Report

When starting a Query/Meta node, it is necessary to perform checks and output them explicitly to help the user diagnose faults and confirm status.

**Example:**

```bash
storage check succeed
meta check failed: timeout, no response. endpoints: xxxxxxxx .
status check failed: address already in use.
```

[Issue 10193: Feature: output the necessary progress when starting a query/meta node](https://github.com/datafuselabs/databend/issues/10193)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.9.47-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.47-nightly)
- [v0.9.46-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.46-nightly)
- [v0.9.45-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.45-nightly)
- [v0.9.44-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.44-nightly)
- [v0.9.43-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.43-nightly)
- [v0.9.42-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.42-nightly)
- [v0.9.41-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.41-nightly)
- [v0.9.40-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.40-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

| [<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) | [<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) | [<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) | [<img alt="Big-Wuu" src="https://avatars.githubusercontent.com/u/52322009?v=4&s=117" width="117" />](https://github.com/Big-Wuu) | [<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) | [<img alt="cameronbraid" src="https://avatars.githubusercontent.com/u/672405?v=4&s=117" width="117" />](https://github.com/cameronbraid) |
| :-------------------------------------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------------------: | :--------------------------------------------------------------------------------------------------------------------------------------: |
|                                              [andylokandy](https://github.com/andylokandy)                                              |                                              [ariesdevil](https://github.com/ariesdevil)                                              |                                              [b41sh](https://github.com/b41sh)                                              |                                              [Big-Wuu](https://github.com/Big-Wuu)                                               |                                             [BohuTANG](https://github.com/BohuTANG)                                              |                                             [cameronbraid](https://github.com/cameronbraid)                                              |

| [<img alt="Chasen-Zhang" src="https://avatars.githubusercontent.com/u/15354455?v=4&s=117" width="117" />](https://github.com/Chasen-Zhang) | [<img alt="ClSlaid" src="https://avatars.githubusercontent.com/u/44747719?v=4&s=117" width="117" />](https://github.com/ClSlaid) | [<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) | [<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) | [<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) | [<img alt="johnhaxx7" src="https://avatars.githubusercontent.com/u/12479235?v=4&s=117" width="117" />](https://github.com/johnhaxx7) |
| :----------------------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------: |
|                                              [Chasen-Zhang](https://github.com/Chasen-Zhang)                                               |                                              [ClSlaid](https://github.com/ClSlaid)                                               |                                              [dantengsky](https://github.com/dantengsky)                                               |                                             [drmingdrmer](https://github.com/drmingdrmer)                                             |                                              [everpcpc](https://github.com/everpcpc)                                              |                                              [johnhaxx7](https://github.com/johnhaxx7)                                               |

| [<img alt="lichuang" src="https://avatars.githubusercontent.com/u/1998569?v=4&s=117" width="117" />](https://github.com/lichuang) | [<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) | [<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) | [<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) | [<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) | [<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |
| :-------------------------------------------------------------------------------------------------------------------------------: | :--------------------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------------: |
|                                              [lichuang](https://github.com/lichuang)                                              |                                             [mergify[bot]](https://github.com/apps/mergify)                                              |                                              [PsiACE](https://github.com/PsiACE)                                               |                                              [RinChanNOWWW](https://github.com/RinChanNOWWW)                                               |                                               [soyeric128](https://github.com/soyeric128)                                               |                                              [sundy-li](https://github.com/sundy-li)                                              |

| [<img alt="suyanhanx" src="https://avatars.githubusercontent.com/u/24221472?v=4&s=117" width="117" />](https://github.com/suyanhanx) | [<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) | [<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) | [<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) | [<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) | [<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |
| :----------------------------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------------------------------------------------: | :----------------------------------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------------------------------------------------------: |
|                                              [suyanhanx](https://github.com/suyanhanx)                                               |                                              [TCeason](https://github.com/TCeason)                                               |                                              [Xuanwo](https://github.com/Xuanwo)                                              |                                              [xudong963](https://github.com/xudong963)                                               |                                              [youngsofun](https://github.com/youngsofun)                                              |                                              [zhang2014](https://github.com/zhang2014)                                              |

| [<img alt="zhyass" src="https://avatars.githubusercontent.com/u/34016424?v=4&s=117" width="117" />](https://github.com/zhyass) |
| :----------------------------------------------------------------------------------------------------------------------------: |
|                                              [zhyass](https://github.com/zhyass)                                               |
