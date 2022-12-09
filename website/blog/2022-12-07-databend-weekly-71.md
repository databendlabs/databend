---
title: 'This Week in Databend #71'
date: 2022-12-07
slug: 2022-12-07-databend-weekly
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

**Planner**

- optimize topk in cluser mode ([#9092](https://github.com/datafuselabs/databend/pull/9092))

**Query**

- support `select * exclude [column_name | (col_name, col_name,...)]` ([#9009](https://github.com/datafuselabs/databend/pull/9009))
- alter table flashback ([#8967](https://github.com/datafuselabs/databend/pull/8967))
- new table function `read_parquet` to read parquet files as a table ([#9080](https://github.com/datafuselabs/databend/pull/9080))
- support `select * from @stage` ([#9123](https://github.com/datafuselabs/databend/pull/9123))

**Storage**

- cache policy ([#9062](https://github.com/datafuselabs/databend/pull/9062))
- support hive nullable partition ([#9064](https://github.com/datafuselabs/databend/pull/9064))

### Code Refactoring :tada:

**Memory Tracker**

- keep tracker state consistent ([#8973](https://github.com/datafuselabs/databend/pull/8973))

**REST API**

- drop ctx after query finished ([#9091](https://github.com/datafuselabs/databend/pull/9091))

### Bug Fixes :wrench:

**Configs**

- add more tests for hive config loading ([#9074](https://github.com/datafuselabs/databend/pull/9074))

**Planner**

- try to fix table name case sensibility ([#9055](https://github.com/datafuselabs/databend/pull/9055))

**Functions**

- vector_const like bug fix ([#9082](https://github.com/datafuselabs/databend/pull/9082))

**Storage**

- update last_snapshot_hint file when purge ([#9060](https://github.com/datafuselabs/databend/pull/9060))

**Cluster**

- try fix broken pipe or connect reset ([#9104](https://github.com/datafuselabs/databend/pull/9104))

## What's On In Databend

Stay connected with the latest news about Databend.

#### RESTORE TABLE

By the snapshot ID or timestamp you specify in the command, Databend restores the table to a prior state where the snapshot was created. To retrieve snapshot IDs and timestamps of a table, use [FUSE_SNAPSHOT](https://databend.rs/doc/sql-functions/system-functions/fuse_snapshot).


```sql
-- Restore with a snapshot ID
ALTER TABLE <table> FLASHBACK TO (SNAPSHOT => '<snapshot-id>');
-- Restore with a snapshot timestamp
ALTER TABLE <table> FLASHBACK TO (TIMESTAMP => '<timestamp>'::TIMESTAMP);
```

**Learn More**

- [Docs | RESTORE TABLE](https://databend.rs/doc/sql-commands/ddl/table/ddl-restore-table)
- [PR | alter table flashback](https://github.com/datafuselabs/databend/pull/8967)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

#### Adding Build Information to Error Report

An error report currently only contains an error code and some information about why the error occurred. When build information is available, troubleshooting will become easier.

```bash
"Code: xx. Error: error msg... (version ...)"
```

[Issue 9117: Add Build Information to the Error Report](https://github.com/datafuselabs/databend/issues/9117)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.8.144-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.144-nightly)
- [v0.8.143-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.143-nightly)
- [v0.8.142-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.142-nightly)
- [v0.8.141-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.141-nightly)
- [v0.8.140-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.140-nightly)
- [v0.8.139-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.139-nightly)
- [v0.8.138-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.138-nightly)
- [v0.8.137-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.137-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[drmingdrmer](https://github.com/drmingdrmer) |[everpcpc](https://github.com/everpcpc) |

[<img alt="lichuang" src="https://avatars.githubusercontent.com/u/1998569?v=4&s=117" width="117" />](https://github.com/lichuang) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="sandflee" src="https://avatars.githubusercontent.com/u/5102100?v=4&s=117" width="117" />](https://github.com/sandflee) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |
:---: |:---: |:---: |:---: |:---: |:---: |
[lichuang](https://github.com/lichuang) |[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |[RinChanNOWWW](https://github.com/RinChanNOWWW) |[sandflee](https://github.com/sandflee) |[soyeric128](https://github.com/soyeric128) |

[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |
:---: |:---: |:---: |:---: |:---: |:---: |
[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |[Xuanwo](https://github.com/Xuanwo) |[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[zhang2014](https://github.com/zhang2014) |

[<img alt="ZhiHanZ" src="https://avatars.githubusercontent.com/u/25170437?v=4&s=117" width="117" />](https://github.com/ZhiHanZ) |
:---: |
[ZhiHanZ](https://github.com/ZhiHanZ) |

## Connect With Us

We'd love to hear from you. Feel free to run the code and see if Databend works for you. Submit an issue with your problem if you need help.

[DatafuseLabs Community](https://github.com/datafuselabs/) is open to everyone who loves data warehouses. Please join the community and share your thoughts.

- [Databend Official Website](https://databend.rs)
- [GitHub Discussions](https://github.com/datafuselabs/databend/discussions) (Feature requests, bug reports, and contributions)
- [Twitter](https://twitter.com/Datafuse_Labs) (Stay in the know)
- [Slack Channel](https://link.databend.rs/join-slack) (Chat with the community)
