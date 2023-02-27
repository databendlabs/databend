---
title: 'This Week in Databend #74'
date: 2022-12-28
slug: 2022-12-28-databend-weekly
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

- remove stream when a watch client is dropped ([#9334](https://github.com/datafuselabs/databend/pull/9334))

**Planner**

- support selectivity estimation for range predicates ([#9398](https://github.com/datafuselabs/databend/pull/9398))

**Query**

- support copy on error ([#9312](https://github.com/datafuselabs/databend/pull/9312))
- support databend-local ([#9282](https://github.com/datafuselabs/databend/pull/9282))
- external storage support location part prefix ([#9381](https://github.com/datafuselabs/databend/pull/9381))

**Storage**

- rangefilter support in ([#9330](https://github.com/datafuselabs/databend/pull/9330))
- try to improve object storage io read ([#9335](https://github.com/datafuselabs/databend/pull/9335))
- support table compression ([#9370](https://github.com/datafuselabs/databend/pull/9370))

**Metrics**

- add more metrics for fuse compact and block write ([#9399](https://github.com/datafuselabs/databend/pull/9399))

**Sqllogictest**

- add no-fail-fast support ([#9391](https://github.com/datafuselabs/databend/pull/9391))

### Code Refactoring :tada:

**\***

- adopt rustls entirely, removing all deps to native-tls ([#9358](https://github.com/datafuselabs/databend/pull/9358))

**Format**

- remove format_xxx settings ([#9360](https://github.com/datafuselabs/databend/pull/9360))
- adjust interface of FileFormatOptionsExt ([#9395](https://github.com/datafuselabs/databend/pull/9395))

**Planner**

- remove `SyncTypeChecker` ([#9352](https://github.com/datafuselabs/databend/pull/9352))

**Query**

- split fuse source to read data and deserialize ([#9353](https://github.com/datafuselabs/databend/pull/9353))
- avoid io copy in read parquet data ([#9365](https://github.com/datafuselabs/databend/pull/9365))
- add uncompressed buffer for parquet reader ([#9379](https://github.com/datafuselabs/databend/pull/9379))

**Storage**

- add read/write settings ([#9359](https://github.com/datafuselabs/databend/pull/9359))

### Bug Fixes :wrench:

**Format**

- fix align_flush with header only ([#9327](https://github.com/datafuselabs/databend/pull/9327))

**Settings**

- use logical CPU number as default value of num_cpus ([#9396](https://github.com/datafuselabs/databend/pull/9396))

**Processors**

- the data type on both sides of the union does not match ([#9361](https://github.com/datafuselabs/databend/pull/9361))

**HTTP Handler**

- false alarm (warning log) about query not exists ([#9380](https://github.com/datafuselabs/databend/pull/9380))


**Sqllogictest**

- refactor sqllogictest http client and fix expression string like ([#9363](https://github.com/datafuselabs/databend/pull/9363))

## What's On In Databend

Stay connected with the latest news about Databend.

#### Introducing databend-local​

Inspired by [clickhouse-local](https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/), databend-local allows you to perform fast processing on local files, without the need of launching a Databend cluster.

```sql
> export CONFIG_FILE=tests/local/config/databend-local.toml
> cargo run --bin=databend-local -- --sql="SELECT * FROM tbl1" --table=tbl1=/path/to/databend/docs/public/data/books.parquet

exec local query: SELECT * FROM tbl1
+------------------------------+---------------------+------+
| title                        | author              | date |
+------------------------------+---------------------+------+
| Transaction Processing       | Jim Gray            | 1992 |
| Readings in Database Systems | Michael Stonebraker | 2004 |
| Transaction Processing       | Jim Gray            | 1992 |
| Readings in Database Systems | Michael Stonebraker | 2004 |
+------------------------------+---------------------+------+
4 rows in set. Query took 0.015 seconds.
```

**Learn More**

- [PR | support databend-local](https://github.com/datafuselabs/databend/pull/9282)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

#### Compressing Short Strings​

When processing the same queries with short strings involved, Databend usually reads more data than other databases, such as Snowflake.

```sql
SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
```

Such queries might be more efficient if short strings (URLs, etc) are compressed.

[Issue 9001: performance: compressing for short strings](https://github.com/datafuselabs/databend/issues/9001)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.8.167-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.167-nightly)
- [v0.8.166-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.166-nightly)
- [v0.8.165-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.165-nightly)
- [v0.8.164-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.164-nightly)
- [v0.8.163-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.163-nightly)
- [v0.8.162-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.162-nightly)
- [v0.8.161-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.161-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |[<img alt="eastfisher" src="https://avatars.githubusercontent.com/u/10803535?v=4&s=117" width="117" />](https://github.com/eastfisher) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[ariesdevil](https://github.com/ariesdevil) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[drmingdrmer](https://github.com/drmingdrmer) |[eastfisher](https://github.com/eastfisher) |

[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |
:---: |:---: |:---: |:---: |:---: |:---: |
[everpcpc](https://github.com/everpcpc) |[leiysky](https://github.com/leiysky) |[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |[RinChanNOWWW](https://github.com/RinChanNOWWW) |[soyeric128](https://github.com/soyeric128) |

[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |[<img alt="zhyass" src="https://avatars.githubusercontent.com/u/34016424?v=4&s=117" width="117" />](https://github.com/zhyass) |
:---: |:---: |:---: |:---: |:---: |:---: |
[sundy-li](https://github.com/sundy-li) |[Xuanwo](https://github.com/Xuanwo) |[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[zhang2014](https://github.com/zhang2014) |[zhyass](https://github.com/zhyass) |

## Connect With Us

We'd love to hear from you. Feel free to run the code and see if Databend works for you. Submit an issue with your problem if you need help.

[DatafuseLabs Community](https://github.com/datafuselabs/) is open to everyone who loves data warehouses. Please join the community and share your thoughts.

- [Databend Official Website](https://databend.rs)
- [GitHub Discussions](https://github.com/datafuselabs/databend/discussions) (Feature requests, bug reports, and contributions)
- [Twitter](https://twitter.com/DatabendLabs) (Stay in the know)
- [Slack Channel](https://link.databend.rs/join-slack) (Chat with the community)
