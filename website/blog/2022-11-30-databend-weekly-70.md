---
title: 'This Week in Databend #70'
date: 2022-11-30
slug: 2022-11-30-databend-weekly
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

- better checking of format options ([#8981](https://github.com/datafuselabs/databend/pull/8981))
- add basic schema infer for parquet ([#9043](https://github.com/datafuselabs/databend/pull/9043))

**Query**

- QualifiedName support 'db.table.*' and 'table.*' ([#8965](https://github.com/datafuselabs/databend/pull/8965))
- support bulk insert without exprssion ([#8966](https://github.com/datafuselabs/databend/pull/8966))

**Storage**

- add cache layer for fuse engine ([#8830](https://github.com/datafuselabs/databend/pull/8830))
- add system table system.memory_statistics ([#8945](https://github.com/datafuselabs/databend/pull/8945))
- add optimize statistic ddl support ([#8891](https://github.com/datafuselabs/databend/pull/8891))

### Code Refactoring :tada:

**Base**

- remove common macros ([#8936](https://github.com/datafuselabs/databend/pull/8936))

**Format**

- TypeDeserializer get rid of FormatSetting ([#8950](https://github.com/datafuselabs/databend/pull/8950))

**Planner**

- refactor extract or predicate ([#8951](https://github.com/datafuselabs/databend/pull/8951))

**Processors**

- optimize join by merging build data block ([#8961](https://github.com/datafuselabs/databend/pull/8961))

**New Expression**

- allow sparse column id in chunk, redo [#8789](https://github.com/datafuselabs/databend/pull/8789) with a new approach. ([#9008](https://github.com/datafuselabs/databend/pull/9008))

### Documentation :notebook_with_decorative_cover:

- i18n support with crowdin ([#8987](https://github.com/datafuselabs/databend/pull/8987), [#8997](https://github.com/datafuselabs/databend/pull/8997), etc.)

### Bug Fixes :wrench:

**Base**

- try fix lost tracker ([#8932](https://github.com/datafuselabs/databend/pull/8932))

**Meta**

- fix share db bug, create DatabaseIdToName if need ([#9006](https://github.com/datafuselabs/databend/pull/9006))

**Mysql handler**

- fix mysql conns leak ([#8894](https://github.com/datafuselabs/databend/pull/8894))

**Processors**

- try fix update list memory leak ([#9023](https://github.com/datafuselabs/databend/pull/9023))

**Storage**

- read and write block in parallel when compact ([#8921](https://github.com/datafuselabs/databend/pull/8921))

## What's On In Databend

Stay connected with the latest news about Databend.

#### Infer Schema at a Glance

You usually need to create a table before loading data from a file stored on a stage or somewhere. Unfortunately, sometimes you might not know the file schema to create the table or are unable to input the schema due to its complexity. 

Introducing the capability to infer schema from an existing file will make the work much easier. You will even be able to query data directly from a stage using a SELECT statement like `select * from @my_stage`.

```sql
INFER 's3://mybucket/data.csv' FILE_FORMAT = ( TYPE = CSV );
+-------------+---------+----------+
| COLUMN_NAME | TYPE    | NULLABLE |
|-------------+---------+----------|
| CONTINENT   | TEXT    | True     |
| COUNTRY     | VARIANT | True     |
+-------------+---------+----------+
```

We've added support for inferring the basic schema from parquet files in [#9043](https://github.com/datafuselabs/databend/pull/9043), and we're now working on [#7211](https://github.com/datafuselabs/databend/issues/7211) to implement `select from @stage`.

**Learn More**

- [PR | add basic schema infer for parquet](https://github.com/datafuselabs/databend/pull/9043)
- [Issue | query data from S3 location or stage](https://github.com/datafuselabs/databend/pull/7211)
- [PR | rfc: Infer Schema](https://github.com/datafuselabs/databend/pull/8645)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

#### Add Tls Support for Mysql Handler

[opensrv-mysql v0.3.0](https://github.com/datafuselabs/opensrv/discussions/35) that was released recently includes support for TLS. It sounds like a good idea to introduce it to Databend.

```rust
let (is_ssl, init_params) = opensrv_mysql::AsyncMysqlIntermediary::init_before_ssl(
    &mut shim,
    &mut r,
    &mut w,
    &Some(tls_config.clone()),
)
.await
.unwrap();

opensrv_mysql::secure_run_with_options(shim, w, ops, tls_config, init_params).await
```

[Issue 8983: Feature: tls support for mysql handler](https://github.com/datafuselabs/databend/issues/8983)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.8.136-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.136-nightly)
- [v0.8.135-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.135-nightly)
- [v0.8.134-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.134-nightly)
- [v0.8.133-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.133-nightly)
- [v0.8.132-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.132-nightly)
- [v0.8.131-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.131-nightly)
- [v0.8.130-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.130-nightly)
- [v0.8.129-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.129-nightly)
- [v0.8.128-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.128-nightly)
- [v0.8.127-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.127-nightly)
- [v0.8.126-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.126-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[ariesdevil](https://github.com/ariesdevil) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[drmingdrmer](https://github.com/drmingdrmer) |

[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="flaneur2020" src="https://avatars.githubusercontent.com/u/129800?v=4&s=117" width="117" />](https://github.com/flaneur2020) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="lichuang" src="https://avatars.githubusercontent.com/u/1998569?v=4&s=117" width="117" />](https://github.com/lichuang) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |
:---: |:---: |:---: |:---: |:---: |:---: |
[everpcpc](https://github.com/everpcpc) |[flaneur2020](https://github.com/flaneur2020) |[leiysky](https://github.com/leiysky) |[lichuang](https://github.com/lichuang) |[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |

[<img alt="sandflee" src="https://avatars.githubusercontent.com/u/5102100?v=4&s=117" width="117" />](https://github.com/sandflee) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) |[<img alt="TracyZYJ" src="https://avatars.githubusercontent.com/u/37072511?v=4&s=117" width="117" />](https://github.com/TracyZYJ) |[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |
:---: |:---: |:---: |:---: |:---: |:---: |
[sandflee](https://github.com/sandflee) |[soyeric128](https://github.com/soyeric128) |[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |[TracyZYJ](https://github.com/TracyZYJ) |[Xuanwo](https://github.com/Xuanwo) |

[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="yufan022" src="https://avatars.githubusercontent.com/u/30121694?v=4&s=117" width="117" />](https://github.com/yufan022) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |[<img alt="zhyass" src="https://avatars.githubusercontent.com/u/34016424?v=4&s=117" width="117" />](https://github.com/zhyass) |
:---: |:---: |:---: |:---: |:---: |
[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[yufan022](https://github.com/yufan022) |[zhang2014](https://github.com/zhang2014) |[zhyass](https://github.com/zhyass) |

## Connect With Us

We'd love to hear from you. Feel free to run the code and see if Databend works for you. Submit an issue with your problem if you need help.

[DatafuseLabs Community](https://github.com/datafuselabs/) is open to everyone who loves data warehouses. Please join the community and share your thoughts.

- [Databend Website](https://databend.rs)
- [GitHub Discussions](https://github.com/datafuselabs/databend/discussions) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/Datafuse_Labs) (Get the news fast)
- [Slack Channel](https://link.databend.rs/join-slack) (For live discussion with the Community)
