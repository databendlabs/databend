---
title: 'This Week in Databend #73'
date: 2022-12-21
slug: 2022-12-21-databend-weekly
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

**Multiple Catalogs**

- implement show tables (from|in catalog.database) ([#9153](https://github.com/datafuselabs/databend/pull/9153))

**Planner**

- introduce histogram in column statistics ([#9310](https://github.com/datafuselabs/databend/pull/9310))

**Query**

- support attaching stage for insert values ([#9249](https://github.com/datafuselabs/databend/pull/9249))
- add native format in fuse table ([#9279](https://github.com/datafuselabs/databend/pull/9279))
- add internal_enable_sandbox_tenant config and sandbox_tenant ([#9277](https://github.com/datafuselabs/databend/pull/9277))

**Sqllogictest**

- introduce rust native sqllogictest framework ([#9150](https://github.com/datafuselabs/databend/pull/9150))

### Code Refactoring :tada:

**\***

- unify apply_file_format_options for copy & insert ([#9323](https://github.com/datafuselabs/databend/pull/9323))

**IO**

- remove unused code ([#9266](https://github.com/datafuselabs/databend/pull/9266))

**meta**

- test watcher count ([#9324](https://github.com/datafuselabs/databend/pull/9324))

**Planner**

- replace TableContext in planner with PlannerContext ([#9290](https://github.com/datafuselabs/databend/pull/9290))

### Bug Fixes :wrench:

**Base**

- try fix SIGABRT when catch unwind ([#9269](https://github.com/datafuselabs/databend/pull/9269))
- replace #[thread_local] to thread_local macro ([#9280](https://github.com/datafuselabs/databend/pull/9280))

**Query**

- fix unknown database in query without relation to this database ([#9250](https://github.com/datafuselabs/databend/pull/9250))
- fix wrong current_role when drop the role ([#9276](https://github.com/datafuselabs/databend/pull/9276))

## What's On In Databend

Stay connected with the latest news about Databend.

#### Introduced a Rust Native Sqllogictest Framework

Sqllogictest verifies the results returned from a SQL database engine by comparing them with the results of other engines for the same queries.

In the past, Databend ran such tests using a program written in Python and migrated a large number of test cases from other popular databases. We implemented the program again with [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) in recent days.

**Learn More**

- [Doc | sqllogictest](https://github.com/datafuselabs/databend/tree/main/tests/sqllogictests)
- [PR | introduce rust native sqllogictest framework](https://github.com/datafuselabs/databend/pull/9150)

#### Experimental: Native Format

[PA](https://github.com/sundy-li/pa) is a native storage format based on Apache Arrow. Similar to [Arrow IPC](https://arrow.apache.org/docs/python/ipc.html), PA aims at optimizing the storage layer.

Databend is introducing PA as a native storage format in the hope of getting a performance boost, though it's still at an early stage of development.

```sql
create table tmp (a int) ENGINE=FUSE STORAGE_FORMAT='native';
```

**Learn More**

- [PR | add native format in fuse table](https://github.com/datafuselabs/databend/pull/9279)
- [GitHub | PA - A native storage format based on Apache Arrow](https://github.com/sundy-li/pa)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

#### Checking File Existence Before Returning Presigned URL​

When presigning a file, Databend currently returns a potentially valid URL based on the filename without checking if the file really exists. Thus, the 404 error might occur if the file doesn't exist at all.

[Issue 8702: Before return presign url add file exist judgement](https://github.com/datafuselabs/databend/issues/8702)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.8.160-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.160-nightly)
- [v0.8.159-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.159-nightly)
- [v0.8.158-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.158-nightly)
- [v0.8.157-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.157-nightly)
- [v0.8.156-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.156-nightly)
- [v0.8.155-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.155-nightly)
- [v0.8.154-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.154-nightly)
- [v0.8.153-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.153-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="ClSlaid" src="https://avatars.githubusercontent.com/u/44747719?v=4&s=117" width="117" />](https://github.com/ClSlaid) |[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |
:---: |:---: |:---: |:---: |:---: |:---: |
[ariesdevil](https://github.com/ariesdevil) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[ClSlaid](https://github.com/ClSlaid) |[drmingdrmer](https://github.com/drmingdrmer) |[everpcpc](https://github.com/everpcpc) |

[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="sandflee" src="https://avatars.githubusercontent.com/u/5102100?v=4&s=117" width="117" />](https://github.com/sandflee) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |
:---: |:---: |:---: |:---: |:---: |:---: |
[leiysky](https://github.com/leiysky) |[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |[sandflee](https://github.com/sandflee) |[soyeric128](https://github.com/soyeric128) |[sundy-li](https://github.com/sundy-li) |

[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |[<img alt="ZhiHanZ" src="https://avatars.githubusercontent.com/u/25170437?v=4&s=117" width="117" />](https://github.com/ZhiHanZ) |[<img alt="zhyass" src="https://avatars.githubusercontent.com/u/34016424?v=4&s=117" width="117" />](https://github.com/zhyass) |
:---: |:---: |:---: |:---: |:---: |:---: |
[Xuanwo](https://github.com/Xuanwo) |[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[zhang2014](https://github.com/zhang2014) |[ZhiHanZ](https://github.com/ZhiHanZ) |[zhyass](https://github.com/zhyass) |

## Connect With Us

We'd love to hear from you. Feel free to run the code and see if Databend works for you. Submit an issue with your problem if you need help.

[DatafuseLabs Community](https://github.com/datafuselabs/) is open to everyone who loves data warehouses. Please join the community and share your thoughts.

- [Databend Official Website](https://databend.rs)
- [GitHub Discussions](https://github.com/datafuselabs/databend/discussions) (Feature requests, bug reports, and contributions)
- [Twitter](https://twitter.com/DatabendLabs) (Stay in the know)
- [Slack Channel](https://link.databend.rs/join-slack) (Chat with the community)
