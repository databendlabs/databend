---
title: 'This Week in Databend #78'
date: 2023-01-25
slug: 2023-01-25-databend-weekly
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

**SQL**

- eliminate extra group by scalars ([#9708](https://github.com/datafuselabs/databend/pull/9708))

**Query**

- add privilege check for insert/delete/optimize ([#9664](https://github.com/datafuselabs/databend/pull/9664))
- enable empty projection ([#9675](https://github.com/datafuselabs/databend/pull/9675))
- add aggregate limit in final aggregate stage ([#9716](https://github.com/datafuselabs/databend/pull/9716))
- add optional column names to create/alter view statement ([#9715](https://github.com/datafuselabs/databend/pull/9715))

**Storage**

- add prewhere support in native storage format ([#9600](https://github.com/datafuselabs/databend/pull/9600))

### Code Refactoring :tada:

**IO**

- move io constants to `common/io` ([#9700](https://github.com/datafuselabs/databend/pull/9700))
- refine `fuse/io/read` ([#9711](https://github.com/datafuselabs/databend/pull/9711))

**Planner**

- rename `Scalar` to `ScalarExpr` ([#9665](https://github.com/datafuselabs/databend/pull/9665))

**Storage**

- refactor cache layer ([#9672](https://github.com/datafuselabs/databend/pull/9672))
- `pruner.rs` -> `fuse_bloom_pruner.rs` ([#9710](https://github.com/datafuselabs/databend/pull/9710))
- make pruner hierarchy to chain ([#9714](https://github.com/datafuselabs/databend/pull/9714))

### Build/Testing/CI Infra Changes :electric_plug:

- support setup minio storage & external s3 storage in docker image ([#9676](https://github.com/datafuselabs/databend/pull/9676))

### Bug Fixes :wrench:

**Expression**

- fix missing `simple_cast` ([#9671](https://github.com/datafuselabs/databend/pull/9671))

**Query**

- fix `efficiently_memory_final_aggregator` result is not stable ([#9685](https://github.com/datafuselabs/databend/pull/9685))
- fix `max_result_rows` only limit output results nums ([#9661](https://github.com/datafuselabs/databend/pull/9661))
- fix query hang in two level aggregator ([#9694](https://github.com/datafuselabs/databend/pull/9694))

**Storage**

- may get wrong datablocks if not sorted by output schema ([#9470](https://github.com/datafuselabs/databend/pull/9470))
- bloom filter is using wrong cache key ([#9706](https://github.com/datafuselabs/databend/pull/9706))

## What's On In Databend

Stay connected with the latest news about Databend.

### Databend All-in-One Docker Image

Databend Docker Image now supports setting up MinIO storage and external AWS S3 storage.

Now you can easily use a Docker image for your first experiment with Databend.

**Run with MinIO as backend**

```bash
docker run \
    -p 8000:8000 \
    -p 9000:9000 \
    -e MINIO_ENABLED=true \
    datafuselabs/databend
```

**Run with self managed query config**

```bash
docker run \
    -p 8000:8000 \
    -e DATABEND_QUERY_CONFIG_FILE=/etc/databend/mine.toml \
    -v query_config_file:/etc/databend/mine.toml \
    datafuselabs/databend
```

**Learn More**

- [PR | ci: support setup minio storage & external s3 storage in docker image](https://github.com/datafuselabs/databend/pull/9676)
- [Docker Hub | Databend All-in-One Docker Image](https://hub.docker.com/r/datafuselabs/databend)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Vector Search

Vector search captures the meaning and context of unstructured data, and is commonly used for text or image processing, enabling the use of semantics to find similar results and obtain more valid results than traditional keyword retrieval.

Databend plans to provide users with a richer and more efficient means of querying by supporting vector search, and the introduction of Faiss Index may be an initial solution.

[Issue 9699: feat: vector search (Faiss index)](https://github.com/datafuselabs/databend/issues/9699)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.9.14-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.14-nightly)
- [v0.9.13-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.13-nightly)
- [v0.9.12-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.12-nightly)
- [v0.9.11-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.11-nightly)
- [v0.9.10-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.10-nightly)
- [v0.9.09-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.9-nightly)
- [v0.9.08-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.8-nightly)
- [v0.9.07-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.9.7-nightly)

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |[<img alt="dependabot[bot]" src="https://avatars.githubusercontent.com/in/29110?v=4&s=117" width="117" />](https://github.com/apps/dependabot) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[ariesdevil](https://github.com/ariesdevil) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[dantengsky](https://github.com/dantengsky) |[dependabot[bot]](https://github.com/apps/dependabot) |

[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="flaneur2020" src="https://avatars.githubusercontent.com/u/129800?v=4&s=117" width="117" />](https://github.com/flaneur2020) |[<img alt="johnhaxx7" src="https://avatars.githubusercontent.com/u/12479235?v=4&s=117" width="117" />](https://github.com/johnhaxx7) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |
:---: |:---: |:---: |:---: |:---: |:---: |
[everpcpc](https://github.com/everpcpc) |[flaneur2020](https://github.com/flaneur2020) |[johnhaxx7](https://github.com/johnhaxx7) |[leiysky](https://github.com/leiysky) |[mergify[bot]](https://github.com/apps/mergify) |[PsiACE](https://github.com/PsiACE) |

[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="sandflee" src="https://avatars.githubusercontent.com/u/5102100?v=4&s=117" width="117" />](https://github.com/sandflee) |[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |[<img alt="zhyass" src="https://avatars.githubusercontent.com/u/34016424?v=4&s=117" width="117" />](https://github.com/zhyass) |
:---: |:---: |:---: |:---: |:---: |:---: |
[RinChanNOWWW](https://github.com/RinChanNOWWW) |[sandflee](https://github.com/sandflee) |[sundy-li](https://github.com/sundy-li) |[xudong963](https://github.com/xudong963) |[zhang2014](https://github.com/zhang2014) |[zhyass](https://github.com/zhyass) |

## Connect With Us

We'd love to hear from you. Feel free to run the code and see if Databend works for you. Submit an issue with your problem if you need help.

[DatafuseLabs Community](https://github.com/datafuselabs/) is open to everyone who loves data warehouses. Please join the community and share your thoughts.

- [Databend Official Website](https://databend.rs)
- [GitHub Discussions](https://github.com/datafuselabs/databend/discussions) (Feature requests, bug reports, and contributions)
- [Twitter](https://twitter.com/DatabendLabs) (Stay in the know)
- [Slack Channel](https://link.databend.rs/join-slack) (Chat with the community)
