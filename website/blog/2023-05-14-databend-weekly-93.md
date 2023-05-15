---
title: "This Week in Databend #93"
date: 2023-05-14
slug: 2023-05-14-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: DongHaowen
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: hantmac
  - name: lichuang
  - name: Mehrbod2002
  - name: PsiACE
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: ZhiHanZ
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

> The upgrade tool `meta-upgrade-09` will no longer be available in the release package. If you're using Databend 0.9 or an earlier version, you can seek help from the community.

## What's On In Databend

Stay connected with the latest news about Databend.

### Databend's Segment Caching Mechanism Now Boasts Improved Memory Usage

Databend's segment caching mechanism has received a significant upgrade that reduces its memory usage to 1.5/1000 of the previous usage in a test scenario.

The upgrade involves a different "representation" of cached segments, called `CompactSegmentInfo`. This presentation consists mainly of two components:

- The decoded min/max indexes and other statistical information.
- The undecoded (and compressed) raw bytes of block-metas.

During segment pruning, if any segments are pruned, there is no need to decode the block-metas represented by raw bytes. If they are not pruned, then their raw bytes are decoded on-the-fly for block pruning and scanning purposes (and dropped if no longer needed).

If you are interested in learning more, please check out the resources listed below.

- [PR #11347 | refactor: hybrid segment cache](https://github.com/datafuselabs/databend/pull/11347)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Bind `databend` into Python

Databend now offers a Python binding that allows users to execute SQL queries against Databend using Python even without deploying a Databend instance.

To use this functionality, simply import `SessionContext` from `databend` module and create an instance of it:

```python
from databend import SessionContext

ctx = SessionContext()
```

You can then run SQL queries using the `sql()` method on your session context object:

```python
df = ctx.sql("select number, number + 1, number::String as number_p_1 from numbers(8)")
```

The resulting DataFrame can be converted to PyArrow or Pandas format using the `to_py_arrow()` or `to_pandas()` methods respectively:

```python
df.to_pandas() # Or, df.to_py_arrow()
```

Feel free to integrate it with your data science workflow.

- [databend · PyPI](https://pypi.org/project/databend/)

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- Read the two new tutorials added to [Transform Data During Load](https://databend.rs/doc/load-data/data-load-transform) to learn how to perform arithmetic operations during loading and load data into a table with additional columns.
- Read [Working with Stages](https://databend.rs/doc/load-data/stage/whystage) to gain a deeper understanding and learn how to manage and use it effectively.
- Added functions: `date_format`, `str_to_date` and `str_to_timestamp`.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Add `open-sharing` Binary to Databend Image 

**Open Sharing** is a cheap and secure data sharing protocol for databend query on multi-cloud environments. Databend provides a binary called `open-sharing`, which is a tenant-level sharing endpoint. You can read [databend | sharing-endpoint - README.md](https://github.com/datafuselabs/databend/blob/main/src/query/sharing-endpoint/README.md) to learn more information.

To facilitate the deployment of `open-sharing` endpoint instances using K8s or Docker, it is recommended to add it to Databend's docker image.

[Issue #11182 | Feature: added open-sharing binary in the databend-query image](https://github.com/datafuselabs/databend/issues/11182)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

* [@Mehrbod2002](https://github.com/Mehrbod2002) made their first contribution in [#11367](https://github.com/datafuselabs/databend/pull/11367). Added validation for `max_storage_io_requests`.
* [@DongHaowen](https://github.com/DongHaowen) made their first contribution in [#11362](https://github.com/datafuselabs/databend/pull/11362). Specified database in benchmark.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.30-nightly...v1.1.38-nightly>
