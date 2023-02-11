---
title: 'This Week in Databend #76'
date: 2023-01-11
slug: 2023-01-11-databend-weekly
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

- add reader-min-msg-ver and msg-min-reader-ver in proto-conv ([#9535](https://github.com/datafuselabs/databend/pull/9535))

**Planner**

- support `tuple.1` and `get(1)(tuple)` ([#9493](https://github.com/datafuselabs/databend/pull/9493))
- support display estimated rows in `EXPLAIN` ([#9528](https://github.com/datafuselabs/databend/pull/9528))

**Query**

- efficiently memory two level group by in standalone mode ([#9504](https://github.com/datafuselabs/databend/pull/9504))

**Storage**

- support nested type in `read_parquet` ([#9486](https://github.com/datafuselabs/databend/pull/9486))
- add build options table ([#9502](https://github.com/datafuselabs/databend/pull/9502))

### Code Refactoring :tada:

- merge new expression ([#9411](https://github.com/datafuselabs/databend/pull/9411))
- remove and rename crates ([#9481](https://github.com/datafuselabs/databend/pull/9481))
- bump rust version ([#9540](https://github.com/datafuselabs/databend/pull/9540))

**Expression**

- move negative functions to binder ([#9484](https://github.com/datafuselabs/databend/pull/9484))
- use error_to_null() to eval try_cast ([#9545](https://github.com/datafuselabs/databend/pull/9545))
  
**Functions**

- replace h3ron to h3o ([#9553](https://github.com/datafuselabs/databend/pull/9553))

**Format**

- extract `AligningStateTextBased` ([#9472](https://github.com/datafuselabs/databend/pull/9472))
- richer error context ([#9534](https://github.com/datafuselabs/databend/pull/9534))

**Query**

- use ctx to store the function evaluation error ([#9501](https://github.com/datafuselabs/databend/pull/9501))
- refactor map access to support view read tuple inner ([#9516](https://github.com/datafuselabs/databend/pull/9516))

**Storage**

- bump opendal for streaming read support ([#9503](https://github.com/datafuselabs/databend/pull/9503))
- refactor bloom index to use vectorized siphash function ([#9542](https://github.com/datafuselabs/databend/pull/9542))

### Bug Fixes :wrench:

**HashTable**

- fix memory leak for unsized hash table ([#9551](https://github.com/datafuselabs/databend/pull/9551))

**Storage**

- fix row group stats collection ([#9537](https://github.com/datafuselabs/databend/pull/9537))

## What's On In Databend

Stay connected with the latest news about Databend.

### New Year, New Expression!

We're so thrilled to tell you that Databend now fully works with New Expression after more than a half year of dedicated work. New Expression introduces a *formal type system* to Databend and supports *type-safe downward casting* , making the definition of functions easier.

New Expression is still being tuned, and a new version (v0.9) of Databend will be released once the tuning work is complete.

**Learn More**

- [PR | refactor: Merge new expression](https://github.com/datafuselabs/databend/pull/9411)
- [Issue | Tracking: issues about new expression](https://github.com/datafuselabs/databend/issues/9480)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### `UNNEST` Function

The UNNEST function takes an array as a parameter, and returns a table containing each element of the array in a row.

**Syntax**

```
UNNEST(ARRAY) [WITH OFFSET]
```

If you're interested in becoming a contributor, helping us develop the `UNNEST` function would be a good start.

[Issue 9549: Feature: Support `unnest`](https://github.com/datafuselabs/databend/issues/9549)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

- [v0.8.177-nightly](https://github.com/datafuselabs/databend/releases/tag/v0.8.177-nightly)
- [v0.8.176-patch1](https://github.com/datafuselabs/databend/releases/tag/v0.8.176-patch1)

We're gearing up for the v0.9 release of Databend. Stay tuned.

## Contributors

Thanks a lot to the contributors for their excellent work this week.

[<img alt="andylokandy" src="https://avatars.githubusercontent.com/u/9637710?v=4&s=117" width="117" />](https://github.com/andylokandy) |[<img alt="ariesdevil" src="https://avatars.githubusercontent.com/u/7812909?v=4&s=117" width="117" />](https://github.com/ariesdevil) |[<img alt="b41sh" src="https://avatars.githubusercontent.com/u/1070352?v=4&s=117" width="117" />](https://github.com/b41sh) |[<img alt="BohuTANG" src="https://avatars.githubusercontent.com/u/172204?v=4&s=117" width="117" />](https://github.com/BohuTANG) |[<img alt="ClSlaid" src="https://avatars.githubusercontent.com/u/44747719?v=4&s=117" width="117" />](https://github.com/ClSlaid) |[<img alt="dantengsky" src="https://avatars.githubusercontent.com/u/22081156?v=4&s=117" width="117" />](https://github.com/dantengsky) |
:---: |:---: |:---: |:---: |:---: |:---: |
[andylokandy](https://github.com/andylokandy) |[ariesdevil](https://github.com/ariesdevil) |[b41sh](https://github.com/b41sh) |[BohuTANG](https://github.com/BohuTANG) |[ClSlaid](https://github.com/ClSlaid) |[dantengsky](https://github.com/dantengsky) |

[<img alt="dependabot[bot]" src="https://avatars.githubusercontent.com/in/29110?v=4&s=117" width="117" />](https://github.com/apps/dependabot) |[<img alt="drmingdrmer" src="https://avatars.githubusercontent.com/u/44069?v=4&s=117" width="117" />](https://github.com/drmingdrmer) |[<img alt="everpcpc" src="https://avatars.githubusercontent.com/u/1808802?v=4&s=117" width="117" />](https://github.com/everpcpc) |[<img alt="flaneur2020" src="https://avatars.githubusercontent.com/u/129800?v=4&s=117" width="117" />](https://github.com/flaneur2020) |[<img alt="leiysky" src="https://avatars.githubusercontent.com/u/22445410?v=4&s=117" width="117" />](https://github.com/leiysky) |[<img alt="mergify[bot]" src="https://avatars.githubusercontent.com/in/10562?v=4&s=117" width="117" />](https://github.com/apps/mergify) |
:---: |:---: |:---: |:---: |:---: |:---: |
[dependabot[bot]](https://github.com/apps/dependabot) |[drmingdrmer](https://github.com/drmingdrmer) |[everpcpc](https://github.com/everpcpc) |[flaneur2020](https://github.com/flaneur2020) |[leiysky](https://github.com/leiysky) |[mergify[bot]](https://github.com/apps/mergify) |

[<img alt="PsiACE" src="https://avatars.githubusercontent.com/u/36896360?v=4&s=117" width="117" />](https://github.com/PsiACE) |[<img alt="RinChanNOWWW" src="https://avatars.githubusercontent.com/u/33975039?v=4&s=117" width="117" />](https://github.com/RinChanNOWWW) |[<img alt="soyeric128" src="https://avatars.githubusercontent.com/u/106025534?v=4&s=117" width="117" />](https://github.com/soyeric128) |[<img alt="sundy-li" src="https://avatars.githubusercontent.com/u/3325189?v=4&s=117" width="117" />](https://github.com/sundy-li) |[<img alt="TCeason" src="https://avatars.githubusercontent.com/u/33082201?v=4&s=117" width="117" />](https://github.com/TCeason) |[<img alt="wubx" src="https://avatars.githubusercontent.com/u/320680?v=4&s=117" width="117" />](https://github.com/wubx) |
:---: |:---: |:---: |:---: |:---: |:---: |
[PsiACE](https://github.com/PsiACE) |[RinChanNOWWW](https://github.com/RinChanNOWWW) |[soyeric128](https://github.com/soyeric128) |[sundy-li](https://github.com/sundy-li) |[TCeason](https://github.com/TCeason) |[wubx](https://github.com/wubx) |

[<img alt="Xuanwo" src="https://avatars.githubusercontent.com/u/5351546?v=4&s=117" width="117" />](https://github.com/Xuanwo) |[<img alt="xudong963" src="https://avatars.githubusercontent.com/u/41979257?v=4&s=117" width="117" />](https://github.com/xudong963) |[<img alt="youngsofun" src="https://avatars.githubusercontent.com/u/5782159?v=4&s=117" width="117" />](https://github.com/youngsofun) |[<img alt="zhang2014" src="https://avatars.githubusercontent.com/u/8087042?v=4&s=117" width="117" />](https://github.com/zhang2014) |
:---: |:---: |:---: |:---: |
[Xuanwo](https://github.com/Xuanwo) |[xudong963](https://github.com/xudong963) |[youngsofun](https://github.com/youngsofun) |[zhang2014](https://github.com/zhang2014) |

## Connect With Us

We'd love to hear from you. Feel free to run the code and see if Databend works for you. Submit an issue with your problem if you need help.

[DatafuseLabs Community](https://github.com/datafuselabs/) is open to everyone who loves data warehouses. Please join the community and share your thoughts.

- [Databend Official Website](https://databend.rs)
- [GitHub Discussions](https://github.com/datafuselabs/databend/discussions) (Feature requests, bug reports, and contributions)
- [Twitter](https://twitter.com/Datafuse_Labs) (Stay in the know)
- [Slack Channel](https://link.databend.rs/join-slack) (Chat with the community)
