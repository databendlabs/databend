---
title: "This Week in Databend #90"
date: 2023-04-22
slug: 2023-04-22-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: drmingdrmer
  - name: everpcpc
  - name: flaneur2020
  - name: jun0315
  - name: kemingy
  - name: lichuang
  - name: neil4dong
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: wubx
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Table Meta Optimization

Databend meta files now default to V3, enhancing metadata serialization/deserialization efficiency & reducing storage space consumption. Witness a 20X size decrease & 2X performance boost!

The format begins with a header that stores the version number of the segment, followed by the encoding format (Bincode) and compression method, both of which are stored in an enumerated manner. The length of the serialized data (blocks and summary) is also included in the header. Finally, the data is serialized in sequence before being compressed and stored.

By using Bincode, the new metadata format is able to serialize data in a compact and efficient manner. This can significantly improve reading speed and reduce the size of stored files. Additionally, by using Zstd compression, the format is able to further reduce file size while maintaining fast decompression speeds.

If you are interested in learning more, please check out the resources listed below.

- [feat(query): table meta optimize](https://github.com/datafuselabs/databend/pull/11015)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Custom Linker Script for Static Linking

When it comes to compiling and linking code, static linking can be useful in certain scenarios, such as when deploying to systems with limited dependencies.

[@dl00](https://github.com/dl00) and [@bossmc](https://github.com/bossmc) suggested creating a custom linker script to address some of challenges and unexpected behaviors.

```bash
#!/bin/bash

args=()

for arg in "$@"; do
    if [[ $arg = *"Bdynamic"* ]]; then
        args+=() # we do not want this arg
    elif [[ $arg = *"crti.o"* ]]; then
        args+=("$arg" "-Bstatic")
    elif [[ $arg = *"crtn.o"* ]]; then
        args+=("-lgcc" "-lgcc_eh" "-lc" "/usr/local/lib/gcc/${MUSL_TARGET}/9.4.0/crtendS.o" "$arg")
    else
        args+=("$arg")
    fi
done

echo "RUNNING WITH ARGS: ${args[@]}"
/usr/local/bin/${MUSL_TARGET}-g++ "${args[@]}"
```

The script, written in bash, removes unnecessary arguments and adds necessary ones for static linking. While this solution may not be perfect, it worked and may be useful for others facing similar issues.

- [ci: fix static link with libstdc++ for musl build](https://github.com/datafuselabs/databend/pull/11145)
- [link: failed to static link to c++ library when global variable is used](https://github.com/rust-lang/rust/issues/36710)

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- [Deploying Databend on Docker](https://databend.rs/doc/deploy/deploying-local#deploying-databend-on-docker)
- Added support for table with cluster key definition for `REPLACE INTO` statement.
- Added window function `percent_rank`.
- Added JSON path functions: `jsonb_path_query_first`,`jsonb_path_query`,`jsonb_path_query_array`.
- Added `SOUNDS LIKE` syntax for String comparing.
- Split log store and state-machine store in Meta Service.
- Lazy materialize according to virtual column `_row_id`.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Aggregate Indices

While approximate queries in the Data Warehouse can reuse cached data to reduce IO overhead, caches have limitations in terms of capacity and timeliness, and cannot solve the issue of incremental calculation. 

To address these challenges, pre-aggregation technology in OLAP is a useful tool for reducing data redundancy. Materialized views are a great way to implement pre-aggregation. Databend plan to introduce aggregate indices as a means of implementing materialized views through secondary indices.

[Issue #11183 | Tracking: aggregating index feature](https://github.com/datafuselabs/databend/pull/11183)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@neil4dong](https://github.com/neil4dong) made their first contribution in [#11043](https://github.com/datafuselabs/databend/pull/11043). The PR adds `SOUNDS LIKE` syntax for String comparing.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.1.2-nightly...v1.1.14-nightly>
