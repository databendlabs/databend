---
title: "This Week in Databend #86"
date: 2023-03-24
slug: 2023-03-24-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Chasen-Zhang
  - name: dantengsky
  - name: everpcpc
  - name: leiysky
  - name: lichuang
  - name: mergify[bot]
  - name: PsiACE
  - name: RinChanNOWWW
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
  - name: xinlifoobar
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: yufan022
  - name: zhang2014
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### FlightSQL Handler in Progress

Flight SQL is an innovative open database protocol that caters to modern architectures. It boasts a columnar-oriented design and provides seamless support for parallel processing of data partitions.

The benefits of supporting FlightSQL include reducing serialization and deserialization during query execution, as well as easily supporting SDKs in different languages using predefined `*.proto` files.

We're currently engaged in developing support for the FlightSQL Handler. If you're interested, refer to the following links:

- [Issue #10745 | tracking: FlightSQL handler](https://github.com/datafuselabs/databend/issues/10745)
- [PR #10732 | feat: basic impl for FlightSQL handler](https://github.com/datafuselabs/databend/pull/10732).

### Natural Language to SQL

By integrating with the popular AI services, Databend now provide you an efficient built-in solution - the `AI_TO_SQL` function.

With this function, instructions written in natural language can be converted into SQL query statements aligned with table schema. With just a few modifications (or possibly none at all), it can be put into production.

```sql
SELECT * FROM ai_to_sql(
    'List the total amount spent by users from the USA who are older than 30 years, grouped by their names, along with the number of orders they made in 2022',
    '<openai-api-key>');
*************************** 1. row ***************************
     database: openai
generated_sql: SELECT name, SUM(price) AS total_spent, COUNT(order_id) AS total_orders
               FROM users
                        JOIN orders ON users.id = orders.user_id
               WHERE country = 'USA' AND age > 30 AND order_date BETWEEN '2022-01-01' AND '2022-12-31'
               GROUP BY name;
```

The function is now available on both Databend and Databend Cloud. To learn more about how it works, refer to the following links:

- [Doc | AI Functions - AI_TO_SQL](https://databend.rs/doc/sql-functions/ai-functions/ai-to-sql)
- [Blog | Databend Understands You Better with OpenAI](https://databend.rs/blog/ai2sql)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Vector Similarity Calculation in Databend

Databend has added a new function called `cosine_distance`. This function accepts two input vectors, `from` and `to`, which are represented as slices of f32 values.

```sql
select cosine_distance([3.0, 45.0, 7.0, 2.0, 5.0, 20.0, 13.0, 12.0], [2.0, 54.0, 13.0, 15.0, 22.0, 34.0, 50.0, 1.0]) as sim
----
0.8735807
```

The Rust implementation efficiently performs calculations by utilizing the `ArrayView` type from the [ndarray](https://crates.io/crates/ndarray) crate.

```rust
pub fn cosine_distance(from: &[f32], to: &[f32]) -> Result<f32> {
    if from.len() != to.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            from.len(),
            to.len(),
        )));
    }

    let a = ArrayView::from(from);
    let b = ArrayView::from(to);
    let aa_sum = (&a * &a).sum();
    let bb_sum = (&b * &b).sum();

    Ok((&a * &b).sum() / ((aa_sum).sqrt() * (bb_sum).sqrt()))
}
```

Do you remember how to register scalar functions in Databend? You can check [Doc | How to Write a Scalar Function](https://databend.rs/doc/contributing/how-to-write-scalar-functions) and [PR | #10737](https://github.com/datafuselabs/databend/pull/10737) to verify your answer.

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- Learn how to monitor Databend using Prometheus and Grafana: *[Doc | Monitor - Prometheus & Grafana](https://databend.rs/doc/monitor/prometheus-and-granafa)*
- [Metabase Databend Driver](https://github.com/databendcloud/metabase-databend-driver/releases/latest) helps you connect Databend to Metabase and dashboard your data: *[Doc | Integrations - Metabase](https://databend.rs/doc/integrations/gui-tool/metabase)*
- Databend now supports `PIVOT`, `UNPIVOT`, `GROUP BY CUBE` and `GROUP BY ROLLUP` query syntax. For more information, please see PR [#10676](https://github.com/datafuselabs/databend/pull/10676) and [#10601](https://github.com/datafuselabs/databend/pull/10601).

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Enable `-Zgitoxide` to Speed up Git Dependencies Download

Enabling `-Zgitoxide` can speed up the download of our Git dependencies significantly, which is much faster than using Git only.

This feature integrates cargo with [gitoxide](https://github.com/Byron/gitoxide), a pure Rust implementation of Git that is idiomatic, lean, fast, and safe.

[![asciicast](https://asciinema.org/a/542159.svg)](https://asciinema.org/a/542159)

[Issue #10466 | CI: Enable `-Zgitoxide` to speed our git deps download speed](https://github.com/datafuselabs/databend/issues/10466)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@SkyFan2002](https://github.com/SkyFan2002) made their first contribution in [#10656](https://github.com/datafuselabs/databend/pull/10656). This pull request aimed to resolve inconsistent results caused by variations in column name case while executing SQL statements with `EXCLUDE`.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.0.22-nightly...v1.0.33-nightly>
