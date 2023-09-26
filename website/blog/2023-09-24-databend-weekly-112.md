---
title: "This Week in Databend #112"
date: 2023-09-24
slug: 2023-09-24-databend-weekly
cover_url: 'weekly/weekly-112.jpg'
image: 'weekly/weekly-112.jpg'
tags: [weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: akoshchiy
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTang
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: flaneur2020
  - name: gitccl
  - name: JackTan25
  - name: leiysky
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: xudong963
  - name: youngsofun
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

### Understanding User-Defined Function

User-Defined Functions (UDFs) enable you to define their own custom operations to process data within Databend, enabling better data handling, task execution, and the creation of more efficient data workflows.

User-Defined Functions (UDFs) are typically written using lambda expressions or implemented via a UDF server with programming languages such as Python and are executed as part of Databend's query processing pipeline. Advantages of using UDFs include:

- Customized Data Transformations.
- Performance Optimization.
- Code Reusability.

If you are interested in learning more, please check out the resources listed below.

- [Docs | User-Defined Function](https://databend.rs/doc/sql-commands/ddl/udf)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Refactoring Databend Metrics Component with Prometheus

[metrics-rs](https://github.com/metrics-rs/metrics) is a general abstraction layer across different metrics solutions like statsd, prometheus, new-relic, etc. However, metrics-rs is not well-suited for measuring metrics about histogram.

Nowadays prometheus is the de facto standard in the metrics area. Using a raw Prometheus client without pushing metrics to other systems like StatsD offers several benefits:

- **Improved Performance**: By adhering to best practices for metric handling, memory allocation for metrics can be optimized to achieve O(1) complexity, eliminating the need for local buffered queues.
- **Minimizing Abstraction Layers**: Streamlining metric handling helps reduce unnecessary abstraction layers, simplifying the code path for improved clarity and comprehension.
- **Improved Coding Standards**: Addressing the existing variance in metric practices across different modules by adopting Prometheus community standards can help establish a more consistent and cohesive approach to metrics within our codebase.

Now, Databend's observability metrics have been fully migrated to Prometheus implementation. This brings you a more comprehensive and reliable observability experience while keeping the original metrics almost unchanged.

If you are interested in learning more, please check out the resources listed below:

- [PR #12787 | feat(observability): replace metrics-rs with prometheus-client](https://github.com/datafuselabs/databend/pull/12787)
- [Issue #12635 | Tracking: replace metrics-rs with prometheus-client-rs](https://github.com/datafuselabs/databend/issues/12635)
- [Issue #9422 | Feature: Refactoring Databend Metrics Component with Prometheus](https://github.com/datafuselabs/databend/issues/9422)

## Highlights

We have also made these improvements to Databend that we hope you will find helpful:

- Added support for `COMPACT` distributed execution.
- Added the `json_path_exists` function.
- Added the `recluster_block_size` setting to control the block size during re-clustering.
- Added support for conversion from `DECIMAL` type to `INT` type.
- Added support for an inverted filter to optimize filter execution, leading to a performance boost of up to 4 times in certain scenarios.
- SQLSmith testing now supports generating table functions, window functions, subqueries, and the `WITH` clause.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Implementing the `GREATEST` Function

The `GREATEST` function returns the maximum value in a list of expressions.

**Syntax:**

```SQL
GREATEST( <expr1> [ , <expr2> ... ] )
```

**Example:**

```SQL
SELECT id, name, category, price, rating,
       CASE
         WHEN rating = 1 THEN 0.02
         WHEN rating = 2 THEN 0.04
         WHEN rating = 3 THEN 0.06
         WHEN rating = 4 THEN 0.08
         ELSE 0.1
       END AS increase_percentage_based_on_rating,
       rank() OVER (PARTITION BY category ORDER BY rating) AS rating_rank,
       CASE
         WHEN rating_rank = 1 THEN 0.2
         WHEN rating_rank = 2 THEN 0.1
         ELSE 0
       END AS increase_percentage_based_on_rank,
       GREATEST(increase_percentage_based_on_rating, 
                increase_percentage_based_on_rank) AS final_increase_percentage,
       CAST(price * (1 + final_increase_percentage) AS DECIMAL(10, 2))
         AS adjusted_price
FROM products
```

[Issue #12944 | feat: GREATEST function](https://github.com/datafuselabs/databend/issues/12944)

Please let us know if you're interested in contributing to this feature, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.2.116-nightly...v1.2.128-nightly>
