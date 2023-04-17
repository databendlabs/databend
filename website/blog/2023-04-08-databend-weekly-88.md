---
title: "This Week in Databend #88"
date: 2023-04-08
slug: 2023-04-08-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ArberSephirotheca
  - name: ariesdevil
  - name: BohuTANG
  - name: dantengsky
  - name: Dousir9
  - name: everpcpc
  - name: flaneur2020
  - name: jsoref
  - name: leiwenfang
  - name: leiysky
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: wubx
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Support Eager Aggregation

Eager aggregation helps improve the performance of queries that involve grouping and joining data. It works by partially pushing a groupby past a join, which reduces the number of input rows to the join and may result in a better overall plan.

Databend recently added support for Eager aggregation. Here is an example of how it works.

```
Input:
                expression
                    |
         aggregate: SUM(x), SUM(y)
                    |
                   join
                    | \
                    | (y)
                    |
                   (x)
(1) Eager Groupby-Count:
                expression
                    |
     final aggregate: SUM(eager SUM(x)), SUM(y * cnt)
                    |
                    join
                    | \
                    | (y)
                    |
                    eager group-by: eager SUM(x), eager count: cnt
(2) Eager Split:
                expression
                    |
     final aggregate: SUM(eager SUM(x) * cnt2), SUM(eager SUM(y) * cnt1)
                    |
                    join
                    | \
                    | eager group-by: eager SUM(y), eager count: cnt2
                    |
                    eager group-by: eager SUM(x), eager count: cnt1
```

If you are interested in learning more, please check out the resources listed below.

- [PR #10716 | feat(planner): support eager aggregation](https://github.com/datafuselabs/databend/pull/10716)
- [Paper | Eager Aggregation and Lazy Aggregation](https://www.vldb.org/conf/1995/P345.PDF)

### Support All TPC-DS Queries

Databend now supports all TPC-DS queries!

TPC-DS is a decision support benchmark that models several generally applicable aspects of a decision support system, including queries and data maintenance. The benchmark provides a representative evaluation of performance as a general-purpose decision support system.

If you are interested in learning more, please check out the resources listed below.

- [PR | feat: support cte in dphyp](https://github.com/datafuselabs/databend/pull/10910)
- [PR | chore(test): add tpcds to test](https://github.com/datafuselabs/databend/pull/10673)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### databend-driver - A driver for Databend in Rust

The Databend community has crafted a Rust driver that allows developers to connect to Databend and execute SQL queries in Rust.

Here's an example of how to use the driver:

```Rust
use databend_driver::new_connection;

let dsn = "databend://root:@localhost:8000/default?sslmode=disable";
let conn = new_connection(dsn).unwrap();

let sql_create = "CREATE TABLE books (
    title VARCHAR,
    author VARCHAR,
    date Date
);";
conn.exec(sql_create).await.unwrap();
let sql_insert = "INSERT INTO books VALUES ('The Little Prince', 'Antoine de Saint-Exupéry', '1943-04-06');";
conn.exec(sql_insert).await.unwrap();
```

Feel free to try it out and give us feedback. For more information, follow the resources listed below.

- [Crates.io - databend-driver](https://crates.io/crates/databend-driver)
- [Github - datafuselabs/databend-client](https://github.com/datafuselabs/databend-client)

### AskBend - SQL-based Knowledge Base Search and Completion

AskBend is a Rust project that utilizes the power of Databend and OpenAI to create a SQL-based knowledge base from Markdown files.

With AskBend, you can easily search and retrieve the most relevant information to your queries using SQL. The project automatically generates document embeddings from the content, enabling you to quickly find the information you need.

How it works:

1. Read and parse Markdown files from a directory.
2. Store the content in the `askbend.doc` table.
3. Compute embeddings for the content using Databend Cloud's built-in AI capabilities.
4. When a users asks a question, generate the embedding using Databend Cloud's SQL-based `ai_embedding_vector` function.
5. Find the most relevant doc.content using Databend Cloud's SQL-based `cosine_distance` function.
6. Use OpenAI's completion capabilities with Databend Cloud's SQL-based `ai_text_completion` function.
7. Output the completion result in Markdown format.

If you want to learn more about AskBend or try out the existing live demo, you can refer to the resources listed below:

- [Live Demo - asking for Databend documentation](https://ask.databend.rs/)
- [Github - datafuselabs/askbend](https://github.com/datafuselabs/askbend)

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- New Aggregation Functions Added: `QUANTILE_DISC`, `KURTOSIS`, `SKEWNESS`
- Learn everything about AI functions in Databend: [Docs - AI Functions](https://databend.rs/doc/sql-functions/ai-functions/)

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Add Nullable Table Schema Tests to Databend

Currently, Databend table schema is not nullable by default. So almost all of tests table schemas are not nullable, we need to add some tests which table schemas are nullable to cover.

To achieve this goal, we need to add some new test cases in Databend. These test cases should include nullable table schemas to ensure that Databend can handle these cases correctly.

[Issue #10969 | test: add some tests which table schemas are nullable](https://github.com/datafuselabs/databend/issues/10969)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@Dousir9](https://github.com/Dousir9) made their first contribution in [#10884](https://github.com/datafuselabs/databend/pull/10884). The PR fixes the wrong cardinality estimation when the aggregation function's argument has multiple columns.
- [@YimingQiao](https://github.com/YimingQiao) made their first contribution in [#10906](https://github.com/datafuselabs/databend/pull/10906). The PR adds function summarization of KURTOSIS and SKEWNESS and reorders the functions to make it consistent with the function order in the navigation bar.
- [@jsoref](https://github.com/jsoref) made their first contribution in [#10914](https://github.com/datafuselabs/databend/pull/10914). The PR helps improve the quality of the code and documentation by fixing spelling errors.
- [@leiwenfang](https://github.com/leiwenfang) made their first contribution in [#10917](https://github.com/datafuselabs/databend/pull/10917). The PR beautifies the covers of blog.
- [@ArberSephirotheca](https://github.com/ArberSephirotheca) made their first contribution in [#10949](https://github.com/datafuselabs/databend/pull/10949). The PR adds a new function called `to_unix_timestamp()` which converts Databend timestamp to Unix timestamp.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.0.46-nightly...v1.0.57-nightly>
