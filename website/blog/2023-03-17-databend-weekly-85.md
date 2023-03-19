---
title: "This Week in Databend #85"
date: 2023-03-17
slug: 2023-03-17-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: Carlosfengv
  - name: Chasen-Zhang
  - name: dantengsky
  - name: dependabot[bot]
  - name: drmingdrmer
  - name: everpcpc
  - name: jun0315
  - name: leiysky
  - name: lichuang
  - name: mergify[bot]
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
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

> :loudspeaker: Read our blog *[Way to Go: OpenDAL successfully entered Apache Incubator](https://databend.rs/blog/opendal-enters-apache-incubator)* to learn about the story of [OpenDAL](https://github.com/apache/incubator-opendal).

## What's On In Databend

Stay connected with the latest news about Databend.

### Data Type: MAP

The MAP data structure holds `Key:Value` pairs using a nested `Array(Tuple(key, value))` and is useful when the data type is constant but the Key's value cannot be fully determined. The Key must be of a specified basic data type and duplicates are not allowed, while the Value can be any data type including nested arrays or tuples. A bloom filter index is created in Map makes it easier and faster to search for values in MAP.

```sql
select * from nginx_log where log['ip'] = '205.91.162.148';
+----+----------------------------------------+
| id | log                                    |
+----+----------------------------------------+
| 1  | {'ip':'205.91.162.148','url':'test-1'} |
+----+----------------------------------------+
1 row in set
```

If you want to learn more about the Map data type, please read the following materials:

- [Docs | Data Types - Map](https://databend.rs/doc/sql-reference/data-types/data-type-map)

### Data Transformation During Loading Process

Do you remember the two RFCs mentioned last week? Now, Databend has added support for data transformation during the loading process into tables. Basic transformation operations can be achieved by using the `COPY INTO <table>` command.

```sql
CREATE TABLE my_table(id int, name string, time date);

COPY INTO my_table
FROM (SELECT t.id, t.name, to_date(t.timestamp) FROM @mystage t)
FILE_FORMAT = (type = parquet) PATTERN='.*parquet';
```

This feature avoids storing pre-transformed data in temporary tables and supports column reordering, column omission, and type conversion operation. In addition, partial data can be loaded from staged Parquet files or their columns can be rearranged. This feature simplifies and streamlines ETL processes, allowing uses to give more attentions on the data analysis without considering how to move their data.

If you're interested, check the following documentation:

- [Docs | Transforming Data During a Load](https://databend.rs/doc/load-data/data-load-transform)
- [PR | feat(storage): Map data type support bloom filter](https://github.com/datafuselabs/databend/pull/10457)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Run Multiple Futures Parallel

Are you interested in how to run futures in parallel? It is worth mentioning that Databend has greatly improved the scanning performance in situations with a huge number of files by utilizing this technique.

The following code, which is less than 30 lines long, will introduce you to how it all works.

```rust
/// Run multiple futures parallel
/// using a semaphore to limit the parallelism number, and a specified thread pool to run the futures.
/// It waits for all futures to complete and returns their results.
pub async fn execute_futures_in_parallel<Fut>(
    futures: impl IntoIterator<Item = Fut>,
    thread_nums: usize,
    permit_nums: usize,
    thread_name: String,
) -> Result<Vec<Fut::Output>>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    // 1. build the runtime.
    let semaphore = Semaphore::new(permit_nums);
    let runtime = Arc::new(Runtime::with_worker_threads(
        thread_nums,
        Some(thread_name),
    )?);

    // 2. spawn all the tasks to the runtime with semaphore.
    let join_handlers = runtime.try_spawn_batch(semaphore, futures).await?;

    // 3. get all the result.
    future::try_join_all(join_handlers)
        .await
        .map_err(|e| ErrorCode::Internal(format!("try join all futures failure, {}", e)))
}
```

If you are interested in this Rust trick, you can read this PR: [feat: improve the parquet get splits to parallel](https://github.com/datafuselabs/databend/pull/10514).

### How to Create a System Table

System tables are tables that provide information about Databend's internal state, such as databases, tables, functions, and settings.

If you're interested in creating a system table, check out our recently released documentation which introduces the implementation, registration, and testing of system tables, using the `system.credits` table as an example.

Here is a code snippet:

```rust
let table_info = TableInfo {
    desc: "'system'.'credits'".to_string(),
    name: "credits".to_string(),
    ident: TableIdent::new(table_id, 0),
    meta: TableMeta {
        schema,
        engine: "SystemCredits".to_string(),
        ..Default::default()
    },
..Default::default()
};
```

- [Docs | How to Create a System Table](https://databend.rs/doc/contributing/how-to-write-a-system-table)

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- MindsDB's machine learning capability has now been integrated into Databend: *[Bringing in-database ML to Databend](https://mindsdb.com/integrations/databend-machine-learning)*
- Are you interested in using WebHDFS as the storage for Databend? This blog post may be helpful for you. *[How to Configure WebHDFS as a Storage Backend for Databend](https://databend.rs/blog/2023-03-13-webhdfs-storage-for-backend)*

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Support Quantile with A List

After the merge of PR [#10474](https://github.com/datafuselabs/databend/pull/10474), Databend began to support quantile aggregation functions, but currently only supports setting a single floating-point value as the level. If it could also support passing in a list, it may help simplify SQL writing in some scenarios.

```sql
SELECT QUANTILE([0.25, 0.5, 0.75])(number) FROM numbers(25);
+-------------------------------------+
| quantile([0.25, 0.5, 0.75])(number) |
+-------------------------------------+
|            [6, 12, 18]              |
+-------------------------------------+
```

[Feature: quantile support list and add functions kurtosis() and skewness()](https://github.com/datafuselabs/databend/issues/10589)

Additionally, the `kurtosis(x)` and `skewness(x)` mentioned in this issue seem a good starting point for contributing to Databend.

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.0.11-nightly...v1.0.21-nightly>
