---
title: Designing and Implementing the Query Result Cache
description: The efficiency of returning results for multiple identical queries has considerably boosted with the introduction of query result cache in Databend 1.0. This post explains how it works.
slug: cache
date: 2023-03-09
tags: [cache]
category: Engineering
cover_url: cache.png
image: cache.png
authors:
- name: Yijun Zhao
  url: https://github.com/ariesdevil
  image_url: https://github.com/ariesdevil.png
---

The query result cache is designed for queries with low update frequency. It caches the result set returned by the first query so that Databend can return results immediately from the cache for subsequent identical queries on the same data. For example, to obtain the top 5 best-selling products every ten seconds, we need to repeatedly run a query like this:

```sql
SELECT product, count(product) AS sales_count
FROM sales_log
GROUP BY product
ORDER BY sales_count DESC
LIMIT 5;
```

## Designing Query Result Cache

### Lifecycle of Query Result Cache

Each cached result set comes with a Time to Live (TTL) that is refreshed each time the cached result set is accessed. The default expiration time is 300 seconds, which can be modified through the setting `query_result_cache_ttl_secs`. A cached result set will no longer be available after it expires.

Cached results will become inaccurate when the underlying data (such as snapshot IDs, segment IDs, and partition locations) changes. In this case, you can still proceed with the cached results if you allow the inconsistence. To do so, set `query_result_cache_allow_inconsistent` to 1.

### Storing Cached Results

Databend uses key-value pairs to store query result sets. For each query, Databend constructs a corresponding key based on the query information and stores some metadata of the query result set as the value in the meta service. The rule for generating the key is as follows:

```rust
// Serialize the AST as a string, 
// then obtain the corresponding hash value through a hash function.
let ast_hash = sha256(formatted_ast);
// Concatenate the prefix of the result cache, the current tenant, 
// and the hash value generated above to obtain the final key.
let key = format!("{RESULT_CACHE_PREFIX}/{tenant}/{ast_hash}");
```

The structure of the value is as follows. Please note that the value only stores the metadata of the corresponding result set, and the actual result set will be written to your current storage, such as local file system or S3.

```rust
struct ResultCacheValue {
    /// The original query SQL.
    pub sql: String,
    /// Associated query id
    pub query_id: String,
    /// The query time.
    pub query_time: u64,
    /// Time-to-live of this query.
    pub ttl: u64,
    /// The size of the result cache (bytes).
    pub result_size: usize,
    /// The number of rows in the result cache.
    pub num_rows: usize,
    /// The sha256 of the partitions for each table in the query.
    pub partitions_shas: Vec<String>,
    /// The location of the result cache file.
    pub location: String,
}
```

### Reading from Cache

The process of reading from cache is relatively simple and can be illustrated using the following pseudocode:

```rust
// Generate the key for the corresponding query statement by using the formatted AST.
let key = gen_result_cache_key(formatted_ast);

// construct cache reader
let cache_reader = ResultCacheReader::create(ctx, key, meta_client, allow_inconsistent);

// The cache reader obtains the corresponding ResultCacheValue from the meta service using the key. 
// The structure of ResultCacheValue is shown in the previous code snippet.
let value = cache_reader.get(key)

// If it is acceptable to tolerate inconsistencies 
// or the hash values of the partitions covered by the query are the same,
// the cache reader will read the cached result set 
// from the underlying storage using the location and return it.
if allow_inconsistent || value.partitions_shas == ctx.partitions_shas {
    read_result_from_cache(&value.location)
}
```

### Writing to Cache

When a query does not hit the cache, the writing cache process is triggered. Databend uses a pipeline approach to schedule and process read and write tasks. The usual pipeline process is "source -> transform -> transform... -> sink", and writing cache will add a sink phase, so it is necessary to first parallelize a pipeline to duplicate the upstream data (the "duplicate" part in the figure below).

As the output port of the preceding node and the input port of the subsequent node in the pipeline are one-to-one correspondence, we use shuffle to reorder them to connect the preceding and subsequent processing nodes.

```sql
           ┌─────────┐ 1  ┌─────────┐ 1
           │         ├───►│         ├───►Dummy───►Downstream
Upstream──►│Duplicate│ 2  │         │ 3
           │         ├───►│         ├───►Dummy───►Downstream
           └─────────┘    │         │
                          │ Shuffle │
           ┌─────────┐ 3  │         │ 2  ┌─────────┐
           │         ├───►│         ├───►│  Write  │
Upstream──►│Duplicate│ 4  │         │ 4  │ Result  │
           │         ├───►│         ├───►│  Cache  │
           └─────────┘    └─────────┘    └─────────┘ 
```

:::note
If a query includes functions like `now()`, `rand()`, or `uuid()`, the result set will not be cached. In addition, system tables will not be cached, either.

Currently, the maximum amount of data cached for a query result is 1MiB. You can adjust the allowed cache size by setting `query_result_cache_max_bytes`.
:::

## Using Query Result Cache

### Enabling Query Result Cache

```
// To enable query result cache, set the following configuration.
// Databend will enable this configuration by default in the future.
SET enable_query_result_cache=1;

// To tolerate inaccurate results, set the following configuration.
SET query_result_cache_allow_inconsistent=1;
```

### Testing Query Result Cache

```sql
SET enable_query_result_cache=1;

SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
+---------------------+-------------+------+----------------+----------------------+
| watchid             | clientip    | c    | sum(isrefresh) | avg(resolutionwidth) |
+---------------------+-------------+------+----------------+----------------------+
| 6655575552203051303 |  1611957945 |    2 |              0 |               1638.0 |
| 8566928176839891583 | -1402644643 |    2 |              0 |               1368.0 |
| 7904046282518428963 |  1509330109 |    2 |              0 |               1368.0 |
| 7224410078130478461 |  -776509581 |    2 |              0 |               1368.0 |
| 5957995970499767542 |  1311505962 |    1 |              0 |               1368.0 |
| 5295730445754781367 |  1398621605 |    1 |              0 |               1917.0 |
| 8635802783983293129 |   900266514 |    1 |              1 |               1638.0 |
| 5650467702003458413 |  1358200733 |    1 |              0 |               1368.0 |
| 6470882100682188891 | -1911689457 |    1 |              0 |               1996.0 |
| 6475474889432602205 |  1501294204 |    1 |              0 |               1368.0 |
+---------------------+-------------+------+----------------+----------------------+
10 rows in set (3.255 sec)

SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
+---------------------+-------------+------+----------------+----------------------+
| watchid             | clientip    | c    | sum(isrefresh) | avg(resolutionwidth) |
+---------------------+-------------+------+----------------+----------------------+
| 6655575552203051303 |  1611957945 |    2 |              0 |               1638.0 |
| 8566928176839891583 | -1402644643 |    2 |              0 |               1368.0 |
| 7904046282518428963 |  1509330109 |    2 |              0 |               1368.0 |
| 7224410078130478461 |  -776509581 |    2 |              0 |               1368.0 |
| 5957995970499767542 |  1311505962 |    1 |              0 |               1368.0 |
| 5295730445754781367 |  1398621605 |    1 |              0 |               1917.0 |
| 8635802783983293129 |   900266514 |    1 |              1 |               1638.0 |
| 5650467702003458413 |  1358200733 |    1 |              0 |               1368.0 |
| 6470882100682188891 | -1911689457 |    1 |              0 |               1996.0 |
| 6475474889432602205 |  1501294204 |    1 |              0 |               1368.0 |
+---------------------+-------------+------+----------------+----------------------+
10 rows in set (0.066 sec)
```

As you can see, the results are returned immediately at the second time.

### RESULT_SCAN table function

Query result cache also provides a table function called RESULT_SCAN, which allows users to quickly retrieve the results of previous queries based on the query ID within the same session. Please refer to the [documentation](https://databend.rs/doc/sql-functions/table-functions/result_scan) for usage instructions.

Additionally, users can use `SELECT * from system.query_cache` to obtain the metadata of all cached result sets for the current tenant.

| Item               | Description                                                            |
|--------------------|------------------------------------------------------------------------|
| sql                | The original SQL statement                                             |
| query_id           | The query ID corresponding to the result set                           |
| result_size        | The size of the cached result set                                      |
| num_rows           | The number of rows in the cached result set                            |
| partitions_sha     | The hash value of the partitions covered by the query                  |
| location           | The location of the cached result set in the underlying storage        |
| active_result_scan | Whether the result set can be used by RESULT_SCAN, indicated by "true" |

## What's Next

- Clean expired cached data: As the cached result sets are not available after TTL expiration, the underlying data is not cleared automatically. In the future, a scheduled task can be used to clean up expired data.
- Compress the cached results to further save space.
- Cache result sets for complex SQL, such as (INSERT INTO xxx SELECT..., COPY FROM SELECT).

## Acknowledgments

The design of Databend's query result cache was inspired by ClickHouse and Snowflake. To learn more about the details of query result cache, please refer to the following links:

- [Databend Query Result Cache RFC](https://databend.rs/doc/contributing/rfcs/query-result-cache)
- [Query Cache in ClickHouse](https://clickhouse.com/docs/en/operations/query-cache/)
- [ClickHouse query cache blog](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)
- [Snowflake RESULT_SCAN function](https://docs.snowflake.com/en/sql-reference/functions/result_scan)
- [Tuning the Result Cache in Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgdba/tuning-result-cache.html)