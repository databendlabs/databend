---
title: Query Result Cache
---

- RFC PR: [datafuselabs/databend#10014](https://github.com/datafuselabs/databend/pull/10014)
- Tracking Issue: [datafuselabs/databend#10011](https://github.com/datafuselabs/databend/issues/10011)

## Summary

Support query result cache for faster query response.

## Motivation

For some expensive queries with data that doesn't change frequently, we can cache the results to speed up the query response. For the same query with the same underlying data, we can return the cached result directly which improves the query efficiency greatly.

For example, if we want to execute the following query to get the top 5 well-sold products every 10 seconds:

```sql
SELECT product, count(product) AS sales_count
FROM sales_log
GROUP BY product
ORDER BY sales_count DESC
LIMIT 5;
```

If we execute the full query pipeline every time, the cost may be every expensive, but the result is very small (5 rows). As the data of `sales_log` may not change very frequently, we can cache the result of the query and return the cached result directly for the same query.

## Detail design

### Lifecycle of query result cache

Each result cache has a time-to-live (TTL). Each access to the result cache will refresh the TTL .When the TTL is expired, the result cache will not be used any more.

Besides TTL, when the underlying data is changed (we can infer this by snapshot ids, segment ids or partition locations), the result cache will also be invalidated.

### Result cache storage

Databend uses key-value pairs to record query result caches. For every query, Databend will construct a key to represent the query, and store related information in the value.

Databend will not store the query result directly in the key-value storage. Instead, Databend only stores the location of the result cache file in the value. The actual result cache will be stored in the storage layer (local fs, s3, ...).

#### Key

Query result cache is indexed by its abstract syntax tree (AST). Databend serializes the AST to a string and use hashes it as the key.

The key generation is like:

```rust
let ast_str = ast.to_string();
let key = format!("_cache/{}/{}", tenant, hash(ast_str.as_bytes()));
```

#### Value structure

A query result cache value's structure is like:

```rust
pub struct ResultCacheValue {
    /// The query SQL.
    pub sql: String,
    /// The query time.
    pub query_time: DateTime<Utc>,
    /// Time-to-live of this query.
    pub ttl: usize,
    /// The size of the result cache (bytes).
    pub result_size: usize,
    /// The location of the result cache file.
    pub location: String,

    // May be other information
    // ...
}
```

#### Key-value storage

`databend-meta` has a capability to store and query key-value pairs. Databend use it to store query result cache key-value pairs.

#### Garbage collection

Databend will cache every query result if query result cache is enabled. If the result cache is expired, the cache will not be used any more. To save the disk or object storage space, Databend needs a daemon thread to scan all the query cache periodically and remove the expired ones.

### Related configurations

- `enable_query_result_cache`: whether to enable query result cache (default: false).
- `query_result_cache_max_bytes`: the maximum size of the result cache for one query (default: 1048576 bytes, 1MB).
- `query_result_cache_ttl_secs`: the time-to-live of the result cache (default: 300 seconds).

### Write result cache

`TransformWriteResultCache` is used to handle query result cache writing:

```rust
pub struct TransformWriteResultCache {
    ctx: Arc<QueryContext>,
    cache_key: String,
    cache_writer: ResultCacheWriter,
}
```

When constructing the query pipeline, Databend will add `TransformWriteResultCache` to the end of the pipeline:

```rust
impl Interpreter for SelectInterpreterV2 {
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let build_res = self.build_pipeline().await?;
        if self.ctx.get_settings().get_query_result_cache().enable_query_result_cache {
            build_res.main_pipeline.add_transform(TransformWriteResultCache::try_create)?;
        }
        Ok(build_res)
    }
}
```

The process of `TransformWriteResultCache` is like:

1. If upstream is finished, use `cache_writer` to generate  and write the result to a cache file. Go to 6.
2. Read a `DataBlock` from the input port.
3. If `cache_writer` is full (reach `query_result_cache_max_bytes`), goto 5 (do not write to cache).
4. Push the `DataBlock` into `cache_writer`.
5. Output the `DataBlock` to the output port. Goto 1.
6. Finish.

### Read result cache

Before constructing the select interpreter, Databend will check if the query result cache is available.

Databend will validate the `ResultCacheValue` by the cache key (AST) from `databend-meta` first. If the result cache is available and valid, Databend will get the query result from the result cache file; otherwise, Databend will continue to build and execute the original query pipeline.

### System table `system.query_cache`

System table `system.query.cache` is used to look up query result cache information.

The table contains such information:

- `sql`: the cached SQL.
- `query_time`: the last query time.
- `expired_time`: the expired time of the result cache.
- `result_size`: the size of the result cache (bytes).
- `location`: the location of the result cache file.

### Table function `RESULT_SCAN`

`RESULT_SCAN` is a useful table function to retrieve the result set of a previous query.

It can be used like:

```sql
select * from RESULT_SCAN('<query_id>');
select * from RESULT_SCAN(LAST_QUERY_ID());
```

If the previous query result is cached, we can get the result set quickly from query result cache.

### Non-deterministic functions

Some functions are non-deterministic, such as `now()`, `rand()`, `uuid()`, etc. If these functions are used in the query, the result will not be cached.

## References

- [Query Cache in ClickHouse](https://clickhouse.com/docs/en/operations/query-cache/)
- [Blog about the query cache in ClickHouse](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)
- [RESULT_SCAN in snowflake](https://docs.snowflake.com/en/sql-reference/functions/result_scan)
- [Tuning the Result Cache in Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgdba/tuning-result-cache.html)
