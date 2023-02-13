---
title: Query Result Cache
---

- RFC PR: [datafuselabs/databend#10014](https://github.com/datafuselabs/databend/pull/10014)
- Tracking Issue: [datafuselabs/databend#10011](https://github.com/datafuselabs/databend/issues/10011)

## Summary

Support query result cache for faster query response.

## Motivation

For some expensive queries with data that doesn't change frequently, we can cache the results to speed up the query response. For the same query with the same underlying data, we can return the cached result directly which improves the query efficiency greatly.

For example, if we want to execute the following query to get the top 5 well-saled products everyt 10 seconds:

```sql
SELECT product, count(product) AS sales_count
FROM sales_log
GROUP BY product
ORDER BY sales_count DESC
LIMIT 5;
```

If we execute the full query pipeline every time, the cost may be every expensive, but the result is very small (5 rows). As the data of `sales_log` may not change very frequently, we can cache the result of the query and return the cached result directly for the same query.

## Detail design

### Result cache storage

Databend uses key-value pairs to record query result caches. For every query, Databend will construct a key to represent the query, and store ralated information in the value.

Databend will not store the query result directly in the key-value storage. Instead, Databend only stores the location of the result cache file in the value. The actual result cache will be stored in the storage layer (local fs, s3, ...).

#### Key structure

A query result cache key contains such information:

- `ast`: the AST of the query.
- `locations`: the locations of partitions used in the query.

Databend will use the information to construct the key. The algorithm is like:

```rust
let ast_str = serialize(ast);
let part_str = join(sort(locations), ",");
let raw_key = join([ast_str, part_str], ",");
let key = "_cache" + hash256(raw_key);
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
    /// The location of the result cache file.
    pub location: String,
}
```

#### Key-value storage

`databend-meta` has a capability to store and query key-value pairs. Databend use it to store query result cache key-value pairs.

#### Garbage collection

Databend will cache every query result is query result cache is enabled. If the result cache is expired, the cache will not be used any more. To save the disk or object storage space, Databend needs a deamon thread to scan all the query cache periodically and remove the expired ones.

### Related configurations

- `enable_write_result_cache`: whether to cache the query results (default: true).
- `enable_read_result_cache`: whether to find query result from cache (default: true).
- `max_result_cache_bytes`: the maximum size of the result cache for one query (default: 1048576 bytes, 1MB).
- `result_cache_ttl`: the time-to-live of the result cache (default: 300 seconds).

### Write result cache

`WriteResultCacheTransform` is used to handle query result cache writing:

```rust
pub struct WriteResultCacheTransform {
    ctx: Arc<QueryContext>,
    cache_key: String,
    cache_writer: ResultCacheWriter,
}
```

When constructing the query pipeline, Databend will add `WriteResultCacheTransform` to the end of the pipeline.

The process of `WriteResultCacheTransform` is like:

1. If upstream is finished, use `cache_writer` to generate  and write the result to a cache file. Go to 6.
2. Read a `DataBlock` from the input port.
3. If `cache_writer` is full (reach `max_result_cache_bytes`), goto 5 (do not write to cache).
4. Push the `DataBlock` into `cache_writer`.
5. Output the `DataBlock` to the output port. Goto 1.
6. Finish.

### Read result cache

Before constructing the query pipeline, Databend will check if the query result cache is available. If the result cache is available, Databend will construct `ResultCacheSource` to read `DataBlock` from the result cache file.

The query execution process with query result cache is like:

```rust
impl Interpreter for SelectInterpreterV2 {
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if self.ctx.get_settings().get_query_result_cache().enable_read_result_cache {
            return self.read_from_result_cache().await;
        }
        let build_res = self.build_pipeline().await?;
        if self.ctx.get_settings().get_query_result_cache().enable_write_result_cache {
            build_res.main_pipeline.add_transform(WriteResultCacheTransform::try_create)?;
        }
        Ok(build_res)
    }
}
```

### System table `system.query_cache`

System table `system.query.cache` is used to look up query result cache information.

The table contains such information:

- `sql`: the cached SQL.
- `query_time`: the last query time.
- `expired_time`: the expired time of the result cache.
- `location`: the location of the result cache file.
