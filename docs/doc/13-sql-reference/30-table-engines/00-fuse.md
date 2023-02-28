---
title: Fuse Engine
---

## Description

Fuse engine is the default engine for Databend, it provides a git-like interface for data management. User could query data at any point in time, and restore data to any point in time, there is a blog post about this feature: [Time Travel](https://databend.rs/blog/time-travel).

## Syntax

```sql
CREATE TABLE table_name (
  column_name1 column_type1,
  column_name2 column_type2,
  ...
) [ENGINE = Fuse] [CLUSTER BY(<expr> [, <expr>, ...] )] [options];
```

Read more about the create table statement in [ddl-create-table](../../14-sql-commands/00-ddl/20-table/10-ddl-create-table.md)

### Default engine

If engine is not specified, we will default to using `Engine = Fuse`.


### Cluster Key

The `CLUSTER BY` parameter specifies the sorting method for data that consists of multiple expressions, which is useful during compaction or recluster. A suitable `CLUSTER BY` parameter can significantly accelerate queries.


### Options

Fuse engine support following common case-insensitive options:

- `compression = '<compression>'`, `compression` could be `lz4`, `zstd`, `snappy`, `none`. Compression method defaults to be `zstd` in object storage but `lz4` in fs storage.

- `storage_format = '<storage_format>'`, `storage_format` could be `parquet` and `native`. Storage format defaults to be `parquet` in object storage but `native` in fs storage.

- `snapshot_loc = '<snapshot_loc>'`, it's a location parameter in string which could easily share a table without data copy.

- `block_size_threshold = '<block_size_threshold>'`, specifies the maximum data size for a file.
- `block_per_segment = '<block_per_segment>'`, specifies the maximum number of files that can be stored in a segment.
- `row_per_block = '<row_per_block>'`, specifies the maximum number of rows that can be stored in a file.


## What's storage format

By default, the storage_format is set to Parquet, which means the data is stored in Parquet format in the storage. Parquet is an open format that is suitable for cloud-native object storage and has a high compression ratio.

The storage_format also supports the Native format, which is an experimental format that primarily optimizes the additional memory copy overhead introduced when writing data to the storage. The Native format is suitable for storage devices such as file systems.
