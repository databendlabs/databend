---
title: ALTER TABLE OPTION
---

Modify the table options on an existing table.

## Syntax

```sql
ALTER TABLE [database.]table_name SET OPTIONS (options)
```

Read more about the options in [fuse-options](../../../13-sql-reference/30-table-engines/00-fuse.md#options)

## Examples

```sql
create table t(a int, b int);

alter table t set options(bloom_index_columns='a');

set hide_options_in_show_create_table=0;

show create table t;
+-------+-------------------------------------------------------------------------+
| Table | Create Table                                                            |
+-------+-------------------------------------------------------------------------+
| t     | CREATE TABLE `t` (
  `a` INT,
  `b` INT
) ENGINE=FUSE BLOOM_INDEX_COLUMNS='a' COMPRESSION='zstd' STORAGE_FORMAT='parquet' |
+-------+-------------------------------------------------------------------------+

-- disable all the bloom filter index.
alter table t set options(bloom_index_columns='');

show create table t;
+-------+-------------------------------------------------------------------------+
| Table | Create Table                                                            |
+-------+-------------------------------------------------------------------------+
| t     | CREATE TABLE `t` (
  `a` INT,
  `b` INT
) ENGINE=FUSE BLOOM_INDEX_COLUMNS='' COMPRESSION='zstd' STORAGE_FORMAT='parquet'  |
+-------+-------------------------------------------------------------------------+
```
