---
title: READ_PARQUET
---

Reads a list of parquet files and returns a table.

Ths parquet files in the list must have the same schema.

This table function is mainly used for developers to test parquet reading. This function read parquet files on the local file system and it's insecure for production use.

**Only absolute paths are supported.**

If you want to use this function, you need to set `allow_insecure = true` in databend-query configuration file:

```toml
[storage]
allow_insecure: true
```

## Syntax

```sql
READ_PARQUET('path1', 'path2', ...)
READ_PARQUET('glob path', ...)
```

## Examples

```sql
select * from read_parquet('/data/test1.parquet');
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 | aa   |  2.0 |
|    2 | bb   |  3.0 |
+------+------+------+
2 rows in set (0.042 sec)

select * from read_parquet('/data/test2.parquet');
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    3 | cc   |  5.0 |
+------+------+------+
1 row in set (0.038 sec)

select * from read_parquet('/data/test1.parquet', '/data/test2.parquet');
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 | aa   |  2.0 |
|    2 | bb   |  3.0 |
|    3 | cc   |  5.0 |
+------+------+------+
3 rows in set (0.040 sec)

select * from read_parquet('/data/*.parquet');
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 | aa   |  2.0 |
|    2 | bb   |  3.0 |
|    3 | cc   |  5.0 |
+------+------+------+
3 rows in set (0.039 sec)

select * from read_parquet('/data/*.parquet') where c > 2.5;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    3 | cc   |  5.0 |
|    2 | bb   |  3.0 |
+------+------+------+
```
