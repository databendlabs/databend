---
id: dml-insert
title: INSERT
---

Writing data.

## Syntax

```
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```


!!! note
    Local engine is one of `Memory`, `Parquet`, `JSONEachRow`, `Null` or `CSV`, data will be stored in the DatabendQuery memory/disk locally.

    Remote engine is `remote`, will be stored in the remote DatabendStore cluster.

## Examples

### Memory engine

```sql
mysql> CREATE TABLE test(a UInt64, b Varchar) Engine = Memory;

mysql> INSERT INTO test(a,b) values(888, 'stars');
mysql> INSERT INTO test values(1024, 'stars');

mysql> SELECT * FROM test;
+------+-------+
| a    | b     |
+------+-------+
|  888 | stars |
| 1024 | stars |
+------+-------+
```