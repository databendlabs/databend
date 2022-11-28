---
title: EXPLAIN SYNTAX
---

Outputs formatted SQL code. This command works as a SQL formatter that makes your code easy to read.

## Syntax

```sql
EXPLAIN SYNTAX <statement>
```

## Examples

```sql
EXPLAIN SYNTAX select a, sum(b) as sum from t1 where a in (1, 2) and b > 0 and b < 100 group by a order by a;

 ----
 SELECT
     a,
     sum(b) AS sum
 FROM
     t1
 WHERE
     a IN (1, 2)
     AND b > 0
     AND b < 100
 GROUP BY a
 ORDER BY a
```

```sql
EXPLAIN SYNTAX copy into 's3://mybucket/data.csv' from t1 file_format = ( type = 'CSV' field_delimiter = ',' record_delimiter = '\n' skip_header = 1) size_limit=10;

 ----
 COPY
 INTO 's3://mybucket/data.csv'
 FROM t1
 FILE_FORMAT = (
     field_delimiter = ",",
     record_delimiter = "\n",
     skip_header = "1",
     type = "CSV"
 )
 SIZE_LIMIT = 10
```