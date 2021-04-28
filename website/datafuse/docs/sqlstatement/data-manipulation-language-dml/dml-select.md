---
id: dml-select
title: SELECT
---

Retrieves data from a table.

## Syntax

```sql
SELECT
    [ALL | DISTINCT]
    select_expr [[AS] alias], ...
    [INTO variable [, ...]]
    [ FROM table_references
    [WHERE expr]
    [GROUP BY {{col_name | expr | position}, ...
    | extended_grouping_expr}]
    [HAVING expr]
    [ORDER BY {col_name | expr} [ASC | DESC], ...]
    [LIMIT row_count]
    ]
```

## Examples

### SELECT * FROM ...
```sql
mysql> SELECT * FROM numbers(3);
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+
```

### SELECT * FROM LIMIT

```sql
mysql> SELECT * FROM numbers(3) LIMIT 1;
+--------+
| number |
+--------+
|      0 |
+--------+
```

### SELECT * FROM ORDER BY ...

```sql
mysql> SELECT * FROM numbers(3) ORDER BY number asc;
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+

mysql> SELECT * FROM numbers(3) ORDER BY number desc;
+--------+
| number |
+--------+
|      2 |
|      1 |
|      0 |
+--------+
```

### SELECT * FROM GROUP BY ...

```sql
mysql> SELECT number FROM numbers(10) GROUP BY number%3;
+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+

mysql> SELECT MAX(number) FROM numbers(10) GROUP BY number%3;
+-------------+
| max(number) |
+-------------+
|           7 |
|           8 |
|           9 |
+-------------+
```

### Nested Sub-Selects

SELECT statements can be nested in queries.

```sql
SELECT ... [SELECT ...[SELECT [...]]]
```

```sql
mysql> SELECT MIN(number) FROM (SELECT number%3 AS number FROM numbers(10)) GROUP BY number%2;
+-------------+
| min(number) |
+-------------+
|           1 |
|           0 |
+-------------+
```
