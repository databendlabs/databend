---
id: select-statement
title: SELECT
---

SELECT queries perform data retrieval.

Syntax:
```text
SELECT expr_list
[FROM [db.]table | table_function]
[WHERE expr]
[LIMIT m]
```

## SELECT

```text
mysql> SELECT * FROM system.numbers(3)

+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
+--------+
3 rows in set (0.00 sec)
```

## SELECT Expression

```text
SELECT (2*number+2)/(number+1) FROM system.numbers(3);

+-------------------------------------+
| (((2 * number) + 2) / (number + 1)) |
+-------------------------------------+
|                                   2 |
|                                   2 |
|                                   2 |
+-------------------------------------+
3 rows in set (0.00 sec)
```

## FROM Table

```text
mysql> SELECT * FROM system.settings;

+----------------+---------+
| name           | value   |
+----------------+---------+
| default_db     | default |
| max_threads    | 8       |
| max_block_size | 65536    |
+----------------+---------+
3 rows in set (0.00 sec)
```


## FROM Table Function

```text
SELECT * FROM system.numbers(1);

+--------+
| number |
+--------+
|      0 |
+--------+
1 row in set (0.00 sec)
```

## WHERE

WHERE clause allows to filter the data that is coming from FROM clause of SELECT.

```text
mysql> SELECT * FROM system.numbers(10) WHERE (number+5)<10;

+--------+
| number |
+--------+
|      0 |
|      1 |
|      2 |
|      3 |
|      4 |
+--------+
5 rows in set (0.00 sec)
```


## LIMIT

LIMIT m allows to select the first m rows from the result.

```text
mysql> SELECT * FROM system.numbers(10) LIMIT 2;

+--------+
| number |
+--------+
|      0 |
|      1 |
+--------+
2 rows in set (0.00 sec)
```
