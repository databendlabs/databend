---
id: select-aggregate
title: AGGREGATE
---


## Count

```text
mysql> SELECT count(number) FROM numbers(3);

+---------------+
| count(number) |
+---------------+
|             3 |
+---------------+
1 row in set (0.01 sec)
```


## Min

```text
mysql> SELECT min(number) FROM numbers(3);

+-------------+
| min(number) |
+-------------+
|           0 |
+-------------+
1 row in set (0.00 sec)
```


## Max

```text
SELECT max(number) FROM numbers(3);

+-------------+
| max(number) |
+-------------+
|           2 |
+-------------+
1 row in set (0.01 sec)
```

## Avg

```text
mysql> SELECT avg(number) FROM numbers(3);

+-------------+
| avg(number) |
+-------------+
|           1 |
+-------------+
1 row in set (0.00 sec)
```

## Sum

```text
mysql> SELECT sum(number) FROM numbers(3);

+-------------+
| sum(number) |
+-------------+
|           3 |
+-------------+
1 row in set (0.00 sec)
```

## Aggregation Arithmetic

Aggregation also supports arithmetic operation.

```text
mysql> SELECT sum(number+3)/count(number) FROM numbers(3);

+-------------------------------------+
| (sum((number + 3)) / count(number)) |
+-------------------------------------+
|                                   4 |
+-------------------------------------+
1 row in set (0.00 sec)
```

## Group By


```text
mysql> SELECT sum(number), number%3, number%4 FROM numbers(100)  GROUP BY number % 3, number % 4;

+-------------+-------------------+-------------------+
| sum(number) | modulo(number, 3) | modulo(number, 4) |
+-------------+-------------------+-------------------+
|         376 |                 2 |                 1 |
|         408 |                 0 |                 1 |
|         450 |                 2 |                 2 |
|         441 |                 1 |                 1 |
|         384 |                 0 |                 2 |
|         400 |                 2 |                 0 |
|         368 |                 1 |                 0 |
|         432 |                 0 |                 0 |
|         459 |                 0 |                 3 |
|         416 |                 1 |                 2 |
|         424 |                 2 |                 3 |
|         392 |                 1 |                 3 |
+-------------+-------------------+-------------------+
```
