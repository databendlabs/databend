---
id: select-aggregate
title: AGGREGATE
---


## Count

```text
mysql> SELECT count(number) FROM system.numbers(3);

+---------------+
| count(number) |
+---------------+
|             3 |
+---------------+
1 row in set (0.01 sec)
```


## Min

```text
mysql> SELECT min(number) FROM system.numbers(3);

+-------------+
| min(number) |
+-------------+
|           0 |
+-------------+
1 row in set (0.00 sec)
```


## Max

```text
SELECT max(number) FROM system.numbers(3);

+-------------+
| max(number) |
+-------------+
|           2 |
+-------------+
1 row in set (0.01 sec)
```

## Avg

```text
mysql> SELECT avg(number) FROM system.numbers(3);

+-------------+
| avg(number) |
+-------------+
|           1 |
+-------------+
1 row in set (0.00 sec)
```

## Sum

```text
mysql> SELECT sum(number) FROM system.numbers(3);

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
mysql> SELECT sum(number+3)/count(number) FROM system.numbers(3);

+-------------------------------------+
| (sum((number + 3)) / count(number)) |
+-------------------------------------+
|                                   4 |
+-------------------------------------+
1 row in set (0.00 sec)
```
