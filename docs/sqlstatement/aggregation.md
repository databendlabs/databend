---
id: sql-statement-aggregation
title: Aggregation
---


## Count

    mysql> SELECT count(number) FROM system.numbers_mt(3);

    +---------------+
    | count(number) |
    +---------------+
    |             3 |
    +---------------+
    1 row in set (0.01 sec)


## Min

    mysql> SELECT min(number) FROM system.numbers_mt(3);

    +-------------+
    | min(number) |
    +-------------+
    |           0 |
    +-------------+
    1 row in set (0.00 sec)


## Max

    SELECT max(number) FROM system.numbers_mt(3);
    +-------------+
    | max(number) |
    +-------------+
    |           2 |
    +-------------+
    1 row in set (0.01 sec)

## Avg

    mysql> SELECT avg(number) FROM system.numbers_mt(3);

    +-------------+
    | avg(number) |
    +-------------+
    |           1 |
    +-------------+
    1 row in set (0.00 sec)

## Sum

    mysql> SELECT sum(number) FROM system.numbers_mt(3);

    +-------------+
    | sum(number) |
    +-------------+
    |           3 |
    +-------------+
    1 row in set (0.00 sec)

## Aggregation Arithmetic

Aggregation also supports arithmetic operation.

    mysql> SELECT sum(number+3)/count(number) FROM system.numbers_mt(3);

    +-------------------------------------+
    | (sum((number + 3)) / count(number)) |
    +-------------------------------------+
    |                                   4 |
    +-------------------------------------+
    1 row in set (0.00 sec)
