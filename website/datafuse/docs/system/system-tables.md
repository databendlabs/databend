---
id: system-tables
title: System Tables
---

Most system tables store their data in RAM. A FuseQuery server creates such system tables at the start.

## system.numbers

This table contains a single UInt64 column named number that contains almost all the natural numbers starting from zero.

You can use this table for tests, or if you need to do a brute force search.

Reads from this table are parallelized too.

Used for tests.

```
mysql> SELECT avg(number) FROM numbers(100000000);
+-------------+
| avg(number) |
+-------------+
|  49999999.5 |
+-------------+
1 row in set (0.04 sec)
```

## system.numbers_mt

The same as system.numbers


## system.settings

Contains information about session settings for current user.

```
mysql> SELECT * FROM system.settings;
+----------------+---------+---------------------------------------------------------------------------------------------------+
| name           | value   | description                                                                                       |
+----------------+---------+---------------------------------------------------------------------------------------------------+
| max_block_size | 10000   | Maximum block size for reading                                                                    |
| max_threads    | 8       | The maximum number of threads to execute the request. By default, it is determined automatically. |
| default_db     | default | The default database for current session                                                          |
+----------------+---------+---------------------------------------------------------------------------------------------------+
3 rows in set (0.00 sec)
```

## system.functions

Contains information about normal and aggregate functions.

```
mysql> SELECT * FROM system.functions limit 10;
+----------+--------------+
| name     | is_aggregate |
+----------+--------------+
| +        |        false |
| plus     |        false |
| -        |        false |
| minus    |        false |
| *        |        false |
| multiply |        false |
| /        |        false |
| divide   |        false |
| %        |        false |
| modulo   |        false |
+----------+--------------+
10 rows in set (0.01 sec)

```
## system.contributors

Contains information about contributors. The order is random at query execution time.

```
mysql> SELECT * FROM system.contributors LIMIT 20;
+-------------------------+
| name                    |
+-------------------------+
| artorias1024            |
| BohuTANG                |
| dependabot[bot]         |
| dependabot-preview[bot] |
| drdr xp                 |
| Eason                   |
| hulunbier               |
| jyizheng                |
| leiysky                 |
| smallfish               |
| sundy-li                |
| sundyli                 |
| taiyang-li              |
| TLightSky               |
| Winter Zhang            |
| wubx                    |
| yizheng                 |
| Yizheng Jiao            |
| zhang2014               |
| zhihanz                 |
+-------------------------+
20 rows in set (0.00 sec)
```