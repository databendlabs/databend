---
id: system-tables
title: System Tables
---

Most of system tables store their data in RAM. A FuseQuery server creates such system tables at the start.

## system.numbers

This table contains a single UInt64 column named number that contains almost all the natural numbers starting from zero.

You can use this table for tests, or if you need to do a brute force search.

Reads from this table are parallelized too.

Used for tests.

    mysql> SELECT avg(number) FROM system.numbers(100000000);
    +-------------+
    | avg(number) |
    +-------------+
    |  49999999.5 |
    +-------------+
    1 row in set (0.04 sec)

## system.numbers_mt

The same as system.numbers


## system.settings

Contains information about session settings for current user.

    mysql> SELECT * FROM system.settings;
    +----------------+---------+---------------------------------------------------------------------------------------------------+
    | name           | value   | description                                                                                       |
    +----------------+---------+---------------------------------------------------------------------------------------------------+
    | max_block_size | 10000   | Maximum block size for reading                                                                    |
    | max_threads    | 8       | The maximum number of threads to execute the request. By default, it is determined automatically. |
    | default_db     | default | The default database for current session                                                          |
    +----------------+---------+---------------------------------------------------------------------------------------------------+
    3 rows in set (0.00 sec)










