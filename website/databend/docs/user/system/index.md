---
title: System Tables
---

Most system tables store their data in RAM. A DatabendQuery server creates such system tables at the start.

## system.numbers

This table contains a single UInt64 column named number that contains almost all the natural numbers starting from zero.

You can use this table for tests, or if you need to do a brute force search.

Reads from this table are parallelized too.

Used for tests.

```sql
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

```sql
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

```sql
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

Contains information about contributors.

```sql
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

## system.credits

Contains information about credits.

```sql
mysql> SELECT * FROM system.credits LIMIT 20;
+-------------------+---------+---------------------------+
| name              | version | license                   |
+-------------------+---------+---------------------------+
| addr2line         | 0.16.0  | Apache-2.0 OR MIT         |
| adler             | 1.0.2   | 0BSD OR Apache-2.0 OR MIT |
| ahash             | 0.6.3   | Apache-2.0 OR MIT         |
| ahash             | 0.7.4   | Apache-2.0 OR MIT         |
| aho-corasick      | 0.7.18  | MIT OR Unlicense          |
| ansi_term         | 0.9.0   | MIT                       |
| ansi_term         | 0.11.0  | MIT                       |
| ansi_term         | 0.12.1  | MIT                       |
| anyhow            | 1.0.43  | Apache-2.0 OR MIT         |
| arbitrary         | 1.0.1   | Apache-2.0 OR MIT         |
| arrayvec          | 0.4.12  | Apache-2.0 OR MIT         |
| arrayvec          | 0.5.2   | Apache-2.0 OR MIT         |
| arrow-flight      | 0.1.0   | Apache-2.0                |
| arrow2            | 0.4.0   | Apache-2.0                |
| assert_cmd        | 2.0.1   | Apache-2.0 OR MIT         |
| async-compat      | 0.2.1   | Apache-2.0 OR MIT         |
| async-raft        | 0.6.1   | Apache-2.0 OR MIT         |
| async-stream      | 0.3.2   | MIT                       |
| async-stream-impl | 0.3.2   | MIT                       |
| async-trait       | 0.1.51  | Apache-2.0 OR MIT         |
+-------------------+---------+---------------------------+
20 rows in set (1.33 sec)
```

## system.query_log

A read-only in-memory table stores all the query logs;

```sql
mysql> SELECT * FROM system.query_log LIMIT 1;
+----------+--------------+-----------+--------------+----------+-----------------------------+---------------------------------------------------------------------------+--------------------------------------+------------+------------+------------+-------------------------+------------------+-----------+--------+---------+-------------+--------------+---------------+-----------+------------+-------------+--------------+-----------+--------------+-------------+-----------------------+----------------+----------------+-------------+----------------+-------+
| log_type | handler_type | tenant_id | cluster_id   | sql_user | sql_user_quota              | sql_user_privileges                                                       | query_id                             | query_kind | query_text | event_date | event_time              | current_database | databases | tables | columns | projections | written_rows | written_bytes | read_rows | read_bytes | result_rows | result_bytes | cpu_usage | memory_usage | client_info | client_address        | exception_code | exception_text | stack_trace | server_version | extra |
+----------+--------------+-----------+--------------+----------+-----------------------------+---------------------------------------------------------------------------+--------------------------------------+------------+------------+------------+-------------------------+------------------+-----------+--------+---------+-------------+--------------+---------------+-----------+------------+-------------+--------------+-----------+--------------+-------------+-----------------------+----------------+----------------+-------------+----------------+-------+
|        1 | MySQL        |           | test_cluster | default  | UserGrantSet { grants: [] } | UserQuota { max_cpu: 0, max_memory_in_bytes: 0, max_storage_in_bytes: 0 } | 83b25875-2722-4439-8944-ffbf7d4462f4 | SelectPlan | SELECT 1   | 2021-12-23 | 2021-12-23 17:55:47.569 | default          |           |        |         |             |            0 |             0 |         0 |          0 |           0 |            0 |        16 |         3527 |             | Some(127.0.0.1:42074) |              0 |                |             |                |       |
+----------+--------------+-----------+--------------+----------+-----------------------------+---------------------------------------------------------------------------+--------------------------------------+------------+------------+------------+-------------------------+------------------+-----------+--------+---------+-------------+--------------+---------------+-----------+------------+-------------+--------------+-----------+--------------+-------------+-----------------------+----------------+----------------+-------------+----------------+-------+
1 row in set (0.05 sec)
Read 2 rows, 1.21 KB in 0.042 sec., 47.27 rows/sec., 28.51 KB/sec.
```
