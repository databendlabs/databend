---
id: kill-query
title: KILL QUERY
---

Attempts to forcibly terminate the currently running queries.

## Syntax

```
KILL QUERY|CONNECTION <query_id>
```

## Examples

```
mysql> show processlist;
+--------------------------------------+-------+-----------------+-------+----------+--------------------------------------------------------------------------------------+
| id                                   | type  | host            | state | database | extra_info                                                                           |
+--------------------------------------+-------+-----------------+-------+----------+--------------------------------------------------------------------------------------+
| c95bd106-e897-407e-8e86-96e47e3d3717 | MySQL | 127.0.0.1:44606 | Query | default  | show processlist                                                                     |
| 0ad851b2-6cde-488d-b965-e016de6a9fd5 | MySQL | 127.0.0.1:44644 | Query | default  | select sum(number) from numbers_mt(10000000000) group by number%3, number%4,number%5 |
+--------------------------------------+-------+-----------------+-------+----------+--------------------------------------------------------------------------------------+
2 rows in set (0.01 sec)
Read 2 rows, 380 B in 0.003 sec., 631.97 rows/sec., 120.07 KB/sec.

mysql> kill query '0ad851b2-6cde-488d-b965-e016de6a9fd5';
Query OK, 0 rows affected (0.01 sec)
Read 0 rows, 0 B in 0.000 sec., 0 rows/sec., 0 B/sec.
```
