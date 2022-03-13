---
title: SHOW PROCESSLIST
---

The Databend process list indicates the operations currently being performed by the set of threads executing within the server.

The SHOW PROCESSLIST statement is one source of process information.

## Syntax

```
SHOW PROCESSLIST
```

## Examples

```sql
mysql> SHOW PROCESSLIST;
+--------------------------------------+-------+-----------------+------+-------+----------+--------------------------------------------------------------------------------------+--------------+------------------------+-------------------------+-------------------------+--------------------------+
| id                                   | type  | host            | user | state | database | extra_info                                                                           | memory_usage | dal_metrics_read_bytes | dal_metrics_write_bytes | scan_progress_read_rows | scan_progress_read_bytes |
+--------------------------------------+-------+-----------------+------+-------+----------+--------------------------------------------------------------------------------------+--------------+------------------------+-------------------------+-------------------------+--------------------------+
| e04dd121-88f4-4290-85be-2b45c6e3b011 | MySQL | 127.0.0.1:65291 | root | Query | default  | select sum(number) from numbers_mt(10000000000) group by number%3, number%4,number%5 |            0 |                      0 |                       0 |              2391200000 |              19129600000 |
| 179c99d5-1894-4d4c-a89e-4b293d404c88 | MySQL | 127.0.0.1:64597 | root | Query | default  | show processlist                                                                     |            0 |                      0 |                       0 |                       0 |                        0 |
+--------------------------------------+-------+-----------------+------+-------+----------+--------------------------------------------------------------------------------------+--------------+------------------------+-------------------------+-------------------------+--------------------------+
```
