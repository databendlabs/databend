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
SHOW PROCESSLIST;
+--------------------------------------+-------+-----------------+------+-------+----------+-------------------------------------------------+--------------+------------------------+-------------------------+-------------------------+--------------------------+---------------------+------+
| id                                   | type  | host            | user | state | database | extra_info                                      | memory_usage | dal_metrics_read_bytes | dal_metrics_write_bytes | scan_progress_read_rows | scan_progress_read_bytes | mysql_connection_id | time |
+--------------------------------------+-------+-----------------+------+-------+----------+-------------------------------------------------+--------------+------------------------+-------------------------+-------------------------+--------------------------+---------------------+------+
| c1152483-de11-4375-bfe3-a35ad2ae9311 | MySQL | 127.0.0.1:57636 | root | Query | default  | select sum(number) from numbers(10000000000000) |            0 |                      0 |                       0 |               816930000 |               6535440000 |                   9 |    4 |
| ed21393e-6b6b-4efe-b333-1643f531e8ac | MySQL | 127.0.0.1:57637 | root | Query | system   | show processlist                                |            0 |                      0 |                       0 |                       0 |                        0 |                  10 |    0 |
+--------------------------------------+-------+-----------------+------+-------+----------+-------------------------------------------------+--------------+------------------------+-------------------------+-------------------------+--------------------------+---------------------+------+
```
