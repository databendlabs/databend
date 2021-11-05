---
id: show-processlist
title: SHOW PROCESSLIST
---

The Databend process list indicates the operations currently being performed by the set of threads executing within the server.

The SHOW PROCESSLIST statement is one source of process information.

## Syntax

```
SHOW PROCESSLIST
```

## Examples

```
mysql> SHOW PROCESSLIST;
+--------------------------------------+-----------------+-------+----------+------------------+------------------+
| id                                   | host            | state | database | extra_info       | memory_usage     |
+--------------------------------------+-----------------+-------+----------+------------------+------------------+
| 1e6e5ed4-5441-43da-9ed6-eb6ba9baeb64 | 127.0.0.1:60080 | Query | default  | show processlist | 1234567891011121 |
| 3d283add-4f60-416d-b9ca-662120614093 | 127.0.0.1:57018 | Query | default  | NULL             | 1234567891011121 |
+--------------------------------------+-----------------+-------+----------+------------------+------------------+
```
