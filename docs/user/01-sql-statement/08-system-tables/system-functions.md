---
title: system.functions
---


Contains information about scalar, aggregate and user defined functions.

```sql
mysql> SELECT * FROM system.functions limit 10;
+------------------+------------+--------------+------------+-------------+
| name             | is_builtin | is_aggregate | definition | description |
+------------------+------------+--------------+------------+-------------+
| format           |          1 |            0 |            |             |
| todatetime32     |          1 |            0 |            |             |
| quote            |          1 |            0 |            |             |
| blake3           |          1 |            0 |            |             |
| subtractmonths   |          1 |            0 |            |             |
| like             |          1 |            0 |            |             |
| right            |          1 |            0 |            |             |
| toyyyymmddhhmmss |          1 |            0 |            |             |
| abs              |          1 |            0 |            |             |
| sign             |          1 |            0 |            |             |
+------------------+------------+--------------+------------+-------------+
```