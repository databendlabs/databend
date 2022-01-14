---
title: system.functions
---


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
```