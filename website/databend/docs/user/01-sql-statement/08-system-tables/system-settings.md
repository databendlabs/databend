---
title: system.settings
---


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
```