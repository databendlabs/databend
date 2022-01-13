---
title: How to get server logs
---

If you get an error from the client, such as:

```mysql
ERROR 2013 (HY000): Lost connection to MySQL server during query
No connection. Trying to reconnect...
```

You can get the server logs from the `system.tracing` table(level=50 only shows the ERROR logs):

```sql
mysql> select * from system.tracing where level=50;
+------+----------------+----------------------------------------------------------------------------------------------------------------------------------+-------+----------+--------+-------------------------------------+
| v    | name           | msg                                                                                                                              | level | hostname | pid    | time                                |
+------+----------------+----------------------------------------------------------------------------------------------------------------------------------+-------+----------+--------+-------------------------------------+
|    0 | databend-query | [EXECUTE - EVENT] panicked at 'index out of bounds: the len is 1 but the index is 1', common/datablocks/src/data_block.rs:104:17 |    50 | thinkpad | 646495 | 2021-11-17T03:31:27.656710495+00:00 |
|    0 | databend-query | [EXECUTE - EVENT] panicked at 'index out of bounds: the len is 1 but the index is 1', common/datablocks/src/data_block.rs:104:17 |    50 | thinkpad | 646495 | 2021-11-17T03:31:27.703538995+00:00 |
|    0 | databend-query | [EXECUTE - EVENT] panicked at 'index out of bounds: the len is 1 but the index is 1', common/datablocks/src/data_block.rs:104:17 |    50 | thinkpad | 646495 | 2021-11-17T03:31:27.755246715+00:00 |
|    0 | databend-query | [EXECUTE - EVENT] panicked at 'index out of bounds: the len is 1 but the index is 1', common/datablocks/src/data_block.rs:104:17 |    50 | thinkpad | 646495 | 2021-11-17T03:31:51.861038285+00:00 |
|    0 | databend-query | [EXECUTE - EVENT] panicked at 'index out of bounds: the len is 1 but the index is 1', common/datablocks/src/data_block.rs:104:17 |    50 | thinkpad | 646495 | 2021-11-17T03:31:51.912497882+00:00 |
|    0 | databend-query | [EXECUTE - EVENT] panicked at 'index out of bounds: the len is 1 but the index is 1', common/datablocks/src/data_block.rs:104:17 |    50 | thinkpad | 646495 | 2021-11-17T03:31:51.956650623+00:00 |
+------+----------------+----------------------------------------------------------------------------------------------------------------------------------+-------+----------+--------+-------------------------------------+
```
