---
title: SHOW SETTINGS
---

Shows the databend's SETTINGS.

You can change it by set command, like `set max_threads = 1`.

## Syntax

```
SHOW SETTINGS
```

## Examples

```sql
mysql> SHOW SETTINGS;
+-----------------------+-----------+
| name                  | value     |
+-----------------------+-----------+
| min_distributed_bytes | 524288000 |
| flight_client_timeout | 60        |
| max_threads           | 16        |
| max_block_size        | 10000     |
| min_distributed_rows  | 100000000 |
+-----------------------+-----------+
```
