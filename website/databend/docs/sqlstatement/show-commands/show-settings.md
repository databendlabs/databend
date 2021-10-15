---
id: show-settings
title: SHOW SETTINGS
---

Shows the databend's SETTINGS.

You can change it by set command, like `set max_threads = 1`.

## Syntax

```
SHOW SETTINGS;
```

## Examples

```
mysql> SHOW SETTINGS;
+------------------------------------------------------------------------------+
| name                                                                         |
+------------------------------------------------------------------------------+
| min_distributed_bytes                                                        |
| max_block_size                                                               |
| flight_client_timeout                                                        |
| min_distributed_rows                                                         |
| max_threads                                                                  |
+------------------------------------------------------------------------------+
```
