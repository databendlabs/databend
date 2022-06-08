---
title: TIMEZONE
---

Return the timezone for the connection.

## Syntax

```
SELECT TIMEZONE()
```

## Examples

```sql
mysql> SELECT TIMEZONE();
+-----------------+
| TIMEZONE('UTC') |
+-----------------+
| UTC             |
+-----------------+

mysql> SET timezone='Asia/Shanghai';

mysql> SELECT TIMEZONE();
+---------------------------+
| TIMEZONE('Asia/Shanghai') |
+---------------------------+
| Asia/Shanghai             |
+---------------------------+

```
