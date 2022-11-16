---
title: REPEAT
---

Returns a string consisting of the string str repeated count times. If count is less than 1, returns an empty string. Returns NULL if str or count are NULL.

## Syntax

```sql
REPEAT(str, count)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| str         | The string. |
| count       | The number. |

## Examples

```sql
SELECT REPEAT('databend', 3);
+--------------------------+
| REPEAT('databend', 3)    |
+--------------------------+
| databenddatabenddatabend |
+--------------------------+

SELECT REPEAT('databend', 0);
+-----------------------+
| REPEAT('databend', 0) |
+-----------------------+
|                       |
+-----------------------+

SELECT REPEAT('databend', NULL);
+--------------------------+
| REPEAT('databend', NULL) |
+--------------------------+
|                     NULL |
+--------------------------+
```


