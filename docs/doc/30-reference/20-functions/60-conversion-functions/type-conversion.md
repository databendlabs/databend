---
title: Type Conversion
---

to_int(8|16|32|64)

## Syntax

```sql
to_int8(expr) — Results in the Int8 data type.
to_int16(expr) — Results in the Int16 data type.
to_int32(expr) — Results in the Int32 data type.
to_int64(expr) — Results in the Int64 data type.
```

## Examples

```sql
SELECT to_int8(1);
+-----------+
| to_int8(1) |
+-----------+
|         1 |
+-----------+
```
