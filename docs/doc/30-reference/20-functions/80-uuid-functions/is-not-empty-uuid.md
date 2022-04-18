---
title: IS_NOT_EMPTY_UUID
---

Checks whether an UUID is not empty.

## Syntax

```sql
IS_NOT_EMPTY_UUID( <uuid> )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<uuid>` | A UUID string. |

## Return Type

If x is not empty, IS_NOT_EMPTY_UUID() returns 1, otherwise it returns 0.

## Examples

```sql
mysql> select is_not_empty_uuid(gen_random_uuid());
+--------------------------------------+
| is_not_empty_uuid(gen_random_uuid()) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+

mysql> select is_not_empty_uuid(gen_zero_uuid());
+------------------------------------+
| is_not_empty_uuid(gen_zero_uuid()) |
+------------------------------------+
|                                  0 |
+------------------------------------+
```
