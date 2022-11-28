---
title: IS_EMPTY_UUID
---

Checks whether an UUID is empty.

## Syntax

```sql
IS_EMPTY_UUID( <uuid> )
```

## Arguments

| Arguments      | Description    |
| -------------- | -------------- |
| `<uuid>` | A UUID string. |

## Return Type

If uuid is empty, IS_EMPTY_UUID() returns 1, otherwise it returns 0.

## Examples

```sql
SELECT is_empty_uuid(gen_random_uuid());
+----------------------------------+
| is_empty_uuid(gen_random_uuid()) |
+----------------------------------+
|                                0 |
+----------------------------------+

SELECT is_empty_uuid(gen_zero_uuid());
+--------------------------------+
| is_empty_uuid(gen_zero_uuid()) |
+--------------------------------+
|                              1 |
+--------------------------------+
```
