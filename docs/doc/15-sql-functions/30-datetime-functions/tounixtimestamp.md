---
title: TO_UNIX_TIMESTAMP
---

Converts a timestamp in a date/time format to a Unix timestamp format. A Unix timestamp represents the number of seconds that have elapsed since January 1, 1970, at 00:00:00 UTC.

## Syntax

```sql
to_unix_timestamp( <expr> )
```

## Arguments

| Arguments   | Description         |
| ----------- | ------------------- |
| `<expr>`    | Timestamp           |

For more information about the timestamp data type, see [Date & Time](../../13-sql-reference/10-data-types/20-data-type-time-date-types.md).

## Return Type

BIGINT

## Examples

```sql
SELECT TO_UNIX_TIMESTAMP(NOW())

----
1681089622


SELECT TO_UNIX_TIMESTAMP('2022-12-31T23:59:59+00:00')

----
1672531199


SELECT TO_UNIX_TIMESTAMP('2022-12-31T23:59:59-08:00')

----
1672559999
```