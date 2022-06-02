
---
title: Type Conversion
---

Type conversion to target type. 

## Syntax

```sql
TO_BOOLEAN( <expr> )
TO_DATE( <expr> )
TO_DATETIME( <expr> )
TO_TIMESTAMP( <expr> )
TO_FLOAT32( <expr> )
TO_FLOAT64( <expr> )
TO_INT8( <expr> )
TO_INT16( <expr> )
TO_INT32( <expr> )
TO_INT64( <expr> )
TO_NULL( <expr> )
TO_STRING( <expr> )
TO_UINT8( <expr> )
TO_UINT16( <expr> )
TO_UINT32( <expr> )
TO_UINT64( <expr> )
```

> `TO_DATETIME( <expr> )` and `TO_TIMESTAMP( <expr> )` uses the following rules to automatically determine the unit of time:
>
> - If the value is less than 31536000000, it is treated as a number of seconds,
> - If the value is greater than or equal to 31536000000 and less than 31536000000000, it is treated as milliseconds.
> - If the value is greater than or equal to 31536000000000, it is treated as microseconds.

## Examples

```sql
SELECT to_boolean('true');
+--------------------+
| to_boolean('true') |
+--------------------+
|                  1 |
+--------------------+

SELECT to_date(19109);
+----------------+
| to_date(19109) |
+----------------+
| 2022-04-27     |
+----------------+

SELECT to_datetime(1651036648);
+----------------------------+
| to_datetime(1651036648)    |
+----------------------------+
| 2022-04-27 05:17:28.000000 |
+----------------------------+

SELECT to_datetime(1651036648123);
+----------------------------+
| to_datetime(1651036648123) |
+----------------------------+
| 2022-04-27 05:17:28.123000 |
+----------------------------+

SELECT to_datetime(1651036648123456);
+-------------------------------+
| to_datetime(1651036648123456) |
+-------------------------------+
| 2022-04-27 05:17:28.123456    |
+-------------------------------+

SELECT to_float32('1.2');
+--------------------+
| to_float32('1.2')  |
+--------------------+
| 1.2000000476837158 |
+--------------------+

SELECT to_float64('1.2');
+-------------------+
| to_float64('1.2') |
+-------------------+
|               1.2 |
+-------------------+

SELECT to_int8('123');
+----------------+
| to_int8('123') |
+----------------+
|            123 |
+----------------+

SELECT to_null(10);
+-------------+
| to_null(10) |
+-------------+
|        NULL |
+-------------+

SELECT to_string(10);
+---------------+
| to_string(10) |
+---------------+
| 10            |
+---------------+
```
