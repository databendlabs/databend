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

SELECT to_datetime(1651036648000000);
+-------------------------------+
| to_datetime(1651036648000000) |
+-------------------------------+
| 2022-04-27 05:17:28.000000    |
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
