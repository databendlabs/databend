---
title: Type Conversion
title_includes: toBoolean, toDate, toDateTime, toTimestamp, toFloat32, toFloat64, toInt8, toInt16, toInt32, toInt64, toNull, toString, toUInt8, toUInt16, toUInt32, toUInt64
---

Type conversion to target type. 

## Syntax

```sql
toBoolean(expr)
toDate(expr)
toDateTime(expr)
toTimestamp(expr)
toFloat32(expr)
toFloat64(expr)
toInt8(expr)
toInt16(expr)
toInt32(expr)
toInt64(expr)
toNull(expr)
toString(expr)
toUInt8(expr)
toUInt16(expr)
toUInt32(expr)
toUInt64(expr)
```

## Examples

```sql
MySQL [(none)]> select toBoolean('true');
+-------------------+
| toBoolean('true') |
+-------------------+
|                 1 |
+-------------------+

MySQL [(none)]> select toDate(19109);
+---------------+
| toDate(19109) |
+---------------+
| 2022-04-27    |
+---------------+

MySQL [(none)]> select toDateTime(1651036648000000);
+------------------------------+
| toDateTime(1651036648000000) |
+------------------------------+
| 2022-04-27 05:17:28.000000   |
+------------------------------+

MySQL [(none)]> select toTimestamp(toDateTime(1651045003000000));
+-------------------------------------------+
| toTimestamp(toDateTime(1651045003000000)) |
+-------------------------------------------+
| 2022-04-27 07:36:43.000000                |
+-------------------------------------------+

MySQL [(none)]> select toFloat32('1.2');
+--------------------+
| toFloat32('1.2')   |
+--------------------+
| 1.2000000476837158 |
+--------------------+

MySQL [(none)]> select toFloat64('1.2');
+------------------+
| toFloat64('1.2') |
+------------------+
|              1.2 |
+------------------+

MySQL [(none)]> select toInt8('123');
+---------------+
| toInt8('123') |
+---------------+
|           123 |
+---------------+

MySQL [(none)]> select toInt16('123');
+----------------+
| toInt16('123') |
+----------------+
|            123 |
+----------------+

MySQL [(none)]> select toInt32('123');
+----------------+
| toInt32('123') |
+----------------+
|            123 |
+----------------+

MySQL [(none)]> select toInt64('123');
+----------------+
| toInt64('123') |
+----------------+
|            123 |
+----------------+

MySQL [(none)]> select toNull(10);
+------------+
| toNull(10) |
+------------+
|       NULL |
+------------+

MySQL [(none)]> select toString(123);
+---------------+
| toString(123) |
+---------------+
| 123           |
+---------------+

MySQL [(none)]> select toUInt8('123');
+----------------+
| toUInt8('123') |
+----------------+
|            123 |
+----------------+

MySQL [(none)]> select toUInt16('123');
+-----------------+
| toUInt16('123') |
+-----------------+
|             123 |
+-----------------+

MySQL [(none)]> select toUInt32('123');
+-----------------+
| toUInt32('123') |
+-----------------+
|             123 |
+-----------------+

MySQL [(none)]> select toUInt64('123');
+-----------------+
| toUInt64('123') |
+-----------------+
|             123 |
+-----------------+
```
