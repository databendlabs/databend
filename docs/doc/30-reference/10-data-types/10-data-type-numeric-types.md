---
title: Numeric
description: Basic Numeric data type.
---

## Integer Data Types

Basic Integer Numbers data types.

| Data Type | Syntax               | Size(Bytes)      | Min Value              | Max Value   |
| ----------|----------------------| --------- | ---------------------- | ----------- |
| Int8      | TINYINT              | 1 |  -128                  |  127        
| UInt8     | TINYINT UNSIGNED     | 1 |  0                     |  255
| Int16     | SMALLINT             | 2 |  -32768                |  32767
| UInt16    | SMALLINT UNSIGNED    | 2 |  0                     |  65535
| Int32     | INT                  | 4 |  -2147483648           |  2147483647
| UInt32    | INT UNSIGNED         | 4 |  0                     |  4294967295
| Int64     | BIGINT               | 8 |  -9223372036854775808  |  9223372036854775807
| UInt64    | BIGINT UNSIGNED      | 8 |  0                     |  18446744073709551615

## Floating-Point Data Types

Basic Float32/Float64 data types.

| Data Type  | Syntax    | Size(Bytes)    |  Min Value                |  Max Value |
| -----------|-----------| ------- |  ------------------------ |------------
| Float32    |  FLOAT    | 4 |  -3.40282347e+38          | 3.40282347e+38
| Float64    |  DOUBLE   | 8 |  -1.7976931348623157E+308 | 1.7976931348623157E+308

## Functions

See [Numeric Functions](/doc/reference/functions/numeric-functions).

## Examples

```sql
mysql> CREATE TABLE test_numeric(tiny TINYINT, tiny_unsigned TINYINT UNSIGNED, smallint SMALLINT, smallint_unsigned SMALLINT UNSIGNED, int INT, int_unsigned INT UNSIGNED, bigint BIGINT, bigint_unsigned BIGINT UNSIGNED);

mysql> DESC test_numeric;
+-------------------+--------+------+---------+
| Field             | Type   | Null | Default |
+-------------------+--------+------+---------+
| tiny              | Int8   | NO   | 0       |
| tiny_unsigned     | UInt8  | NO   | 0       |
| smallint          | Int16  | NO   | 0       |
| smallint_unsigned | UInt16 | NO   | 0       |
| int               | Int32  | NO   | 0       |
| int_unsigned      | UInt32 | NO   | 0       |
| bigint            | Int64  | NO   | 0       |
| bigint_unsigned   | UInt64 | NO   | 0       |
+-------------------+--------+------+---------+
```

