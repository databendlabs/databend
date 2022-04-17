---
title: Numeric Data Types
description: Basic Numeric data type.
---

## Integer Number

Basic Integer Numbers data types.

| Data Type | Syntax               | Size      | Min Value              | Max Value   |
| ----------|----------------------| --------- | ---------------------- | ----------- |
| Int8      | TINYINT              | 1 byte    |  -128                  |  127        
| UInt8     | TINYINT UNSIGNED     | 1 byte    |  0                     |  255
| Int16     | SMALLINT             | 2 byte    |  -32768                |  32767
| UInt16    | SMALLINT UNSIGNED    | 2 byte    |  0                     |  65535
| Int32     | INT                  | 4 byte    |  -2147483648           |  2147483647
| UInt32    | INT UNSIGNED         | 4 byte    |  0                     |  4294967295
| Int64     | BIGINT               | 8 byte    |  -9223372036854775808  |  9223372036854775807
| UInt64    | BIGINT UNSIGNED      | 8 byte    |  0                     |  18446744073709551615

## Floating Point Number

Basic Float32/Float64 data types.

| Data Type  | Syntax    | Size    |  Min Value                |  Max Value |
| -----------|-----------| ------- |  ------------------------ |------------
| Float32    |  FLOAT    | 4 byte  |  -3.40282347e+38          | 3.40282347e+38
| Float64    |  DOUBLE   | 8 byte  |  -1.7976931348623157E+308 | 1.7976931348623157E+308

## Examples

```sql
mysql> create table test_numeric(tiny tinyint, tiny_unsigned tinyint unsigned, smallint smallint, smallint_unsigned smallint unsigned, int int, int_unsigned int unsigned, bigint bigint, bigint_unsigned bigint unsigned);

mysql> desc test_numeric;
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

