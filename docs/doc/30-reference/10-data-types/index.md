---
title: Databend Data Types
sidebar_position: 1
slug: ./
---

Databend supports SQL data types in several categories:
* [Numeric Types](10-data-type-numeric-types.md)
* [Date & Time Types](20-data-type-time-date-types.md)
* [String Types](30-data-type-string-types.md)
* [Boolean Types](00-data-type-logical-types.md)
* [Semi-structured Types](40-data-type-semi-structured-types.md)

## General-Purpose Data Types

| Data Type | Syntax               | Size(byte)      | Min Value              | Max Value   | Format |
| ----------|----------------------| --------- | ---------------------- | ----------- | -------|
| Int8      | TINYINT              | 1 |  -128                  |  127 |
| UInt8     | TINYINT UNSIGNED     | 1 |  0                     |  255 |
| Int16     | SMALLINT             | 2 |  -32768                |  32767 |
| UInt16    | SMALLINT UNSIGNED    | 2 |  0                     |  65535 |
| Int32     | INT                  | 4 |  -2147483648           |  2147483647 |
| UInt32    | INT UNSIGNED         | 4 |  0                     |  4294967295 |
| Int64     | BIGINT               | 8 |  -9223372036854775808  |  9223372036854775807 |
| UInt64    | BIGINT UNSIGNED      | 8 |  0                     |  18446744073709551615 |
| Float32    |  FLOAT    | 4 |  -3.40282347e+38          | 3.40282347e+38 |
| Float64    |  DOUBLE   | 8 |  -1.7976931348623157E+308 | 1.7976931348623157E+308 |
| Date        |  DATE      | 2 |  1000-01-01            | 9999-12-31                    | YYYY-MM-DD             |
| DateTime    |  DATETIME  | 4 |  1970-01-01 00:00:00   | 2105-12-31 23:59:59           | YYYY-MM-DD hh:mm:ss    |
| DateTime64  |  TIMESTAMP | 8 |  1677-09-21 00:12:44.000 | 2262-04-11 23:47:16.854     | YYYY-MM-DD hh:mm:ss.ff |
| String           | VARCHAR | | | | | 

## Semi-structured Data Types

Databend supports three Semi-structured types: ARRAY, OBJECT and VARIANT.

| Data Type | Syntax  | Build from values
| ----------|---------|------------
| Array     | ARRAY   | [1,2,3]
| Object    | OBJECT  | {"a":1,"b":2}
| Variant   | VARIANT | {"a":1,"b":{"c":2}}
