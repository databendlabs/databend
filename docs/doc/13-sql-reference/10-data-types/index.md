---
title: Databend Data Types
sidebar_position: 1
slug: ./
---

Databend supports SQL data types in several categories:
* [Boolean Data Types](00-data-type-logical-types.md)
* [Numeric Data Types](10-data-type-numeric-types.md)
* [Decimal Data Types](11-data-type-decimal-types.md)
* [Date & Time Data Types](20-data-type-time-date-types.md)
* [String Data Types](30-data-type-string-types.md)
* [Array(T) Data Types](40-data-type-array-types.md)
* [Tuple Data Types](41-data-type-tuple-types.md)
* [Semi-structured Data Types](42-data-type-semi-structured-types.md)

## General-Purpose Data Types

| Name          | Aliases | Storage Size | Min Value                | Max Value                      | Description                                                             |
|---------------|---------|--------------|--------------------------|--------------------------------|-------------------------------------------------------------------------|
| **BOOLEAN**   | BOOL    | 1 byte       |                          |                                | Logical boolean (true/false)                                            |
| **TINYINT**   | INT8    | 1 byte       | -128                     | 127                            |                                                                         |
| **SMALLINT**  | INT16   | 2 bytes      | -32768                   | 32767                          |                                                                         |
| **INT**       | INT32   | 4 bytes      | -2147483648              | 2147483647                     |                                                                         |
| **BIGINT**    | INT64   | 8 bytes      | -9223372036854775808     | 9223372036854775807            |                                                                         |
| **FLOAT**     |         | 4 bytes      | -3.40282347e+38          | 3.40282347e+38                 |                                                                         |
| **DOUBLE**    |         | 8 bytes      | -1.7976931348623157E+308 | 1.7976931348623157E+308        |                                                                         |
| **DECIMAL**   |         | 16/32 bytes  |    -10^P / 10^S          |      10^P / 10^S               |                                                                         |
| **DATE**      |         | 4 bytes      | 1000-01-01               | 9999-12-31                     | YYYY-MM-DD                                                              |
| **TIMESTAMP** |         | 8 bytes      | 0001-01-01 00:00:00      | 9999-12-31 23:59:59.999999 UTC | YYYY-MM-DD hh:mm:ss[.fraction], up to microseconds (6 digits) precision |
| **VARCHAR**   | STRING  | variable     |                          |                                |                                                                         |
| **ARRAY**     |         |              |                          |                                | [1,2,3]                                                                 |
| **TUPLE**     |         |              |                          |                                | ('2023-02-14 08:00:00','Valentine's Day')                               |

## Semi-structured Data Types

| Name        | Aliases | Build From Values                         | Description                                                                                                 |
|-------------|---------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| **VARIANT** | JSON    | [1,{"a":1,"b":{"c":2}}]                   | Collection of elements of different data types., including ARRAY and OBJECT.                                |

