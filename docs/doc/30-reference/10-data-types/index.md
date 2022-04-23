---
title: Databend Data Types
sidebar_position: 1
slug: ./
---

Databend supports SQL data types in several categories:
* [Boolean Data Types](00-data-type-logical-types.md)
* [Numeric Data Types](10-data-type-numeric-types.md)
* [Date & Time Data Types](20-data-type-time-date-types.md)
* [String Data Types](30-data-type-string-types.md)
* [Semi-structured Data Types](40-data-type-semi-structured-types.md)

## General-Purpose Data Types

| Name                 | Storage Size | Min Value                   | Max Value               | Description |
|----------------------| -------------| --------------------------- | ----------------------- | -------|
| BOOLEAN              | 1 byte       |                             |                         | Logical boolean (true/false)
| TINYINT              | 1 byte       |  -128                       |  127                    | 
| SMALLINT             | 2 bytes      |  -32768                     |  32767                  |
| INT                  | 4 bytes      |  -2147483648                |  2147483647             |
| BIGINT               | 8 bytes      |  -9223372036854775808       |  9223372036854775807    |
| FLOAT                | 4 bytes      |  -3.40282347e+38            | 3.40282347e+38          |
| DOUBLE               | 8 bytes      |  -1.7976931348623157E+308   | 1.7976931348623157E+308 |
|  DATE                | 4 bytes      |  1000-01-01                 | 9999-12-31                       | YYYY-MM-DD             
|  TIMESTAMP           | 8 bytes      |  0001-01-01 00:00:00        | 9999-12-31 23:59:59.999999 UTC   | YYYY-MM-DD hh:mm:ss[.fraction], up to microseconds (6 digits) precision
| VARCHAR              | variable     | | | | 



## Semi-structured Data Types

Databend supports three Semi-structured types: ARRAY, OBJECT and VARIANT.

| Name | Build From Values    | Description
|---------|----------------------|----------------
| ARRAY   | [1,2,3]              | Zero-based indexed list, each value can have difference data type.
| OBJECT  | {"a":1,"b":{"c":2}}  | Collection of key-value pairs, each key is a VARCHAR, and each value is a VARIANT.
| VARIANT | [1,{"a":1,"b":{"c":2}}] | Collection of elements of different data types., including ARRAY and OBJECT.
