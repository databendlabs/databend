---
title: Data Types
sidebar_position: 1
slug: ./
---

Databend is capable of handling both general and semi-structured data types.

## General Data Types

| Data Type | Alias  | Storage Size | Min Value                | Max Value                      | 
|-----------|--------|--------------|--------------------------|--------------------------------|
| BOOLEAN   | BOOL   | 1 byte       | N/A                      | N/A                            |
| TINYINT   | INT8   | 1 byte       | -128                     | 127                            |
| SMALLINT  | INT16  | 2 bytes      | -32768                   | 32767                          |
| INT       | INT32  | 4 bytes      | -2147483648              | 2147483647                     |
| BIGINT    | INT64  | 8 bytes      | -9223372036854775808     | 9223372036854775807            |
| FLOAT     | N/A    | 4 bytes      | -3.40282347e+38          | 3.40282347e+38                 |
| DOUBLE    | N/A    | 8 bytes      | -1.7976931348623157E+308 | 1.7976931348623157E+308        |
| DECIMAL   | N/A    | 16/32 bytes  | -10^P / 10^S             | 10^P / 10^S                    |
| DATE      | N/A    | 4 bytes      | 1000-01-01               | 9999-12-31                     |
| TIMESTAMP | N/A    | 8 bytes      | 0001-01-01 00:00:00      | 9999-12-31 23:59:59.999999 UTC |
| VARCHAR   | STRING | N/A          | N/A                      | N/A                            |

## Nested / Composite Types

| Data Type | Alias | Sample                           | Description                                                                       |
|-----------|-------|----------------------------------|-----------------------------------------------------------------------------------|
| ARRAY     | N/A   | `[1, 2, 3, 4]`                   | A collection of values of the same data type, accessed by their index.            |
| TUPLE     | N/A   | `('2023-02-14','Valentine Day')` | An ordered collection of values of different data types, accessed by their index. |
| MAP       | N/A   | `{"a":1, "b":2, "c":3}`          | A set of key-value pairs where each key is unique and maps to a value.            |                             |
| VARIANT   | JSON  | `[1,{"a":1,"b":{"c":2}}]`        | Collection of elements of different data types, including `ARRAY` and `OBJECT`.   |