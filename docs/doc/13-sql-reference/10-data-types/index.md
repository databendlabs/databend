---
title: Data Types
---

Databend is capable of handling both general and semi-structured data types.

## General Data Types

| Data Type                                                           | Alias  | Storage Size | Min Value                | Max Value                      | 
|---------------------------------------------------------------------|--------|--------------|--------------------------|--------------------------------|
| [BOOLEAN](./00-data-type-logical-types.md)                          | BOOL   | 1 byte       | N/A                      | N/A                            |
| [TINYINT](./10-data-type-numeric-types.md#integer-data-types)       | INT8   | 1 byte       | -128                     | 127                            |
| [SMALLINT](./10-data-type-numeric-types.md#integer-data-types)      | INT16  | 2 bytes      | -32768                   | 32767                          |
| [INT](./10-data-type-numeric-types.md#integer-data-types)           | INT32  | 4 bytes      | -2147483648              | 2147483647                     |
| [BIGINT](./10-data-type-numeric-types.md#integer-data-types)        | INT64  | 8 bytes      | -9223372036854775808     | 9223372036854775807            |
| [FLOAT](./10-data-type-numeric-types.md#floating-point-data-types)  | N/A    | 4 bytes      | -3.40282347e+38          | 3.40282347e+38                 |
| [DOUBLE](./10-data-type-numeric-types.md#floating-point-data-types) | N/A    | 8 bytes      | -1.7976931348623157E+308 | 1.7976931348623157E+308        |
| [DECIMAL](./11-data-type-decimal-types.md)                          | N/A    | 16/32 bytes  | -10^P / 10^S             | 10^P / 10^S                    |
| [DATE](./20-data-type-time-date-types.md)                           | N/A    | 4 bytes      | 1000-01-01               | 9999-12-31                     |
| [TIMESTAMP](./20-data-type-time-date-types.md)                      | N/A    | 8 bytes      | 0001-01-01 00:00:00      | 9999-12-31 23:59:59.999999 UTC |
| [VARCHAR](./30-data-type-string-types.md)                           | STRING | N/A          | N/A                      | N/A                            |


## Nested / Composite Types

| Data Type                              | Alias | Sample                           | Description                                                                       |
|----------------------------------------|-------|----------------------------------|-----------------------------------------------------------------------------------|
| [ARRAY](./40-data-type-array-types.md) | N/A   | `[1, 2, 3, 4]`                   | A collection of values of the same data type, accessed by their index.            |
| [TUPLE](./41-data-type-tuple-types.md) | N/A   | `('2023-02-14','Valentine Day')` | An ordered collection of values of different data types, accessed by their index. |
| [MAP](./42-data-type-map.md)           | N/A   | `{"a":1, "b":2, "c":3}`          | A set of key-value pairs where each key is unique and maps to a value.            |                             |
| [VARIANT](./43-data-type-variant.md)   | JSON  | `[1,{"a":1,"b":{"c":2}}]`        | Collection of elements of different data types, including `ARRAY` and `OBJECT`.   |