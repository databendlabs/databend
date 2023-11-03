---
title: 'Conversion Functions'
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.187"/>

This page lists functions that allow you to convert an expression from one data type to another.

| Function                      | Description                                                            | Example                                             | Result                     |
|-------------------------------|------------------------------------------------------------------------|-----------------------------------------------------|----------------------------|
| CAST( expr AS data_type )     | Converts a value from one data type to another                         | CAST(1 AS VARCHAR)                                  | 1                          |
| expr::data_type               | Alias for CAST                                                         | 1::VARCHAR                                          | 1                          |
| TRY_CAST( expr AS data_type ) | Converts a value from one data type to another. Returns NULL on error. | TRY_CAST(1 AS VARCHAR)                              | 1                          |
| TO_BITMAP( expr )             | Converts a value to BITMAP data type                                   | TO_BITMAP('1101')                                   | 1101                       |
| BUILD_BITMAP( expr )          | Converts an array of positive integers to a BITMAP value                        | BUILD_BITMAP([1,4,5])::String | 1,4,5 |
| TO_BOOLEAN( expr )            | Converts a value to BOOLEAN data type                                  | TO_BOOLEAN('true')                                  | 1                          |
| TO_FLOAT32( expr )            | Converts a value to FLOAT32 data type                                  | TO_FLOAT32('1.2')                                   | 1.2                        |
| TO_FLOAT64( expr )            | Converts a value to FLOAT64 data type                                  | TO_FLOAT64('1.2')                                   | 1.2                        |
| TO_INT8( expr )               | Converts a value to INT8 data type                                     | TO_INT8('123')                                      | 123                        |
| TO_INT16( expr )              | Converts a value to INT16 data type                                    | TO_INT16('123')                                     | 123                        |
| TO_INT32( expr )              | Converts a value to INT32 data type                                    | TO_INT32('123')                                     | 123                        |
| TO_INT64( expr )              | Converts a value to INT64 data type                                    | TO_INT64('123')                                     | 123                        |
| TO_STRING( expr )             | Converts a value to STRING data type                                   | TO_STRING(10)                                       | 10                         |
| TO_STRING( expr, expr )       | Alias for [DATE_FORMAT](../30-datetime-functions/dateformat.md)         | TO_STRING('2022-12-25', 'Month/Day/Year: %m/%d/%Y') | Month/Day/Year: 12/25/2022 |
| TO_UINT8( expr )              | Converts a value to UINT8 data type                                    | TO_UINT8('123')                                     | 123                        |
| TO_UINT16( expr )             | Converts a value to UINT16 data type                                   | TO_UINT16('123')                                    | 123                        |
| TO_UINT32( expr )             | Converts a value to UINT32 data type                                   | TO_UINT32('123')                                    | 123                        |
| TO_UINT64( expr )             | Converts a value to UINT64 data type                                   | TO_UINT64('123')                                    | 123                        |
| TO_VARIANT( expr )            | Converts a value to VARIANT data type | TO_VARIANT(TO_BITMAP('100,200,300')) | [100,200,300] |

- When converting from floating-point, decimal numbers, or strings to integers or decimal numbers with fractional parts, Databend rounds the values to the nearest integer. This is determined by the setting `numeric_cast_option` (defaults to 'rounding') which controls the behavior of numeric casting operations. When `numeric_cast_option` is explicitly set to 'truncating', Databend will truncate the decimal part, discarding any fractional values.

    ```sql title='Example:'
    SELECT CAST('0.6' AS DECIMAL(10, 0)), CAST(0.6 AS DECIMAL(10, 0)), CAST(1.5 AS INT);

    ┌──────────────────────────────────────────────────────────────────────────────────┐
    │ cast('0.6' as decimal(10, 0)) │ cast(0.6 as decimal(10, 0)) │ cast(1.5 as int32) │
    ├───────────────────────────────┼─────────────────────────────┼────────────────────┤
    │                             1 │                           1 │                  2 │
    └──────────────────────────────────────────────────────────────────────────────────┘

    SET numeric_cast_option = 'truncating';

    SELECT CAST('0.6' AS DECIMAL(10, 0)), CAST(0.6 AS DECIMAL(10, 0)), CAST(1.5 AS INT);

    ┌──────────────────────────────────────────────────────────────────────────────────┐
    │ cast('0.6' as decimal(10, 0)) │ cast(0.6 as decimal(10, 0)) │ cast(1.5 as int32) │
    ├───────────────────────────────┼─────────────────────────────┼────────────────────┤
    │                             0 │                           0 │                  1 │
    └──────────────────────────────────────────────────────────────────────────────────┘
    ```

    The table below presents a summary of numeric casting operations, highlighting the casting possibilities between different source and target numeric data types. Please note that, it specifies the requirement for String to Integer casting, where the source string must contain an integer value.

    | Source Type    | Target Type |
    |----------------|-------------|
    | String         | Decimal     |
    | Float          | Decimal     |
    | Decimal        | Decimal     |
    | Float          | Int         |
    | Decimal        | Int         |
    | String (Int)   | Int         |


- Databend also offers a variety of functions for converting expressions into different date and time formats. For more information, see [Date & Time Functions](../30-datetime-functions/index.md).