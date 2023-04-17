---
title: 'Conversion Functions'
---

SQL Conversion Functions Overview.

| Function                          | Description                                                                            | Example                            | Result                     |
|-----------------------------------|----------------------------------------------------------------------------------------|------------------------------------|----------------------------|
| **CAST( expr AS data_type )**     | Convert a value from one data type to another data type                                | **CAST(1 AS VARCHAR)**             | 1                          |
| **expr::data_type**               | alias for CAST                                                                         | **1::VARCHAR**                     | 1                          |
| **TRY_CAST( expr AS data_type )** | Convert a value from one data type to another data type. If error happens, return NULL | **TRY_CAST(1 AS VARCHAR)**         | 1                          |
| **TO_BOOLEAN( expr )**            | Convert a value to BOOLEAN data type                                                   | **TO_BOOLEAN('true')**             | 1                          |
| **TO_DATE( expr )**               | Convert a value to DATE data type                                                      | **TO_DATE(19109)**                 | 2022-04-27                 |
| **TO_DATETIME( expr )**           | Convert a value to DATETIME data type                                                  | **TO_DATETIME(1651036648)**        | 2022-04-27 05:17:28.000000 |
| **TO_TIMESTAMP( expr )**          | alias for TO_DATETIME                                                                  | **TO_TIMESTAMP(1651036648123456)** | 2022-04-27 05:17:28.123456 |
| **TO_FLOAT32( expr )**            | Convert a value to FLOAT32 data type                                                   | **TO_FLOAT32('1.2')**              | 1.2                        |
| **TO_FLOAT64( expr )**            | Convert a value to FLOAT64 data type                                                   | **TO_FLOAT64('1.2')**              | 1.2                        |
| **TO_INT8( expr )**               | Convert a value to INT8 data type                                                      | **TO_INT8('123')**                 | 123                        |
| **TO_INT16( expr )**              | Convert a value to INT16 data type                                                     | **TO_INT16('123')**                | 123                        |
| **TO_INT32( expr )**              | Convert a value to INT32 data type                                                     | **TO_INT32('123')**                | 123                        |
| **TO_INT64( expr )**              | Convert a value to INT64 data type                                                     | **TO_INT64('123')**                | 123                        |
| **TO_UINT8( expr )**              | Convert a value to UINT8 data type                                                     | **TO_UINT8('123')**                | 123                        |
| **TO_UINT16( expr )**             | Convert a value to UINT16 data type                                                    | **TO_UINT16('123')**               | 123                        |
| **TO_UINT32( expr )**             | Convert a value to UINT32 data type                                                    | **TO_UINT32('123')**               | 123                        |
| **TO_UINT64( expr )**             | Convert a value to UINT64 data type                                                    | **TO_UINT64('123')**               | 123                        |
| **TO_STRING( expr )**             | Convert a value to STRING data type                                                    | **TO_STRING(10)**                  | 10                         |


:::note

`TO_DATETIME( expr )` and `TO_TIMESTAMP( expr )` uses the following rules to automatically determine the unit of time:

- If the value is less than 31536000000, it is treated as a number of seconds,
- If the value is greater than or equal to 31536000000 and less than 31536000000000, it is treated as milliseconds.
- If the value is greater than or equal to 31536000000000, it is treated as microseconds.

:::
