---
title: 'Conversion Functions'
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.39"/>

Below is a list of functions that allow you to convert an expression from one data type to another.

:::tip
Databend also offers a variety of functions for converting expressions into different date and time formats. Check out the **SQL Functions** > **Date & Time Functions** section to explore them.
:::

| Function                          | Description                                                                             | Example                            | Result                     |
|-----------------------------------|-----------------------------------------------------------------------------------------|------------------------------------|----------------------------|
| **CAST( expr AS data_type )**     | Converts a value from one data type to another                                          | **CAST(1 AS VARCHAR)**             | 1                          |
| **expr::data_type**               | Alias for CAST                                                                          | **1::VARCHAR**                     | 1                          |
| **TRY_CAST( expr AS data_type )** | Converts a value from one data type to another. Returns NULL on error.                  | **TRY_CAST(1 AS VARCHAR)**         | 1                          |
| **TO_BOOLEAN( expr )**            | Converts a value to BOOLEAN data type                                                   | **TO_BOOLEAN('true')**             | 1                          |
| **TO_FLOAT32( expr )**            | Converts a value to FLOAT32 data type                                                   | **TO_FLOAT32('1.2')**              | 1.2                        |
| **TO_FLOAT64( expr )**            | Converts a value to FLOAT64 data type                                                   | **TO_FLOAT64('1.2')**              | 1.2                        |
| **TO_INT8( expr )**               | Converts a value to INT8 data type                                                      | **TO_INT8('123')**                 | 123                        |
| **TO_INT16( expr )**              | Converts a value to INT16 data type                                                     | **TO_INT16('123')**                | 123                        |
| **TO_INT32( expr )**              | Converts a value to INT32 data type                                                     | **TO_INT32('123')**                | 123                        |
| **TO_INT64( expr )**              | Converts a value to INT64 data type                                                     | **TO_INT64('123')**                | 123                        |
| **TO_UINT8( expr )**              | Converts a value to UINT8 data type                                                     | **TO_UINT8('123')**                | 123                        |
| **TO_UINT16( expr )**             | Converts a value to UINT16 data type                                                    | **TO_UINT16('123')**               | 123                        |
| **TO_UINT32( expr )**             | Converts a value to UINT32 data type                                                    | **TO_UINT32('123')**               | 123                        |
| **TO_UINT64( expr )**             | Converts a value to UINT64 data type                                                    | **TO_UINT64('123')**               | 123                        |
| **TO_STRING( expr )**             | Converts a value to STRING data type                                                    | **TO_STRING(10)**                  | 10                         |
| **TO_STRING( expr, expr )**       | Converts a date value to a specific STRING format                                       | **TO_STRING('2022-12-25', 'Month/Day/Year: %m/%d/%Y')**| Month/Day/Year: 12/25/2022 |
