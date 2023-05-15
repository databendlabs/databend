---
title: 'Conversion Functions'
---

| Function                          | Description                                                                             | Example                            | Result                     |
|-----------------------------------|-----------------------------------------------------------------------------------------|------------------------------------|----------------------------|
| **CAST( expr AS data_type )**     | Converts a value from one data type to another                                          | **CAST(1 AS VARCHAR)**             | 1                          |
| **expr::data_type**               | Alias for CAST                                                                          | **1::VARCHAR**                     | 1                          |
| **TRY_CAST( expr AS data_type )** | Converts a value from one data type to another. Returns NULL on error.                  | **TRY_CAST(1 AS VARCHAR)**         | 1                          |
| **TO_BOOLEAN( expr )**            | Converts a value to BOOLEAN data type                                                   | **TO_BOOLEAN('true')**             | 1                          |
| **TO_DATE( expr )**               | Converts a value to DATE data type                                                      | **TO_DATE(19109)**                 | 2022-04-27                 |
| **TO_DATE( expr, expr )**         | Converts a patterned string into a date format                                          | **TO_DATE('Month/Day/Year: 12/25/2022','Month/Day/Year: %m/%d/%Y')**| 2022-12-25 |
| **TO_DATETIME( expr )**           | Converts a value to DATETIME data type                                                  | **TO_DATETIME(1651036648)**        | 2022-04-27 05:17:28.000000 |
| **TO_TIMESTAMP( expr )**          | Alias for TO_DATETIME                                                                   | **TO_TIMESTAMP(1651036648123456)** | 2022-04-27 05:17:28.123456 |
| **TO_TIMESTAMP( expr, expr )**    | Converts a patterned string into a timestamp format                                     | **TO_TIMESTAMP('2022年2月4日、8時58分59秒、タイムゾーン：+0900', '%Y年%m月%d日、%H時%M分%S秒、タイムゾーン：%z');** | 2022-02-04 08:58:59.000000 |
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
| **TO_STRING( expr, expr )**       | Converts a date value into a specific string format                                     | **TO_STRING('2022-12-25', 'Month/Day/Year: %m/%d/%Y')**| Month/Day/Year: 12/25/2022 |

:::note
`TO_DATETIME( expr )` and `TO_TIMESTAMP( expr )` use the following rules to automatically determine the unit of time:

- If the value is less than 31536000000, it is treated as a number of seconds,
- If the value is greater than or equal to 31536000000 and less than 31536000000000, it is treated as milliseconds.
- If the value is greater than or equal to 31536000000000, it is treated as microseconds.

`TO_DATE( expr, expr )` must include the timezone information in the patterned string, otherwise NULL will be returned. The output timestamp reflects your Databend timezone. See an example below:

```sql
SET GLOBAL timezone ='Japan';
SELECT TO_TIMESTAMP('2022年2月4日、8時58分59秒、タイムゾーン：+0900', '%Y年%m月%d日、%H時%M分%S秒、タイムゾーン：%z');

---
2022-02-04 08:58:59.000000

SET GLOBAL timezone ='America/Toronto';
SELECT TO_TIMESTAMP('2022年2月4日、8時58分59秒、タイムゾーン：+0900', '%Y年%m月%d日、%H時%M分%S秒、タイムゾーン：%z');

---
2022-02-03 18:58:59.000000
```
:::