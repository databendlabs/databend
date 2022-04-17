---
title: Date & Time
description: Basic Date and Time data type.
---

## Date and Time Types

---
| Data Type   |  Syntax    | Size(Byte)    |  Resolution | Min Value             | Max Value                     | Format |
| ------------|------------| ------- |  ---------- | --------------------- |------------------------------ | ---------------------- |
| Date        |  DATE      | 2 |  day        | 1000-01-01            | 9999-12-31                    | YYYY-MM-DD             |
| DateTime    |  DATETIME  | 4 |  second     | 1970-01-01 00:00:00   | 2105-12-31 23:59:59           | YYYY-MM-DD hh:mm:ss    |
| DateTime64  |  TIMESTAMP | 8 |  nanosecond | 1677-09-21 00:12:44.000 | 2262-04-11 23:47:16.854     | YYYY-MM-DD hh:mm:ss.ff |

## Functions

See [Date & Time Functions](/doc/reference/functions/datetime-functions).

## Example
```sql
mysql> CREATE TABLE test_dt
(
    date DATE,
    datetime DATETIME,
    datetime64 TIMESTAMP 
);

mysql> DESC dt;
+------------+---------------+------+---------+
| Field      | Type          | Null | Default |
+------------+---------------+------+---------+
| date       | Date16        | NO   | 0       |
| datetime   | DateTime32    | NO   | 0       |
| datetime64 | DateTime64(3) | NO   | 0       |
+------------+---------------+------+---------+

mysql> INSERT INTO dt VALUES ('2022-04-07', '2022-04-07 01:01:01', '2022-04-07 01:01:01.123');

mysql> SELECT * FROM dt;
+------------+---------------------+-------------------------+
| date       | datetime            | datetime64              |
+------------+---------------------+-------------------------+
| 2022-04-07 | 2022-04-07 01:01:01 | 2022-04-07 01:01:01.123 |
+------------+---------------------+-------------------------+
```
