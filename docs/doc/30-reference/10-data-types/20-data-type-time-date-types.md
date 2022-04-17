---
title: Date & Time Data Types
description: Basic Date and Time data type.
---

## Date and Time

---
| Data Type   |  Syntax    | Size    |  Resolution | Min Value             | Max Value                     | Precision              |
| ------------|------------| ------- |  ---------- | --------------------- |------------------------------ | ---------------------- |
| Date        |  DATE      | 2 byte  |  day        | 1000-01-01            | 9999-12-31                    | YYYY-MM-DD             |
| DateTime    |  DATETIME  | 4 byte  |  second     | 1970-01-01 00:00:00   | 2105-12-31 23:59:59           | YYYY-MM-DD hh:mm:ss    |
| DateTime64  |  TIMESTAMP | 8 byte  |  nanosecond | 1677-09-21 00:12:44.000 | 2262-04-11 23:47:16.854     | YYYY-MM-DD hh:mm:ss.ff |

## Example
```sql
mysql> create table test_dt
(
    date date,
    datetime dateTime,
    datetime64  timestamp 
);

mysql> desc dt;
+------------+---------------+------+---------+
| Field      | Type          | Null | Default |
+------------+---------------+------+---------+
| date       | Date16        | NO   | 0       |
| datetime   | DateTime32    | NO   | 0       |
| datetime64 | DateTime64(3) | NO   | 0       |
+------------+---------------+------+---------+

mysql> insert into dt values ('2022-04-07', '2022-04-07 01:01:01', '2022-04-07 01:01:01.123');

mysql> select * from dt;
+------------+---------------------+-------------------------+
| date       | datetime            | datetime64              |
+------------+---------------------+-------------------------+
| 2022-04-07 | 2022-04-07 01:01:01 | 2022-04-07 01:01:01.123 |
+------------+---------------------+-------------------------+
```
