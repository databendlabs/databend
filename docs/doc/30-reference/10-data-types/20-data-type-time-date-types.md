---
title: Date & Time
description: Basic Date and Time data type.
---

## Date and Time Data Types

---
|  Name | Storage Size |  Resolution  | Min Value             | Max Value                     | Description
|------------| ------- |  ----------- | --------------------- |--------------------------------- | ---------------------- |
|  DATE      | 4 bytes |  day         | 1000-01-01            | 9999-12-31                       | YYYY-MM-DD             |
|  TIMESTAMP | 8 bytes |  microsecond | 0001-01-01 00:00:00   | 9999-12-31 23:59:59.999999 UTC   | YYYY-MM-DD hh:mm:ss[.fraction], up to microseconds (6 digits) precision

## Functions

See [Date & Time Functions](/doc/reference/functions/datetime-functions).

## Example
```sql
CREATE TABLE test_dt
(
    date DATE,
    ts TIMESTAMP 
);


DESC test_dt;
+-------+--------------+------+---------+-------+
| Field | Type         | Null | Default | Extra |
+-------+--------------+------+---------+-------+
| date  | DATE         | NO   | 0       |       |
| ts    | TIMESTAMP(6) | NO   | 0       |       |
+-------+--------------+------+---------+-------+

INSERT INTO test_dt VALUES ('2022-04-07', '2022-04-07 01:01:01.123456'), ('2022-04-08', '2022-04-08 01:01:01');

SELECT * FROM TEST_DT;
+------------+----------------------------+
| date       | ts                         |
+------------+----------------------------+
| 2022-04-07 | 2022-04-07 01:01:01.123456 |
| 2022-04-08 | 2022-04-08 01:01:01.000000 |
+------------+----------------------------+
```
