---
title: Date & Time
description: Basic Date and Time data type.
---

## Date and Time Data Types

|  Name      | Aliases   | Storage Size |  Resolution  | Min Value             | Max Value                      | Description
|----------- | --------- |  ----------- | -------------|-----------------------| -----------------------------  |
|  DATE      |           | 4 bytes      |  day         | 1000-01-01            | 9999-12-31                     | YYYY-MM-DD             |
|  TIMESTAMP |           | 8 bytes      |  microsecond | 1000-01-01 00:00:00   | 9999-12-31 23:59:59.999999 UTC | YYYY-MM-DD hh:mm:ss[.fraction], up to microseconds (6 digits) precision

## Functions

See [Date & Time Functions](/doc/reference/functions/datetime-functions).

## Example

```sql
CREATE TABLE test_dt 
  ( 
     date DATE, 
     ts   TIMESTAMP 
  ); 

DESC test_dt;
+-------+--------------+------+---------+-------+
| Field | Type         | Null | Default | Extra |
+-------+--------------+------+---------+-------+
| date  | DATE         | NO   | 0       |       |
| ts    | TIMESTAMP(6) | NO   | 0       |       |
+-------+--------------+------+---------+-------+

-- A TIMESTAMP value can optionally include a trailing fractional seconds part in up to microseconds (6 digits) precision.

INSERT INTO test_dt 
VALUES      ('2022-04-07', 
             '2022-04-07 01:01:01.123456'), 
            ('2022-04-08', 
             '2022-04-08 01:01:01'); 

SELECT * 
FROM   test_dt; 
+------------+----------------------------+
| date       | ts                         |
+------------+----------------------------+
| 2022-04-07 | 2022-04-07 01:01:01.123456 |
| 2022-04-08 | 2022-04-08 01:01:01.000000 |
+------------+----------------------------+

-- Databend recognizes TIMESTAMP values in several formats.

CREATE TABLE test_formats 
  ( 
     id INT, 
     a  TIMESTAMP 
  ); 

INSERT INTO test_formats 
VALUES      (1, 
             '2022-01-01 02:00:11'), 
            (2, 
             '2022-01-02T02:00:22'), 
            (3, 
             '2022-02-02T04:00:03+00:00'), 
            (4, 
             '2022-02-03'); 

SELECT * 
FROM   test_formats; 

 ----
 1  2022-01-01 02:00:11.000000
 2  2022-01-02 02:00:22.000000
 3  2022-02-02 04:00:03.000000
 4  2022-02-03 00:00:00.000000

-- Databend automatically adjusts and shows TIMESTAMP values based on your current timezone.

CREATE TABLE test_tz 
  ( 
     id INT, 
     t  TIMESTAMP 
  ); 

SET timezone='UTC';

INSERT INTO test_tz 
VALUES      (1, 
             '2022-02-03T03:00:00'), 
            (2, 
             '2022-02-03T03:00:00+08:00'), 
            (3, 
             '2022-02-03T03:00:00-08:00'), 
            (4, 
             '2022-02-03'), 
            (5, 
             '2022-02-03T03:00:00+09:00'), 
            (6, 
             '2022-02-03T03:00:00+06:00'); 

SELECT * 
FROM   test_tz; 

 ----
 1  2022-02-03 03:00:00.000000
 2  2022-02-02 19:00:00.000000
 3  2022-02-03 11:00:00.000000
 4  2022-02-03 00:00:00.000000
 5  2022-02-02 18:00:00.000000
 6  2022-02-02 21:00:00.000000

SET timezone='Asia/Shanghai';

SELECT * 
FROM   test_tz; 

 ----
 1  2022-02-03 11:00:00.000000
 2  2022-02-03 03:00:00.000000
 3  2022-02-03 19:00:00.000000
 4  2022-02-03 08:00:00.000000
 5  2022-02-03 02:00:00.000000
 6  2022-02-03 05:00:00.000000
```