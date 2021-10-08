---
id: data-type-time-date-types
title: Time and Date Types

---
| Data Type             | Size    |  Resolution | Min Value           | Max Value           | Precision           |
| ----------------------| ------- |  ---------- | ------------------- |-------------------- | ------------------- |
| Date                  | 2 byte  |  day        | 1000-01-01          | 9999-12-31          | YYYY-MM-DD          |
| Date32                | 4 byte  |  day        | 1000-01-01          | 9999-12-31          | YYYY-MM-DD          |
| DateTime/DateTime32   | 4 byte  |  second     | 1970-01-01 00:00:00 | 2105-12-31 23:59:59 | YYYY-MM-DD hh:mm:ss |
| DateTime/DateTime32   | 4 byte  |  second     | 1970-01-01 00:00:00 | 2105-12-31 23:59:59 | YYYY-MM-DD hh:mm:ss |


For example:
```
CREATE TABLE dt
(
    d Date,
    t DateTime,
    event_id UInt8
)
ENGINE = Memory;

INSERT INTO dt VALUES ('2021-09-09', '2021-09-09 01:01:01', 1);

mysql> select * from dt;
+------------+---------------------+----------+
| d          | t                   | event_id |
+------------+---------------------+----------+
| 2021-09-09 | 2021-09-09 01:01:01 |        1 |
+------------+---------------------+----------+
```
