---
title: Time and Date Types

---
| Data Type             | Size    |  Resolution | Min Value             | Max Value                     | Precision              |
| ----------------------| ------- |  ---------- | --------------------- |------------------------------ | ---------------------- |
| Date                  | 2 byte  |  day        | 1000-01-01            | 9999-12-31                    | YYYY-MM-DD             |
| Date32                | 4 byte  |  day        | 1000-01-01            | 9999-12-31                    | YYYY-MM-DD             |
| DateTime/DateTime32   | 4 byte  |  second     | 1970-01-01 00:00:00   | 2105-12-31 23:59:59           | YYYY-MM-DD hh:mm:ss    |
| DateTime64            | 8 byte  |  nanosecond | 1677-09-21 00:12:44.0 | 2262-04-11 23:47:16.854775804 | YYYY-MM-DD hh:mm:ss.ff |

For example:
```
CREATE TABLE dt
(
    d Date,
    t32 DateTime,
    t64 DateTime64,
    event_id UInt8
)
ENGINE = Memory;

INSERT INTO dt VALUES ('2021-09-09', '2021-09-09 01:01:01', '2021-12-21 01:01:01.123', 1);

mysql> select * from dt;
+------------+---------------------+-------------------------+----------+
| d          | t32                 | t64                     | event_id |
+------------+---------------------+-------------------------+----------+
| 2021-09-09 | 2021-09-09 01:01:01 | 2021-12-21 01:01:01.123 |        1 |
+------------+---------------------+-------------------------+----------+
```
