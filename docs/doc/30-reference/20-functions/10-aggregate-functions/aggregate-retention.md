---
title: RETENTION
---

Aggregate function

The RETENTION() function takes as arguments a set of conditions from 1 to 32 arguments of type UInt8 that indicate whether a certain condition was met for the event.

Any condition can be specified as an argument (as in WHERE).

The conditions, except the first, apply in pairs: the result of the second will be true if the first and second are true, of the third if the first and third are true, etc.

## Syntax

```
RETENTION( <cond1> , <cond2> , ..., <cond32> );
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<cond>`  | An expression that returns a Boolean result |

## Return Type

The array of 1 or 0.

## Examples

```
CREATE TABLE retention_test(date DATE, uid INT);
INSERT INTO retention_test SELECT '2018-08-06', number FROM numbers(80);
INSERT INTO retention_test SELECT '2018-08-07', number FROM numbers(70);
INSERT INTO retention_test SELECT '2018-08-08', number FROM numbers(60);
```

```
SELECT sum(r[0]) as r1, sum(r[1]) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07') AS r FROM retention_test WHERE date = '2018-08-06' or date = '2018-08-07' GROUP BY uid);
+------+------+
| r1   | r2   |
+------+------+
|   80 |   70 |
+------+------+

SELECT sum(r[0]) as r1, sum(r[1]) as r2 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-08') AS r FROM retention_test WHERE date = '2018-08-06' or date = '2018-08-08' GROUP BY uid);
+------+------+
| r1   | r2   |
+------+------+
|   80 |   60 |
+------+------+

SELECT sum(r[0]) as r1, sum(r[1]) as r2, sum(r[2]) as r3 FROM (SELECT uid, retention(date = '2018-08-06', date = '2018-08-07', date = '2018-08-08') AS r FROM retention_test GROUP BY uid);
+------+------+------+
| r1   | r2   | r3   |
+------+------+------+
|   80 |   70 |   60 |
+------+------+------+
```