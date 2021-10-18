---
id: crashme
title: CRASHME
---

CRASHME function makes a crash,.

!!! warning
    Only used for testing where panic is required.

    This function is very useful for distributed query stability testing, we can trigger panic by hand.

    Currently CRASHME function there is no permission restrictions.

## Syntax

```sql
crashme(expression )
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression | Any expression, like sleep(1)

## Return Type

Null

## Examples

```
mysql> SELECT * FROM (SELECT crashme(sleep(1)));;

ERROR 2013 (HY000) at line 2: Lost connection to MySQL server during query
```
