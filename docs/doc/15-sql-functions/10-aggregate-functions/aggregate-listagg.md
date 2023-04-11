---
title: LISTAGG
---

Aggregate function.

The LISTAGG() function converts all the non-NULL values of a column to String, separated by the delimiter.

## Syntax

```sql
LISTAGG(expression)
LISTAGG(expression, delimiter)
```

## Arguments

| Arguments   | Description    |
| ----------- | -------------- |
| expression  | Any String expression |
| delimiter   | Optional constant String, if not specified, use empty String |

## Return Type

the String type

## Examples

```sql
CREATE TABLE aggr(a string null);

INSERT INTO aggr VALUES
    ('abc'),
    ('def'),
    (null),
    ('xyz');

SELECT listagg(a) FROM aggr;
+------------+
| listagg(a) |
+------------+
| abcdefxyz  |
+------------+

SELECT listagg(a, '|') FROM aggr;
+-----------------+
| listagg(a, '|') |
+-----------------+
| abc|def|xyz     |
+-----------------+
```
