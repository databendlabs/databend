---
title: STRING_AGG
---

Aggregate function.

The STRING_AGG() function converts all the non-NULL values of a column to String, separated by the delimiter.

## Syntax

```sql
STRING_AGG(expression)
STRING_AGG(expression, delimiter)
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

SELECT string_agg(a) FROM aggr;
+---------------+
| string_agg(a) |
+---------------+
| abcdefxyz     |
+---------------+

SELECT string_agg(a, '|') FROM aggr;
+--------------------+
| string_agg(a, '|') |
+--------------------+
| abc|def|xyz        |
+--------------------+
```
