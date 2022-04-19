---
title: CHECK_JSON
---

Checks the validity of a JSON document.
If the input string is a valid JSON document or a `NULL`, the output is `NULL`.
If the input cannot be translated to a valid JSON value, the output string contains the error message.

## Syntax

```sql
CHECK_JSON(<expr>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>` | An expression of string type

## Return Type

String

## Examples

```sql
SELECT check_json('[1,2,3]');
+-----------------------+
| check_json('[1,2,3]') |
+-----------------------+
| NULL                  |
+-----------------------+
1 row in set (0.01 sec)

SELECT check_json('{"key":"val"}');
+-----------------------------+
| check_json('{"key":"val"}') |
+-----------------------------+
| NULL                        |
+-----------------------------+
1 row in set (0.01 sec)

SELECT check_json('{"key":');
+----------------------------------------------+
| check_json('{"key":')                        |
+----------------------------------------------+
| EOF while parsing a value at line 1 column 7 |
+----------------------------------------------+
1 row in set (0.01 sec)
```
