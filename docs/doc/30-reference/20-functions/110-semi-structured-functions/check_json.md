---
title: Check Json
---

Checks the validity of a JSON document.
If the input string is a valid JSON document or a `NULL`, the output is `NULL`.
If the input cannot be translated to a valid JSON value, the output string contains the error message.

## Syntax

```sql
check_json(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | An expression of string type

## Return Type

String

## Examples

```sql
mysql> select check_json('[1,2,3]');
+-----------------------+
| check_json('[1,2,3]') |
+-----------------------+
| NULL                  |
+-----------------------+
1 row in set (0.01 sec)

mysql> select check_json('{"key":"val"}');
+-----------------------------+
| check_json('{"key":"val"}') |
+-----------------------------+
| NULL                        |
+-----------------------------+
1 row in set (0.01 sec)

mysql> select check_json('{"key":');
+----------------------------------------------+
| check_json('{"key":')                        |
+----------------------------------------------+
| EOF while parsing a value at line 1 column 7 |
+----------------------------------------------+
1 row in set (0.01 sec)
```
