---
title: Parse Json
---

`parse_json` and `try_parse_json` interprets an input string as a JSON document, producing a VARIANT value.

`try_parse_json` returns a NULL value if an error occurs during parsing.

## Syntax

```sql
parse_json(expression)
try_parse_json(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | An expression of string type (e.g. VARCHAR) that holds valid JSON information. |

## Return Type

Variant

## Examples

```sql
mysql> select parse_json('[-1, 12, 289, 2188, false]');
+------------------------------------------+
| parse_json('[-1, 12, 289, 2188, false]') |
+------------------------------------------+
| [-1,12,289,2188,false]                   |
+------------------------------------------+
1 row in set (0.01 sec)

mysql> select try_parse_json('{ "x" : "abc", "y" : false, "z": 10} ');
+---------------------------------------------------------+
| try_parse_json('{ "x" : "abc", "y" : false, "z": 10} ') |
+---------------------------------------------------------+
| {"x":"abc","y":false,"z":10}                            |
+---------------------------------------------------------+
1 row in set (0.01 sec)
```
