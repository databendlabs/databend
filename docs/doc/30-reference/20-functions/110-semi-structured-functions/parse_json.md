---
title: PARSE_JSON
description:
  Interprets input JSON string, producing a VARIANT value
---

`parse_json` and `try_parse_json` interprets an input string as a JSON document, producing a VARIANT value.

`try_parse_json` returns a NULL value if an error occurs during parsing.

## Syntax

```sql
PARSE_JSON(<expr>)
TRY_PARSE_JSON(<expr>)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| `<expr>`| An expression of string type (e.g. VARCHAR) that holds valid JSON information. |

## Return Type

VARIANT

## Examples

```sql
SELECT parse_json('[-1, 12, 289, 2188, false]');
+------------------------------------------+
| parse_json('[-1, 12, 289, 2188, false]') |
+------------------------------------------+
| [-1,12,289,2188,false]                   |
+------------------------------------------+
1 row in set (0.01 sec)

SELECT try_parse_json('{ "x" : "abc", "y" : false, "z": 10} ');
+---------------------------------------------------------+
| try_parse_json('{ "x" : "abc", "y" : false, "z": 10} ') |
+---------------------------------------------------------+
| {"x":"abc","y":false,"z":10}                            |
+---------------------------------------------------------+
1 row in set (0.01 sec)
```
