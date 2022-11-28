---
title: GET_PATH
---

Extracts value from an `ARRAY`, an `OBJECT`, or a `VARIANT` by `path_name`. The value is returned as a `Variant` or `NULL` if either of the arguments is `NULL`.

`GET_PATH` is equivalent to a chain of `GET` functions, `path_name` consists of a concatenation of field names preceded by periods (.), colons (:) or index operators (`[index]`). The first field name does not require the leading identifier to be specified.

## Syntax

```sql
GET_PATH( <array>, <path_name> )
GET_PATH( <object>, <path_name> )
GET_PATH( <variant>, <path_name> )
```

## Arguments

| Arguments           | Description                                                      |
| ------------------- | ---------------------------------------------------------------- |
| `<array>`     | The ARRAY value                                                  |
| `<object>`    | The OBJECT value                                                 |
| `<variant>`   | The VARIANT value that contains either an ARRAY or an OBJECT     |
| `<path_name>` | The String value that consists of a concatenation of field names |

## Return Type

VARIANT

## Examples

```sql
SELECT get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k1[0]');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k1[0]') |
+-----------------------------------------------------------------------+
| 0                                                                     |
+-----------------------------------------------------------------------+

SELECT get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2:k3');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2:k3') |
+-----------------------------------------------------------------------+
| 3                                                                     |
+-----------------------------------------------------------------------+

SELECT get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k4');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k4') |
+-----------------------------------------------------------------------+
| 4                                                                     |
+-----------------------------------------------------------------------+

SELECT get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k5');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k5') |
+-----------------------------------------------------------------------+
| NULL                                                                  |
+-----------------------------------------------------------------------+
```
