---
title: Get Path
---

Extracts value from an `ARRAY`, an `OBJECT`, or a `VARIANT` by `path_name`.
The value is returned as a `Variant` or `NULL` if either of the arguments is `NULL`.

`GET_PATH` is equivalent to a chain of `GET` functions, `path_name` consists of a concatenation of field names preceded by periods (.), colons (:) or index operators (`[index]`). The first field name does not require the leading identifier to be specified.

## Syntax

```sql
get_path(array, path_name)
get_path(object, path_name)
get_path(variant, path_name)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| array       | The ARRAY value
| object      | The OBJECT value
| variant     | The VARIANT value that contains either an ARRAY or an OBJECT
| path_name   | The String value that consists of a concatenation of field names

## Return Type

Variant

## Examples

```sql
mysql> select get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k1[0]');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k1[0]') |
+-----------------------------------------------------------------------+
| 0                                                                     |
+-----------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2:k3');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2:k3') |
+-----------------------------------------------------------------------+
| 3                                                                     |
+-----------------------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k4');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k4') |
+-----------------------------------------------------------------------+
| 4                                                                     |
+-----------------------------------------------------------------------+
1 row in set (0.02 sec)

mysql> select get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k5');
+-----------------------------------------------------------------------+
| get_path(parse_json('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}'), 'k2.k5') |
+-----------------------------------------------------------------------+
| NULL                                                                  |
+-----------------------------------------------------------------------+
1 row in set (0.03 sec)
```
