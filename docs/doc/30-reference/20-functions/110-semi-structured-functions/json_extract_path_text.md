---
title: JSON_EXTRACT_PATH_TEXT
---

Extracts value from a Json string by `path_name`.
The value is returned as a `String` or `NULL` if either of the arguments is `NULL`.
This function is equivalent to `to_varchar(GET_PATH(PARSE_JSON(JSON), PATH_NAME))`.

## Syntax

```sql
json_extract_path_text(expression, path_name)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | The Json String value
| path_name   | The String value that consists of a concatenation of field names

## Return Type

String

## Examples

```sql
mysql> select json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k1[0]');
+-------------------------------------------------------------------------+
| json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k1[0]') |
+-------------------------------------------------------------------------+
| 0                                                                       |
+-------------------------------------------------------------------------+
1 row in set (0.04 sec)

mysql> select json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k2:k3');
+-------------------------------------------------------------------------+
| json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k2:k3') |
+-------------------------------------------------------------------------+
| 3                                                                       |
+-------------------------------------------------------------------------+
1 row in set (0.05 sec)

mysql> select json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k2.k4');
+-------------------------------------------------------------------------+
| json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k2.k4') |
+-------------------------------------------------------------------------+
| 4                                                                       |
+-------------------------------------------------------------------------+
1 row in set (0.03 sec)

mysql> select json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k2.k5');
+-------------------------------------------------------------------------+
| json_extract_path_text('{"k1":[0,1,2], "k2":{"k3":3,"k4":4}}', 'k2.k5') |
+-------------------------------------------------------------------------+
| NULL                                                                    |
+-------------------------------------------------------------------------+
1 row in set (0.03 sec)
```
