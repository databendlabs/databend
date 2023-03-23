---
title: CONCAT_WS
---

CONCAT_WS() stands for Concatenate With Separator and is a special form of CONCAT(). The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.

CONCAT_WS() does not skip empty strings. However, it does skip any NULL values after the separator argument.

## Syntax

```sql
CONCAT_WS(separator, column1, ...)
```

## Arguments

| Arguments   | Description   |
| ----------- | ------------- |
| separator   | string column |
| column      | value column  |

## Return Type

A String data type value Or Null data type.

## Examples

```sql
SELECT CONCAT_WS(',', 'data', 'fuse', 'labs', '2021');
+------------------------------------------------+
| CONCAT_WS(',', 'data', 'fuse', 'labs', '2021') |
+------------------------------------------------+
| data,fuse,labs,2021                            |
+------------------------------------------------+

SELECT CONCAT_WS(',', 'data', NULL, 'bend');
+--------------------------------------+
| CONCAT_WS(',', 'data', NULL, 'bend') |
+--------------------------------------+
| data,bend                            |
+--------------------------------------+


SELECT CONCAT_WS(',', 'data', NULL, NULL, 'bend');
+--------------------------------------------+
| CONCAT_WS(',', 'data', NULL, NULL, 'bend') |
+--------------------------------------------+
| data,bend                                  |
+--------------------------------------------+


SELECT CONCAT_WS(NULL, 'data', 'fuse', 'labs');
+-----------------------------------------+
| CONCAT_WS(NULL, 'data', 'fuse', 'labs') |
+-----------------------------------------+
|                                    NULL |
+-----------------------------------------+

SELECT CONCAT_WS(',', NULL);
+----------------------+
| CONCAT_WS(',', NULL) |
+----------------------+
|                      |
+----------------------+
```