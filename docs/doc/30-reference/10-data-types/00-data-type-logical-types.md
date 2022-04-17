---
title: Boolean
description: Basic logical data type.
---

The BOOLEAN type represents a statement of truth (“true” or “false”).

## Boolean

| Data Type        | Syntax   |
| -----------------| -------- |
| Boolean          | BOOLEAN

## Implicit Conversion

Boolean values can be implicitly converted from numeric values to boolean values.
* Zero (0) is converted to FALSE.
* Any non-zero value is converted to TRUE.

## Functions

See [Conditional Functions](/doc/reference/functions/conditional-functions).

## Example

```sql title='mysql>'
mysql> CREATE TABLE test_boolean(a BOOLEAN, s VARCHAR);

mysql> INSERT INTO test_boolean VALUES(true, 'true'),(false, 'false'), (0, 'false'),(10, 'true');

mysql> SELECT * FROM test_boolean;
+------+-------+
| a    | s     |
+------+-------+
|    1 | true  |
|    0 | false |
|    0 | false |
|    1 | true  |
+------+-------+
```
