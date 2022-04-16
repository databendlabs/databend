---
title: Logical Data Types
description: Basic logical data type.
---

The logical data types supported in Databend.

## Boolean

| Data Type        | Syntax   |
| -----------------| -------- |
| Boolean          | BOOLEAN

## Implicit Conversion

Boolean values can be implicitly converted from numeric values to boolean values.
* Zero (0) is converted to FALSE.
* Any non-zero value is converted to TRUE.

## Example

```sql title='mysql>'
mysql> create table test_boolean(a boolean, s varchar);

mysql> insert into test_boolean values(true, 'true'),(false, 'false'), (0, 'false'),(10, 'true');

mysql> select * from test_boolean;
+------+-------+
| a    | s     |
+------+-------+
|    1 | true  |
|    0 | false |
|    0 | false |
|    1 | true  |
+------+-------+
```
