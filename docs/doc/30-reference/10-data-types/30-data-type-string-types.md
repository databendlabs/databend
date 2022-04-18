---
title: String
description: Basic String data type.
---

## String Data Types

In Databend, strings can be stored in the VARCHAR field.

| Data Type        | Syntax   |
| -----------------| -------- |
| String           | VARCHAR

## Functions

See [String Functions](/doc/reference/functions/string-functions).


## Example

```sql
mysql> CREATE TABLE string_table(text VARCHAR);

mysql> DESC string_table;
+-------+--------+------+---------+
| Field | Type   | Null | Default |
+-------+--------+------+---------+
| text  | String | NO   |         |
+-------+--------+------+---------+

mysql> INSERT INTO string_table VALUES('databend');

mysql> SELECT * FROM string_table;
+----------+
| text     |
+----------+
| databend |
```
