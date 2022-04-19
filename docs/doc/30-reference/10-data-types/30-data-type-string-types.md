---
title: String
description: Basic String data type.
---

## String Data Types

In Databend, strings can be stored in the VARCHAR field, the storage size is variable.

| Name     | Storage Size
| -------- | ------------
| VARCHAR  | variable

## Functions

See [String Functions](/doc/reference/functions/string-functions).


## Example

```sql
CREATE TABLE string_table(text VARCHAR);

DESC string_table;
+-------+---------+------+---------+
| Field | Type    | Null | Default |
+-------+---------+------+---------+
| text  | VARCHAR | NO   |         |
+-------+---------+------+---------+

INSERT INTO string_table VALUES('databend');

SELECT * FROM string_table;
+----------+
| text     |
+----------+
| databend |
+----------+
```
