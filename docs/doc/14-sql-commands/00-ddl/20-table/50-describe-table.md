---
title: DESCRIBE TABLE
---

Shows information about the columns in a given table.

:::tip
[SHOW COLUMNS](../../40-show/show-full-columns.md) provides similar but more information about the columns of a table. 
:::

## Syntax

```sql
DESC|DESCRIBE [database.]table_name
```

## Examples

```sql
CREATE TABLE books
  (
     price  FLOAT Default 0.00,
     pub_time DATETIME Default '1900-01-01',
     author VARCHAR
  );

DESC books; 

Field   |Type     |Null|Default                     |Extra|
--------+---------+----+----------------------------+-----+
price   |FLOAT    |YES |0                           |     |
pub_time|TIMESTAMP|YES |'1900-01-01 00:00:00.000000'|     |
author  |VARCHAR  |YES |NULL                        |     |
```
