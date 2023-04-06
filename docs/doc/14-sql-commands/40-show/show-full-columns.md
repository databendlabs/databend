---
title: SHOW COLUMNS
---

Shows information about the columns in a given table.

:::tip
[DESCRIBE TABLE](../00-ddl/20-table/50-describe-table.md) provides similar but less information about the columns of a table. 
:::

## Syntax

```sql
SHOW  [FULL] COLUMNS
    {FROM | IN} tbl_name
    [{FROM | IN} db_name]
    [LIKE 'pattern' | WHERE expr]
```

When the optional keyword FULL is included, Databend will add the collation, privileges, and comment information for each column in the table to the result.

## Examples

```sql
CREATE TABLE books
  (
     price  FLOAT Default 0.00,
     pub_time DATETIME Default '1900-01-01',
     author VARCHAR
  ); 

SHOW COLUMNS FROM books FROM default;

Field   |Type     |Null|Default     |Extra|Key|
--------+---------+----+------------+-----+---+
author  |VARCHAR  |NO  |            |     |   |
price   |FLOAT    |NO  |0.00        |     |   |
pub_time|TIMESTAMP|NO  |'1900-01-01'|     |   |

SHOW FULL COLUMNS FROM books;

Field   |Type     |Null|Default     |Extra|Key|Collation|Privileges|Comment|
--------+---------+----+------------+-----+---+---------+----------+-------+
author  |VARCHAR  |NO  |            |     |   |         |          |       |
price   |FLOAT    |NO  |0.00        |     |   |         |          |       |
pub_time|TIMESTAMP|NO  |'1900-01-01'|     |   |         |          |       | 

SHOW FULL COLUMNS FROM books LIKE 'a%'

Field |Type   |Null|Default|Extra|Key|Collation|Privileges|Comment|
------+-------+----+-------+-----+---+---------+----------+-------+
author|VARCHAR|NO  |       |     |   |         |          |       |
```