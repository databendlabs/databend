---
title: SHOW COLUMNS
---

Shows information about the columns in a given table.

:::tip
[DESCRIBE TABLE](../00-ddl/20-table/50-describe-table.md) provides similar but less information about the columns of a table. 
:::

## Syntax

```sql
SHOW [FULL] COLUMNS FROM table_name [IN database_name];
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

SHOW COLUMNS FROM books;

Field   |Type     |Null|Default     |Extra|Key|
--------+---------+----+------------+-----+---+
author  |VARCHAR  |NO  |            |     |   |
price   |FLOAT    |NO  |0.00        |     |   |
pub_time|TIMESTAMP|NO  |'1900-01-01'|     |   |

SHOW FULL COLUMNS FROM books;

Field   |Type     |Null|Default     |Extra|Key|Collation|Privileges|comment|
--------+---------+----+------------+-----+---+---------+----------+-------+
author  |VARCHAR  |NO  |            |     |   |         |          |       |
price   |FLOAT    |NO  |0.00        |     |   |         |          |       |
pub_time|TIMESTAMP|NO  |'1900-01-01'|     |   |         |          |       | 
```