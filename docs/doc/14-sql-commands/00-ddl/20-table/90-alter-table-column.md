---
title: ALTER TABLE
description:
  Adds or drops a column of a table.
---

Adds or drops a column of a table.

:::tip
ALTER TABLE can also handle table clustering. For more information, refer to the following pages:

- [ALTER CLUSTER KEY](../70-clusterkey/dml-alter-cluster-key.md)
- [RECLUSTER TABLE](../70-clusterkey/dml-recluster-table.md)
:::

## Syntax

```sql
ALTER TABLE [IF EXISTS] <name> ADD COLUMN <column_name> <data_type> [ NOT NULL | NULL] [ { DEFAULT <expr> }]
ALTER TABLE [IF EXISTS] <name> DROP COLUMN <column_name>
```

## Examples

```sql
DESC books;

Field   |Type     |Null|Default     |Extra|
--------+---------+----+------------+-----+
price   |FLOAT    |NO  |0.00        |     |
pub_time|TIMESTAMP|NO  |'1900-01-01'|     |
author  |VARCHAR  |NO  |""          |     |

ALTER TABLE books ADD COLUMN region varchar;
DESC books;

Field   |Type     |Null|Default     |Extra|
--------+---------+----+------------+-----+
price   |FLOAT    |NO  |0.00        |     |
pub_time|TIMESTAMP|NO  |'1900-01-01'|     |
author  |VARCHAR  |NO  |""          |     |
region  |VARCHAR  |NO  |""          |     |

ALTER TABLE books DROP COLUMN region;
DESC books;

Field   |Type     |Null|Default     |Extra|
--------+---------+----+------------+-----+
price   |FLOAT    |NO  |0.00        |     |
pub_time|TIMESTAMP|NO  |'1900-01-01'|     |
author  |VARCHAR  |NO  |""          |     |
```