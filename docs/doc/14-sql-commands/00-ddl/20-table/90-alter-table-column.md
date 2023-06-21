---
title: ALTER TABLE
description:
  Adds or drops a column of a table.
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.39"/>

Modifies a table by adding, renaming, or removing a column.

## Syntax

```sql
-- Add a column
ALTER TABLE [IF EXISTS] [database.]<table_name> 
ADD COLUMN <column_name> <data_type> [NOT NULL | NULL] [DEFAULT <constant_value>];

-- Rename a column
ALTER TABLE [IF EXISTS] [database.]<table_name>
RENAME COLUMN <column_name> TO <new_column_name>;

-- Remove a column
ALTER TABLE [IF EXISTS] [database.]<table_name> 
DROP COLUMN <column_name>;
```

:::note
Only a constant value can be accepted as a default value when adding a new column. If a non-constant expression is used, an error will occur.
:::

## Examples

This example illustrates the creation of a table called "default.users" with columns for id, username, email, and age. It showcases the addition of columns for business_email, middle_name, and phone_number with various constraints. The example also demonstrates the renaming and subsequent removal of the "age" column.

```sql
-- Create a table
CREATE TABLE default.users (
  id INT,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(255),
  age INT
);

-- Add a column with a default value
ALTER TABLE default.users
ADD COLUMN business_email VARCHAR(255) NOT NULL DEFAULT 'example@example.com';

-- Add a column allowing NULL values
ALTER TABLE default.users
ADD COLUMN middle_name VARCHAR(50) NULL;

-- Add a column with NOT NULL constraint
ALTER TABLE default.users
ADD COLUMN phone_number VARCHAR(20) NOT NULL;

-- Rename a column
ALTER TABLE default.users
RENAME COLUMN age TO new_age;

-- Remove a column
ALTER TABLE default.users
DROP COLUMN new_age;

DESC default.users;

Field         |Type   |Null|Default              |Extra|
--------------+-------+----+---------------------+-----+
id            |INT    |NO  |0                    |     |
username      |VARCHAR|NO  |''                   |     |
email         |VARCHAR|NO  |''                   |     |
business_email|VARCHAR|NO  |'example@example.com'|     |
middle_name   |VARCHAR|YES |NULL                 |     |
phone_number  |VARCHAR|NO  |''                   |     |
```