---
title: ALTER TABLE
description:
  Adds or drops a column of a table.
---

Adds or drops a column of a table.

## Syntax

```sql
ALTER TABLE [IF EXISTS] [database.]<table_name> 
ADD COLUMN <column_name> <data_type> [NOT NULL | NULL] [DEFAULT <constant_expr>];

ALTER TABLE [IF EXISTS] [database.]<table_name> 
DROP COLUMN <column_name>;
```

:::caution
In `ALTER TABLE ADD COLUMN`, the default value for a column must be a constant value.

This is different from [CREATE TABLE](10-ddl-create-table.md), where the default value can be any expression.

If a non-constant expression is used, an error will occur.
:::

## Examples

### Add Column

Add a new column to an existing table:

```sql
-- Create a table
CREATE TABLE students (
  id BIGINT,
  name VARCHAR
);

-- Add a new column 'age' to the 'students' table
ALTER TABLE students ADD COLUMN age INT;
```

### Drop Column

Remove an existing column from a table:

```sql
-- Create a table with three columns
CREATE TABLE employees (
  id BIGINT,
  name VARCHAR,
  department VARCHAR
);

-- Remove the 'department' column from the 'employees' table
ALTER TABLE employees DROP COLUMN department;
```

### Add Column with Default Value

Add a new column to an existing table with a default value:

```sql
-- Create a table
CREATE TABLE orders (
  id BIGINT,
  item VARCHAR
);

-- Add a new column 'status' with a default value 'Pending' to the 'orders' table
ALTER TABLE orders ADD COLUMN status VARCHAR DEFAULT 'Pending';
```

### Add Column with NOT NULL Constraint

Add a new column to an existing table with a NOT NULL constraint, which ensures that a value must be assigned to the column:

```sql
-- Create a table
CREATE TABLE products (
  id BIGINT,
  name VARCHAR
);

-- Add a new column 'price' with a NOT NULL constraint to the 'products' table
ALTER TABLE products ADD COLUMN price INT NOT NULL;
```