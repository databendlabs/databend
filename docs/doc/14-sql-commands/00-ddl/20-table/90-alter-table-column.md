---
title: ALTER TABLE COLUMN
description:
  Adds or drops a column of a table.
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.39"/>

Modifies a table by adding, converting, renaming, removing, or changing the type of a column.

## Syntax

```sql
-- Add a column
ALTER TABLE [IF EXISTS] [database.]<table_name> 
ADD COLUMN <column_name> <data_type> [NOT NULL | NULL] [DEFAULT <constant_value>];

-- Add a virtual computed column
ALTER TABLE [IF EXISTS] [database.]<table_name> 
ADD COLUMN <column_name> <data_type> AS (<expr>) VIRTUAL;

-- Convert a stored computed column to a regular column
ALTER TABLE [IF EXISTS] [database.]<table_name> 
MODIFY COLUMN <column_name> DROP STORED;

-- Rename a column
ALTER TABLE [IF EXISTS] [database.]<table_name>
RENAME COLUMN <column_name> TO <new_column_name>;

-- Change the data type of one or multiple columns
ALTER TABLE [IF EXISTS] [database.]<table_name> 
MODIFY COLUMN <column_name> <new_data_type>[, COLUMN <column_name> <new_data_type>, ...]

-- Remove a column
ALTER TABLE [IF EXISTS] [database.]<table_name> 
DROP COLUMN <column_name>;
```

:::note
- Only a constant value can be accepted as a default value when adding a new column. If a non-constant expression is used, an error will occur.
- Adding a stored computed column with ALTER TABLE is not supported yet.
- When you change the data type of a table's columns, there's a risk of conversion errors. For example, if you try to convert a column with text (String) to numbers (Float), it might cause problems.
:::

## Examples

### Example 1: Adding, Renaming, and Removing a Column

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

### Example 2: Adding a Computed Column

This example demonstrates creating a table for storing employee information, inserting data into the table, and adding a computed column to calculate the age of each employee based on their birth year.

```sql
-- Create a table
CREATE TABLE Employees (
  ID INT,
  Name VARCHAR(50),
  BirthYear INT
);

-- Insert data
INSERT INTO Employees (ID, Name, BirthYear)
VALUES
  (1, 'John Doe', 1990),
  (2, 'Jane Smith', 1985),
  (3, 'Robert Johnson', 1982);

-- Add a computed column named Age
ALTER TABLE Employees
ADD COLUMN Age INT64 AS (2023 - BirthYear) VIRTUAL;

SELECT * FROM Employees;

ID | Name          | BirthYear | Age
------------------------------------
1  | John Doe      | 1990      | 33
2  | Jane Smith    | 1985      | 38
3  | Robert Johnson| 1982      | 41
```

### Example 3: Converting a Computed Column

This example creates a table called "products" with columns for ID, price, quantity, and a computed column "total_price." The ALTER TABLE statement removes the computed functionality from the "total_price" column, converting it into a regular column.

```sql
CREATE TABLE IF NOT EXISTS products (
  id INT,
  price FLOAT64,
  quantity INT,
  total_price FLOAT64 AS (price * quantity) STORED
);

ALTER TABLE products
MODIFY COLUMN total_price DROP STORED;
```

### Example 4: Changing Data Type of a Column

This example creates a table named "students_info" with columns for "id," "name," and "age," inserts some sample data, and then modifies the data type of the "age" column from INT to VARCHAR(10).

```sql
CREATE TABLE students_info (
  id INT,
  name VARCHAR(50),
  age INT
);

INSERT INTO students_info VALUES
  (1, 'John Doe', 25),
  (2, 'Jane Smith', 28),
  (3, 'Michael Johnson', 22);

ALTER TABLE students_info MODIFY COLUMN age VARCHAR(10);

SELECT * FROM students_info;

id|name           |age|
--+---------------+---+
 1|John Doe       |25 |
 2|Jane Smith     |28 |
 3|Michael Johnson|22 |
```