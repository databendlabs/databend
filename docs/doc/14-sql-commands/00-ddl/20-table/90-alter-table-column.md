---
title: ALTER TABLE COLUMN
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.71"/>

import EEFeature from '@site/src/components/EEFeature';

<EEFeature featureName='MASKING POLICY'/>

Modifies a table by adding, converting, renaming, changing, or removing a column.

## Syntax

```sql
-- Add a column to the end of the table
ALTER TABLE [IF EXISTS] [database.]<table_name> 
ADD COLUMN <column_name> <data_type> [NOT NULL | NULL] [DEFAULT <constant_value>];

-- Add a column to a specified position
ALTER TABLE [IF EXISTS] [database.]<table_name> 
ADD COLUMN <column_name> <data_type> [NOT NULL | NULL] [DEFAULT <constant_value>] [FIRST | AFTER <column_name>]

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
MODIFY COLUMN <column_name> <new_data_type> [DEFAULT <constant_value>][, COLUMN <column_name> <new_data_type> [DEFAULT <constant_value>], ...]

-- Set / Unset masking policy for a column
ALTER TABLE [IF EXISTS] [database.]<table_name>
MODIFY COLUMN <column_name> SET MASKING POLICY <policy_name>

ALTER TABLE [IF EXISTS] [database.]<table_name>
MODIFY COLUMN <column_name> UNSET MASKING POLICY

-- Remove a column
ALTER TABLE [IF EXISTS] [database.]<table_name> 
DROP COLUMN <column_name>;
```

:::note
- Only a constant value can be accepted as a default value when adding or modifying a column. If a non-constant expression is used, an error will occur.
- Adding a stored computed column with ALTER TABLE is not supported yet.
- When you change the data type of a table's columns, there's a risk of conversion errors. For example, if you try to convert a column with text (String) to numbers (Float), it might cause problems.
- When you set a masking policy for a column, make sure that the data type (refer to the parameter *arg_type_to_mask* in the syntax of [CREATE MASKING POLICY](../102-mask-policy/create-mask-policy.md)) defined in the policy matches the column.
:::

## Examples

### Example 1: Adding, Renaming, and Removing a Column

This example illustrates the creation of a table called "default.users" with columns 'username', 'email', and 'age'. It showcases the addition of columns 'id' and 'middle_name' with various constraints. The example also demonstrates the renaming and subsequent removal of the "age" column.

```sql
-- Create a table
CREATE TABLE default.users (
  username VARCHAR(50) NOT NULL,
  email VARCHAR(255),
  age INT
);

-- Add a column to the end of the table
ALTER TABLE default.users
ADD COLUMN business_email VARCHAR(255) NOT NULL DEFAULT 'example@example.com';

DESC default.users;

Field         |Type   |Null|Default              |Extra|
--------------+-------+----+---------------------+-----+
username      |VARCHAR|NO  |''                   |     |
email         |VARCHAR|YES |NULL                 |     |
age           |INT    |YES |NULL                 |     |
business_email|VARCHAR|NO  |'example@example.com'|     |

-- Add a column to the beginning of the table
ALTER TABLE default.users
ADD COLUMN id int NOT NULL FIRST;

DESC default.users;

Field         |Type   |Null|Default              |Extra|
--------------+-------+----+---------------------+-----+
id            |INT    |NO  |0                    |     |
username      |VARCHAR|NO  |''                   |     |
email         |VARCHAR|YES |NULL                 |     |
age           |INT    |YES |NULL                 |     |
business_email|VARCHAR|NO  |'example@example.com'|     |

-- Add a column after the column 'username'
ALTER TABLE default.users
ADD COLUMN middle_name VARCHAR(50) NULL AFTER username;

DESC default.users;

Field         |Type   |Null|Default              |Extra|
--------------+-------+----+---------------------+-----+
id            |INT    |NO  |0                    |     |
username      |VARCHAR|NO  |''                   |     |
middle_name   |VARCHAR|YES |NULL                 |     |
email         |VARCHAR|YES |NULL                 |     |
age           |INT    |YES |NULL                 |     |
business_email|VARCHAR|NO  |'example@example.com'|     |

-- Rename a column
ALTER TABLE default.users
RENAME COLUMN age TO new_age;

DESC default.users;

Field         |Type   |Null|Default              |Extra|
--------------+-------+----+---------------------+-----+
id            |INT    |NO  |0                    |     |
username      |VARCHAR|NO  |''                   |     |
middle_name   |VARCHAR|YES |NULL                 |     |
email         |VARCHAR|YES |NULL                 |     |
new_age       |INT    |YES |NULL                 |     |
business_email|VARCHAR|NO  |'example@example.com'|     |

-- Remove a column
ALTER TABLE default.users
DROP COLUMN new_age;

DESC default.users;

Field         |Type   |Null|Default              |Extra|
--------------+-------+----+---------------------+-----+
id            |INT    |NO  |0                    |     |
username      |VARCHAR|NO  |''                   |     |
middle_name   |VARCHAR|YES |NULL                 |     |
email         |VARCHAR|YES |NULL                 |     |
business_email|VARCHAR|NO  |'example@example.com'|     |
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

ALTER TABLE students_info MODIFY COLUMN age VARCHAR(10) DEFAULT '0';
INSERT INTO students_info (id, name) VALUES  (4, 'Eric McMond');

SELECT * FROM students_info;

id|name           |age|
--+---------------+---+
 4|Eric McMond    |0  |
 1|John Doe       |25 |
 2|Jane Smith     |28 |
 3|Michael Johnson|22 |
```

### Example 5: Setting Masking Policy a Column

This example illustrates the process of setting up a masking policy to selectively reveal or mask sensitive data based on user roles.

```sql
-- Create a table and insert sample data
CREATE TABLE user_info (
    id INT,
    email STRING
);

INSERT INTO user_info (id, email) VALUES (1, 'sue@example.com');
INSERT INTO user_info (id, email) VALUES (2, 'eric@example.com');

-- Create a role
CREATE ROLE 'MANAGERS';
GRANT ALL ON *.* TO ROLE 'MANAGERS';

-- Create a user and grant the role to the user
CREATE USER manager_user IDENTIFIED BY 'databend';
GRANT ROLE 'MANAGERS' TO 'manager_user';

-- Create a masking policy
CREATE MASKING POLICY email_mask
AS
  (val string)
  RETURNS string ->
  CASE
  WHEN current_role() IN ('MANAGERS') THEN
    val
  ELSE
    '*********'
  END
  COMMENT = 'hide_email';

-- Associate the masking policy with the 'email' column
ALTER TABLE user_info MODIFY COLUMN email SET MASKING POLICY email_mask;

-- Query with the Root user
SELECT * FROM user_info;

id|email    |
--+---------+
 2|*********|
 1|*********|
```