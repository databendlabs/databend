---
title: DELETE
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.174"/>

Removes one or more rows from a table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Syntax

```sql
DELETE FROM <table_name> [AS <table_alias>] 
[WHERE <condition>]
```
- `AS <table_alias>`: Allows you to set an alias for a table, making it easier to reference the table within a query. This helps simplify and shorten the SQL code, especially when dealing with complex queries involving multiple tables. See an example in [Deleting with subquery using EXISTS / NOT EXISTS clause](#deleting-with-subquery-using-exists--not-exists-clause).

- DELETE does not support the USING clause yet. If you need to use a subquery to identify the rows to be removed, include it within the WHERE clause directly. See examples in [Subquery-Based Deletions](#subquery-based-deletions).

## Examples

### Example 1: Direct Row Deletion

This example illustrates the use of the DELETE command to directly remove a book record with an ID of 103 from a "bookstore" table.

```sql
-- Create a table and insert 5 book records
CREATE TABLE bookstore (
  book_id INT,
  book_name VARCHAR
);

INSERT INTO bookstore VALUES (101, 'After the death of Don Juan');
INSERT INTO bookstore VALUES (102, 'Grown ups');
INSERT INTO bookstore VALUES (103, 'The long answer');
INSERT INTO bookstore VALUES (104, 'Wartime friends');
INSERT INTO bookstore VALUES (105, 'Deconstructed');

-- Delete a book (Id: 103)
DELETE FROM bookstore WHERE book_id = 103;

-- Show all records after deletion
SELECT * FROM bookstore;

101|After the death of Don Juan
102|Grown ups
104|Wartime friends
105|Deconstructed
```

### Example 2: Subquery-Based Deletions

When using a subquery to identify the rows to be deleted, [Subquery Operators](../30-query-operators/subquery.md) and [Comparison Operators](../30-query-operators/comparisons/index.md) can be utilized to achieve the desired deletion.

The examples in this section are based on the following two tables:

```sql
-- Create the 'employees' table
CREATE TABLE employees (
  id INT,
  name VARCHAR,
  department VARCHAR
);

-- Insert values into the 'employees' table
INSERT INTO employees VALUES (1, 'John', 'HR');
INSERT INTO employees VALUES (2, 'Mary', 'Sales');
INSERT INTO employees VALUES (3, 'David', 'IT');
INSERT INTO employees VALUES (4, 'Jessica', 'Finance');

-- Create the 'departments' table
CREATE TABLE departments (
  id INT,
  department VARCHAR
);

-- Insert values into the 'departments' table
INSERT INTO departments VALUES (1, 'Sales');
INSERT INTO departments VALUES (2, 'IT');
```

#### Deleting with subquery using IN / NOT IN clause

```sql
DELETE FROM EMPLOYEES
WHERE DEPARTMENT IN (
    SELECT DEPARTMENT
    FROM DEPARTMENTS
);
```
This deletes employees whose department matches any department in the departments table. It would delete employees with IDs 2 and 3.

#### Deleting with subquery using EXISTS / NOT EXISTS clause

```sql
DELETE FROM EMPLOYEES
WHERE EXISTS (
    SELECT *
    FROM DEPARTMENTS
    WHERE EMPLOYEES.DEPARTMENT = DEPARTMENTS.DEPARTMENT
);

-- Alternatively, you can delete employees using the alias 'e' for the 'EMPLOYEES' table and 'd' for the 'DEPARTMENTS' table when their department matches.
DELETE FROM EMPLOYEES AS e
WHERE EXISTS (
    SELECT *
    FROM DEPARTMENTS AS d
    WHERE e.DEPARTMENT = d.DEPARTMENT
);
```
This deletes employees who belong to a department that exists in the departments table. In this case, it would delete employees with IDs 2 and 3.

#### Deleting with subquery using ALL clause

```sql
DELETE FROM EMPLOYEES
WHERE DEPARTMENT = ALL (
    SELECT DEPARTMENT
    FROM DEPARTMENTS
);
```
This deletes employees whose department matches all departments in the department table. In this case, no employees would be deleted.

#### Deleting with subquery using ANY clause

```sql
DELETE FROM EMPLOYEES
WHERE DEPARTMENT = ANY (
    SELECT DEPARTMENT
    FROM DEPARTMENTS
);
```
This deletes employees whose department matches any department in the departments table. In this case, it would delete employees with IDs 2 and 3.

#### Deleting with subquery combining multiple conditions

```sql
DELETE FROM EMPLOYEES
WHERE DEPARTMENT = ANY (
    SELECT DEPARTMENT
    FROM DEPARTMENTS
    WHERE EMPLOYEES.DEPARTMENT = DEPARTMENTS.DEPARTMENT
)
   OR ID > 2;
```

This deletes employees from the employees table if the value of the department column matches any value in the department column of the departments table or if the value of the id column is greater than 2. In this case, it would delete the rows with id 2, 3, and 4 since Mary's department is "Sales," which exists in the departments table, and the IDs 3 and 4 are greater than 2.