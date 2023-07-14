---
title: DELETE FROM
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.60"/>

Removes one or more rows from a table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Syntax

```sql
DELETE FROM <table_name>
[WHERE <condition>]
```

DELETE FROM does not support the USING clause yet. If you need to use a subquery to identify the rows to be removed, include it within the WHERE clause directly. See examples in [Subquery-Based Deletions](#subquery-based-deletions).

## Examples

### Direct Row Deletion

```sql
-- create a table
CREATE TABLE bookstore (
  book_id INT,
  book_name VARCHAR
);

-- insert values
INSERT INTO bookstore VALUES (101, 'After the death of Don Juan');
INSERT INTO bookstore VALUES (102, 'Grown ups');
INSERT INTO bookstore VALUES (103, 'The long answer');
INSERT INTO bookstore VALUES (104, 'Wartime friends');
INSERT INTO bookstore VALUES (105, 'Deconstructed');

-- show the table before deletion
SELECT * FROM bookstore;

101|After the death of Don Juan
102|Grown ups
103|The long answer
104|Wartime friends
105|Deconstructed

-- delete a book (Id: 103)
DELETE FROM bookstore WHERE book_id = 103;

-- show the table again after deletion
SELECT * FROM bookstore;

101|After the death of Don Juan
102|Grown ups
104|Wartime friends
105|Deconstructed
```

### Subquery-Based Deletions

When using a subquery to identify the rows to be deleted, [Subquery Operators](../30-query-operators/operators-subquery.md) and [Comparison Operators](../../15-sql-functions/02-comparisons-operators/index.md) can be utilized to achieve the desired deletion.

The examples in this section are based on the following two tables:

```sql
-- Table: employees
+----+----------+------------+
| id | name     | department |
+----+----------+------------+
| 1  | John     | HR         |
| 2  | Mary     | Sales      |
| 3  | David    | IT         |
| 4  | Jessica  | Finance    |
+----+----------+------------+

-- Table: departments
+----+------------+
| id | department |
+----+------------+
| 1  | Sales      |
| 2  | IT         |
+----+------------+
```

#### Deleting with subquery using IN / NOT IN clause

```sql
DELETE FROM employees WHERE department IN (SELECT department FROM departments);
```
This deletes employees whose department matches any department in the departments table. It would delete employees with IDs 2 and 3.

#### Deleting with subquery using EXISTS / NOT EXISTS clause

```sql
DELETE FROM employees WHERE EXISTS (SELECT * FROM departments WHERE employees.department = departments.department);
```
This deletes employees who belong to a department that exists in the departments table. In this case, it would delete employees with IDs 2 and 3.

#### Deleting with subquery using ALL clause

```sql
DELETE FROM employees WHERE department = ALL (SELECT department FROM departments);
```
This deletes employees whose department matches all departments in the departments table. In this case, no employees would be deleted.

#### Deleting with subquery using ANY clause

```sql
DELETE FROM employees WHERE department = ANY (SELECT department FROM departments);
```
This deletes employees whose department matches any department in the departments table. In this case, it would delete employees with IDs 2 and 3.

#### Deleting with subquery combining multiple conditions

```sql
DELETE FROM employees WHERE department = ANY (SELECT department FROM departments WHERE employees.department = departments.department) OR id > 2;
```

This deletes employees from the employees table if the value of the department column matches any value in the department column of the departments table or if the value of the id column is greater than 2. In this case, it would delete the rows with id 2, 3, and 4 since Mary's department is "Sales," which exists in the departments table, and the IDs 3 and 4 are greater than 2.