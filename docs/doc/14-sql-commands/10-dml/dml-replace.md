---
title: REPLACE
---


The `REPLACE INTO` statement in Databend is used to either insert a new row into a table or update an existing row if the row already exists. 

If a row with the specified conflict key(*not primary key*) already exists in the table, then the `REPLACE INTO` statement will replace the existing row with the new data. If the row doesn't exist, a new row will be inserted with the specified data.

## Syntax

```sql
REPLACE INTO <target_table> [ ( <col_name> [ , ... ] ) ]
    ON [CONFLICT] <VALUES | QUERY | STAGE>
```
* `<target_table>`: the name of the table to insert or update data.
* `<col_name>`: the column names in the table where the data will be inserted or updated.
* `<VALUES | QUERY | STAGE>`: specifies how to provide the data to be inserted or updated in the columns specified.

## Conflict Key in REPLACE Statement

The conflict key is a column or combination of columns in a table that uniquely identifies a row and is used to determine whether to insert a new row or update an existing row in the table using the `REPLACE INTO` statement. It can be any column or combination of columns with a unique constraint, not just the primary key(Databend doesn't have primary key).

For example, in a table called `employees` with a unique constraint on the `employee_email` column, you can use the `employee_email` column as the conflict key in the `REPLACE INTO` statement like this:

```sql
REPLACE INTO employees (employee_id, employee_name, employee_salary)  ON (employee_email)
VALUES (123, 'John Doe', 50000);
```

## Examples

Here are some examples that show how to use the `REPLACE INTO` statement in Databend:

```sql
CREATE TABLE employees(id INT, name VARCHAR, salary INT);
```

### Using VALUES

```sql
REPLACE INTO employees (id, name, salary) ON (id)
VALUES (1, 'John Doe', 50000);
```

```sql
SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
|    1 | John Doe |  50000 |
+------+----------+--------+
```

### Using QUERY

In this case, the data to be inserted or updated comes from a temporary table called `temp_employees`:

```sql
-- Create a temp stage table
CREATE TABLE temp_employees(id INT, name VARCHAR, salary INT);
INSERT INTO temp_employees (id, name, salary) VALUES (1, 'John Doe', 60000);

REPLACE INTO employees (id, name, salary) ON (id)
SELECT id, name, salary FROM temp_employees WHERE id = 1;
```

```sql
SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
|    1 | John Doe |  60000 |
+------+----------+--------+
```

### Using STAGE

In this case, the data to be inserted or updated comes from a set of parquet files staged in an internal stage called `employees_stage`:
```sql
-- Create a internal stage
CREATE STAGE employees_stage;
-- Stage parquet files to stage
COPY INTO @employees_stage FROM 
(SELECT number, CONCAT('name-', TO_STRING(number)), number*1000 FROM numbers(10))
FILE_FORMAT = (TYPE = PARQUET);

-- Replace into with stage files
REPLACE INTO employees (id, name, salary) ON (id) 
SELECT * FROM @employees_stage (PATTERN => '.*parquet');
```

```sql
SELECT  * FROM Employees;
+------+--------+--------+
| id   | name   | salary |
+------+--------+--------+
|    0 | name-0 |      0 |
|    1 | name-1 |   1000 |
|    2 | name-2 |   2000 |
|    3 | name-3 |   3000 |
|    4 | name-4 |   4000 |
|    5 | name-5 |   5000 |
|    6 | name-6 |   6000 |
|    7 | name-7 |   7000 |
|    8 | name-8 |   8000 |
|    9 | name-9 |   9000 |
+------+--------+--------+
```