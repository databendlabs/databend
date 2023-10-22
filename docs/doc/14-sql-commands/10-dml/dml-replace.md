---
title: REPLACE
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.55"/>

REPLACE INTO can either insert multiple new rows into a table or update existing rows if those rows already exist, using the following sources of data:

- Direct values

- Query results

- Staged files: Databend enables you to replace data into a table from staged files with the REPLACE INTO statement. This is achieved through Databend's capacity to [Query Staged Files](../../12-load-data/00-transform/05-querying-stage.md) and subsequently incorporate the query result into the table.

:::tip atomic operations
Databend ensures data integrity with atomic operations. Inserts, updates, replaces, and deletes either succeed completely or fail entirely.
:::

## Syntax

```sql
REPLACE INTO <table_name> [ ( <col_name> [ , ... ] ) ]
    ON (<CONFLICT KEY>) ...
```

REPLACE INTO updates existing rows when the specified conflict key is found in the table and inserts new rows if the conflict key is not present. The conflict key is a column or combination of columns in a table that uniquely identifies a row and is used to determine whether to insert a new row or update an existing row in the table using the REPLACE INTO statement. See an example below:

```sql
CREATE TABLE employees (
    employee_id INT,
    employee_name VARCHAR(100),
    employee_salary DECIMAL(10, 2),
    employee_email VARCHAR(255)
);

-- This REPLACE INTO inserts a new row
REPLACE INTO employees (employee_id, employee_name, employee_salary, employee_email) 
ON (employee_email)
VALUES (123, 'John Doe', 50000, 'john.doe@example.com');

-- This REPLACE INTO updates the inserted row
REPLACE INTO employees (employee_id, employee_name, employee_salary, employee_email) 
ON (employee_email)
VALUES (123, 'John Doe', 60000, 'john.doe@example.com');
```

## Distributed REPLACE INTO

`REPLACE INTO` support distributed execution in cluster environments. You can enable distributed REPLACE INTO by setting ENABLE_DISTRIBUTED_REPLACE_INTO to 1. This helps enhance data loading performance and scalability in cluster environments.

```sql
SET enable_distributed_replace_into = 1;
```

## Examples

### Example 1: Replace with Direct Values

This example replaces data with direct values:

```sql
CREATE TABLE employees(id INT, name VARCHAR, salary INT);

REPLACE INTO employees (id, name, salary) ON (id)
VALUES (1, 'John Doe', 50000);

SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
| 1    | John Doe |  50000 |
+------+----------+--------+
```

### Example 2: Replace with Query Results

This example replaces data with a query result:

```sql
CREATE TABLE employees(id INT, name VARCHAR, salary INT);

CREATE TABLE temp_employees(id INT, name VARCHAR, salary INT);

INSERT INTO temp_employees (id, name, salary) VALUES (1, 'John Doe', 60000);

REPLACE INTO employees (id, name, salary) ON (id)
SELECT id, name, salary FROM temp_employees WHERE id = 1;

SELECT  * FROM Employees;
+------+----------+--------+
| id   | name     | salary |
+------+----------+--------+
|    1 | John Doe |  60000 |
+------+----------+--------+
```

### Example 3: Replace with Staged Files

This example demonstrates how to replace existing data in a table with data from a staged file.

1. Create a table called `sample`

```sql
CREATE TABLE sample
(
    id      INT,
    city    VARCHAR,
    score   INT,
    country VARCHAR DEFAULT 'China'
);

INSERT INTO sample
    (id, city, score)
VALUES
    (1, 'Chengdu', 66);
```

2. Set up an internal stage with sample data

Firstly, we create a stage named `mystage`. Then, we load sample data into this stage.
```sql
CREATE STAGE mystage;
       
COPY INTO @mystage
FROM 
(
    SELECT * 
    FROM 
    (
        VALUES 
        (1, 'Chengdu', 80),
        (3, 'Chongqing', 90),
        (6, 'Hangzhou', 92),
        (9, 'Hong Kong', 88)
    )
)
FILE_FORMAT = (TYPE = PARQUET);
```

3. Replace existing data using the staged Parquet file with `REPLACE INTO`

:::tip
You can specify the file format and various copy-related settings with the FILE_FORMAT and COPY_OPTIONS available in the [COPY INTO](dml-copy-into-table.md) command. When `purge` is set to `true`, the original file will only be deleted if the data update is successful. 
:::

```sql
REPLACE INTO sample 
    (id, city, score) 
ON
    (Id)
SELECT
    $1, $2, $3
FROM
    @mystage
    (FILE_FORMAT => 'parquet');
```

4. Verify the data replacement

Now, we can query the sample table to see the changes:
```sql
SELECT * FROM sample;
```

The results should be:
```sql
┌─────────────────────────────────────────────────────────────────────────┐
│        id       │       city       │      score      │      country     │
│ Nullable(Int32) │ Nullable(String) │ Nullable(Int32) │ Nullable(String) │
├─────────────────┼──────────────────┼─────────────────┼──────────────────┤
│               1 │ Chengdu          │              80 │ China            │
│               3 │ Chongqing        │              90 │ China            │
│               6 │ Hangzhou         │              92 │ China            │
│               9 │ Hong Kong        │              88 │ China            │
└─────────────────────────────────────────────────────────────────────────┘
```