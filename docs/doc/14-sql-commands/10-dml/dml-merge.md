---
title: MERGE INTO
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.2.89"/>

Performs INSERT, UPDATE, or DELETE operations on rows within a target table, all in accordance with conditions and matching criteria specified within the statement, using data from a specified source.

The data source, which can a subquery, is linked to the target data via a JOIN expression. This expression assesses whether rows from the source and target tables match and subsequently determines which operations (INSERT, UPDATE, or DELETE) should be applied to the rows of the target table.

A MERGE INTO statement can contain one or more MATCHED and NOT MATCHED clauses, instructing Databend on how to handle matched and unmatched scenarios. For a MATCHED clause, you have the option to choose between performing an UPDATE or DELETE operation. Conversely, in the case of a NOT MATCHED clause, the available choice is INSERT.

If a MERGE INTO statement includes multiple MATCHED clauses, a condition needs to be specified for each clause except the last one. These conditions determine the criteria under which the associated operations are executed. Databend evaluates the conditions in the specified order. Once a condition is met, it triggers the specified operation, and any remaining MATCHED clauses are skipped. If the MERGE INTO statement also includes multiple NOT MATCHED clauses, Databend handles them in the same way.

:::note
MERGE INTO is currently in an experimental state. Before using MERGE INTO, you need to run "SET enable_experimental_merge_into = 1;" to enable the feature.
:::

## Syntax

```sql
MERGE INTO <target_table> 
    USING <source> ON <join_expr> { matchedClause | notMatchedClause } [ ... ]

matchedClause ::=
  WHEN MATCHED [ AND <condition> ] THEN 
  { UPDATE SET <col_name> = <expr> [ , <col_name2> = <expr2> ... ] | DELETE } 

notMatchedClause ::=
   WHEN NOT MATCHED [ AND <condition> ] THEN INSERT 
   [ ( <col_name> [ , ... ] ) ] VALUES ( <expr> [ , ... ] )
```

## Examples

```sql
-- Create the 'employees' table
CREATE TABLE employees (
    employee_id INT,
    employee_name VARCHAR(255),
    department VARCHAR(255)
);

-- Create the 'salaries' table
CREATE TABLE salaries (
    employee_id INT,
    salary DECIMAL(10, 2)
);

-- Insert initial employee data
INSERT INTO employees VALUES
    (1, 'Alice', 'HR'),
    (2, 'Bob', 'IT'),
    (3, 'Charlie', 'Finance'),
    (4, 'David', 'HR');

-- Insert initial salary data
INSERT INTO salaries VALUES
    (1, 50000.00),
    (2, 60000.00);

-- Now, let's use Databend to update salaries for existing employees
-- based on their department and increment salaries for HR employees

SET enable_experimental_merge_into = 1;

MERGE INTO salaries
USING employees
ON salaries.employee_id = employees.employee_id
WHEN MATCHED AND employees.department = 'HR' THEN
    UPDATE SET
        salaries.salary = salaries.salary + 1000.00
WHEN MATCHED THEN
    UPDATE SET
        salaries.salary = salaries.salary + 500.00
WHEN NOT MATCHED THEN
    INSERT (employee_id, salary)
    VALUES (employees.employee_id, 55000.00);
```