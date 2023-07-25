---
title: GROUP BY
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.32"/>

The GROUP BY clause enables you to group rows based on the same group-by-item expressions and then apply aggregate functions to each resulting group. The group-by-item expressions can include column names or aliases, numerical references to positions in the SELECT list, general expressions, or all non-aggregate items in the SELECT list.

The GROUP BY clause in Databend comes with the following extensions for more comprehensive data grouping and versatile data analysis:

- [GROUP BY CUBE](./08-query-group-by-cube.md)
- [GROUP BY GROUPING SETS](./07-query-group-by-grouping-sets.md)
- [GROUP BY ROLLUP](./09-query-group-by-rollup.md)

## Syntax

```sql
SELECT ...
    FROM ...
    [ ... ]
GROUP BY [ ALL | groupItem [ , groupItem [ , ... ] ] ]
    [ ... ]
```

Where:

- **ALL**: When the keyword "ALL" is used, Databend groups the data based on all non-aggregate items in the SELECT list.
- **groupItem**: A group item can be one of the following:
    - A column name or alias defined in the SELECT list.
    - A numerical reference to the position of a column in the SELECT list.
    - Any expression that involves columns from the tables used in the current query context.

## Examples

The GROUP BY examples in this section are built upon the following data setup:

```sql
-- Create a sample table named "employees"
CREATE TABLE employees (
    id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT,
    job_id INT,
    hire_date DATE
);

-- Insert sample data into the "employees" table
INSERT INTO employees (id, first_name, last_name, department_id, job_id, hire_date)
VALUES (1, 'John', 'Doe', 1, 101, '2021-01-15'),
       (2, 'Jane', 'Smith', 1, 101, '2021-02-20'),
       (3, 'Alice', 'Johnson', 1, 102, '2021-03-10'),
       (4, 'Bob', 'Brown', 2, 201, '2021-03-15'),
       (5, 'Charlie', 'Miller', 2, 202, '2021-04-10'),
       (6, 'Eve', 'Davis', 2, 202, '2021-04-15');
```

### Group By One Column

This query groups employees by their `department_id` and counts the number of employees in each department:
```sql
SELECT department_id, COUNT(*) AS num_employees
FROM employees
GROUP BY department_id;
```

Output:
```sql
+---------------+---------------+
| department_id | num_employees |
+---------------+---------------+
|             1 |             3 |
|             2 |             3 |
+---------------+---------------+
```

### Group By Multiple Columns

This query groups employees by `department_id` and `job_id`, then counts the number of employees in each group:
```sql
SELECT department_id, job_id, COUNT(*) AS num_employees
FROM employees
GROUP BY department_id, job_id;
```

Output:
```sql
+---------------+--------+---------------+
| department_id | job_id | num_employees |
+---------------+--------+---------------+
|             1 |    101 |             2 |
|             1 |    102 |             1 |
|             2 |    201 |             1 |
|             2 |    202 |             2 |
+---------------+--------+---------------+
```

### Group By ALL

This query groups employees by using the GROUP BY ALL clause, which groups all non-aggregate columns in the SELECT list. Please note that, in this case, the result will be identical to grouping by `department_id` and `job_id` since these are the only non-aggregate items present in the SELECT list.

```sql
SELECT department_id, job_id, COUNT(*) AS num_employees
FROM employees
GROUP BY ALL;
```

Output:
```sql
+---------------+--------+---------------+
| department_id | job_id | num_employees |
+---------------+--------+---------------+
|             1 |    101 |             2 |
|             1 |    102 |             1 |
|             2 |    201 |             1 |
|             2 |    202 |             2 |
+---------------+--------+---------------+
```


### Group By Position

This query is equivalent to the "Group By One Column" example above. The position 1 refers to the first item in the SELECT list, which is `department_id`:
```sql
SELECT department_id, COUNT(*) AS num_employees
FROM employees
GROUP BY 1;
```

Output:
```sql
+---------------+---------------+
| department_id | num_employees |
+---------------+---------------+
|             1 |             3 |
|             2 |             3 |
+---------------+---------------+
```

### Group By Expression

This query groups employees by the year they were hired and counts the number of employees hired in each year:
```sql
SELECT EXTRACT(YEAR FROM hire_date) AS hire_year, COUNT(*) AS num_hires
FROM employees
GROUP BY EXTRACT(YEAR FROM hire_date);
```

Output:
```sql
+-----------+-----------+
| hire_year | num_hires |
+-----------+-----------+
|      2021 |         6 |
+-----------+-----------+
```