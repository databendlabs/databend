---
title: GROUP BY
---

The GROUP BY clause in Databend SQL allows you to group rows sharing the same group-by-item expressions and apply aggregate functions to the resulting groups. A group-by-item expression can be a column name, a number referencing a position in the [SELECT](./01-query-select.md) list, or a general expression.

Extensions include [GROUP BY CUBE](./08-query-group-by-cube.md), [GROUP BY GROUPING SETS](./07-query-group-by-grouping-sets.md), and [GROUP BY ROLLUP](./09-query-group-by-rollup.md).

## Syntax

```sql
SELECT ...
    FROM ...
    [ ... ]
GROUP BY groupItem [ , groupItem [ , ... ] ]
    [ ... ]
```

Where:
```sql
groupItem ::= { <column_alias> | <position> | <expr> }
```

- `<column_alias>`: Column alias appearing in the query block’s SELECT list

- `<position>`: Position of an expression in the SELECT list

- `<expr>`: Any expression on tables in the current scope


## Examples

Sample Data Setup:
```sql
-- Create a sample employees table
CREATE TABLE employees (
    id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department_id INT,
    job_id INT,
    hire_date DATE
);

-- Insert sample data into the employees table
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

