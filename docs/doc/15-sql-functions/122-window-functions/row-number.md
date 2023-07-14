---
title: ROW_NUMBER
---


## ROW_NUMBER

Returns a unique row number for each row within a window partition.

The row number starts at 1 and continues up sequentially.

## Syntax

```sql
ROW_NUMBER() OVER (
  [ PARTITION BY <expr1> [, <expr2> ... ] ]
  ORDER BY <expr3> [ , <expr4> ... ] [ { ASC | DESC } ]
  )
```

## Examples


Suppose we have a table called employees with columns employee_id, first_name, last_name, department, and salary.

We want to number employees within each department based on their salaries in descending order.

**Create the table**

```sql
CREATE TABLE employees (
  employee_id INT,
  first_name VARCHAR,
  last_name VARCHAR,
  department VARCHAR,
  salary INT
);
```

**Insert data**

```sql
INSERT INTO employees (employee_id, first_name, last_name, department, salary) VALUES
  (1, 'John', 'Doe', 'IT', 90000),
  (2, 'Jane', 'Smith', 'HR', 85000),
  (3, 'Mike', 'Johnson', 'IT', 82000),
  (4, 'Sara', 'Williams', 'Sales', 77000),
  (5, 'Tom', 'Brown', 'HR', 75000);

```

**Numbering employees within departments**
```sql
SELECT
    employee_id,
    first_name,
    last_name,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
FROM
    employees;
```

Result:

| employee_id | first_name | last_name | department | salary | row_num |
|-------------|------------|-----------|------------|--------|---------|
| 1           | John       | Doe       | IT         | 90000  | 1       |
| 3           | Mike       | Johnson   | IT         | 82000  | 2       |
| 2           | Jane       | Smith     | HR         | 85000  | 1       |
| 5           | Tom        | Brown     | HR         | 75000  | 2       |
| 4           | Sara       | Williams  | Sales      | 77000  | 1       |
