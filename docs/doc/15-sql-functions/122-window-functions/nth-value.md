---
title: NTH_VALUE
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.50"/>

Returns the Nth value from an ordered group of values.

See also:

- [FIRST_VALUE](first-value.md)
- [LAST_VALUE](last-value.md)

## Syntax

```sql
NTH_VALUE(expression, n) OVER ([PARTITION BY partition_expression] ORDER BY order_expression [window_frame])
```

For the syntax of window frame, see [Window Frame Syntax](index.md#window-frame-syntax).

## Examples

```sql
CREATE TABLE employees (
  employee_id INT,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  salary DECIMAL(10,2)
);

INSERT INTO employees (employee_id, first_name, last_name, salary)
VALUES
  (1, 'John', 'Doe', 5000.00),
  (2, 'Jane', 'Smith', 6000.00),
  (3, 'David', 'Johnson', 5500.00),
  (4, 'Mary', 'Williams', 7000.00),
  (5, 'Michael', 'Brown', 4500.00);

-- Use NTH_VALUE to retrieve the first name of the employee with the second highest salary
SELECT employee_id, first_name, last_name, salary,
       NTH_VALUE(first_name, 2) OVER (ORDER BY salary DESC) AS second_highest_salary_first_name
FROM employees;

employee_id | first_name | last_name | salary  | second_highest_salary_first_name
------------+------------+-----------+---------+----------------------------------
4           | Mary       | Williams  | 7000.00 | Jane
2           | Jane       | Smith     | 6000.00 | Jane
3           | David      | Johnson   | 5500.00 | Jane
1           | John       | Doe       | 5000.00 | Jane
5           | Michael    | Brown     | 4500.00 | Jane
```