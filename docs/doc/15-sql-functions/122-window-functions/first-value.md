---
title: FIRST_VALUE
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.50"/>

Returns the first value from an ordered group of values.

See also:

- [LAST_VALUE](last-value.md)
- [NTH_VALUE](nth-value.md)

## Syntax

```sql
FIRST_VALUE(expression) OVER ([PARTITION BY partition_expression] ORDER BY order_expression [window_frame])
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

-- Use FIRST_VALUE to retrieve the first name of the employee with the highest salary
SELECT employee_id, first_name, last_name, salary,
       FIRST_VALUE(first_name) OVER (ORDER BY salary DESC) AS highest_salary_first_name
FROM employees;


employee_id | first_name | last_name | salary  | highest_salary_first_name
------------+------------+-----------+---------+--------------------------
4           | Mary       | Williams  | 7000.00 | Mary
2           | Jane       | Smith     | 6000.00 | Mary
3           | David      | Johnson   | 5500.00 | Mary
1           | John       | Doe       | 5000.00 | Mary
5           | Michael    | Brown     | 4500.00 | Mary
```