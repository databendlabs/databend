---
title: AVG_IF
---


## AVG_IF 

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```sql
AVG_IF(<column>, <cond>)
```

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE employees (
  id INT,
  salary INT,
  department VARCHAR
);

INSERT INTO employees (id, salary, department)
VALUES (1, 50000, 'HR'),
       (2, 60000, 'IT'),
       (3, 55000, 'HR'),
       (4, 70000, 'IT'),
       (5, 65000, 'IT');
```

**Query Demo: Calculate Average Salary for IT Department**

```sql
SELECT AVG_IF(salary, department = 'IT') AS avg_salary_it
FROM employees;
```

**Result**
```sql
| avg_salary_it   |
|-----------------|
|     65000.0     |
```