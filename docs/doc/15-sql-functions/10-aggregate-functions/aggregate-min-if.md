---
title: MIN_IF
---


## MIN_IF 

The suffix `_IF` can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```
MIN_IF(<column>, <cond>)
```

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE project_budgets (
  id INT,
  project_id INT,
  department VARCHAR,
  budget FLOAT
);

INSERT INTO project_budgets (id, project_id, department, budget)
VALUES (1, 1, 'HR', 1000),
       (2, 1, 'IT', 2000),
       (3, 1, 'Marketing', 3000),
       (4, 2, 'HR', 1500),
       (5, 2, 'IT', 2500);
```

**Query Demo: Find Minimum Budget for IT Department**

```sql
SELECT MIN_IF(budget, department = 'IT') AS min_it_budget
FROM project_budgets;
```

**Result**
```sql
| min_it_budget |
|---------------|
|     2000      |
```
