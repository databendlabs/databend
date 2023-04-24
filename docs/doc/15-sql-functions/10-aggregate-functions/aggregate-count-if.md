---
title: COUNT_IF
---


## COUNT_IF 

The suffix `_IF` can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition.

```sql
COUNT_IF(<column>, <cond>)
```

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE orders (
  id INT,
  customer_id INT,
  status VARCHAR,
  total FLOAT
);

INSERT INTO orders (id, customer_id, status, total)
VALUES (1, 1, 'completed', 100),
       (2, 2, 'completed', 200),
       (3, 1, 'pending', 150),
       (4, 3, 'completed', 250),
       (5, 2, 'pending', 300);
```

**Query Demo: Count Completed Orders**
```sql
SELECT COUNT_IF(status, status = 'completed') AS completed_orders
FROM orders;
```

**Result**
```sql
| completed_orders |
|------------------|
|        3         |
```

