---
title: COVAR_POP
---

COVAR_POP returns the population covariance of a set of number pairs. 

## Syntax

```sql
COVAR_POP(<expr1>, <expr2>)
```

## Arguments

| Arguments |        Description       |
|-----------| ------------------------ |
| `<expr1>` | Any numerical expression |
| `<expr2>` | Any numerical expression |

## Return Type

float64

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE product_sales (
  id INT,
  product_id INT,
  units_sold INT,
  revenue FLOAT
);

INSERT INTO product_sales (id, product_id, units_sold, revenue)
VALUES (1, 1, 10, 1000),
       (2, 2, 20, 2000),
       (3, 3, 30, 3000),
       (4, 4, 40, 4000),
       (5, 5, 50, 5000);
```

**Query Demo: Calculate Population Covariance between Units Sold and Revenue**

```sql
SELECT COVAR_POP(units_sold, revenue) AS covar_pop_units_revenue
FROM product_sales;
```

**Result**
```sql
| covar_pop_units_revenue |
|-------------------------|
|        20000.0          |
```