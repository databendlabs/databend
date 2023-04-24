---
title: COVAR_SAMP
---

Aggregate function.

The covar_samp() function returns the sample covariance (Σ((x - x̅)(y - y̅)) / (n - 1)) of two data columns.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
COVAR_SAMP(<expr1>, <expr2>)
```

## Arguments

| Arguments |        Description       |
|-----------| ------------------------ |
| `<expr1>` | Any numerical expression |
| `<expr2>` | Any numerical expression |

## Return Type

float64, when n <= 1, returns +∞.

## Example

**Create a Table and Insert Sample Data**

```sql
CREATE TABLE store_sales (
  id INT,
  store_id INT,
  items_sold INT,
  profit FLOAT
);

INSERT INTO store_sales (id, store_id, items_sold, profit)
VALUES (1, 1, 100, 1000),
       (2, 2, 200, 2000),
       (3, 3, 300, 3000),
       (4, 4, 400, 4000),
       (5, 5, 500, 5000);
```

**Query Demo: Calculate Sample Covariance between Items Sold and Profit**

```sql
SELECT COVAR_SAMP(items_sold, profit) AS covar_samp_items_profit
FROM store_sales;
```

**Result**

```sql
| covar_samp_items_profit |
|-------------------------|
|        250000.0         |
```