---
title: QUANTILE_DISC
---

Aggregate function.

The `QUANTILE_DISC()` function computes the exact quantile number of a numeric data sequence.
The `QUANTILE` alias to `QUANTILE_DISC`

:::caution
NULL values are not counted.
:::

## Syntax

```sql
QUANTILE_DISC(<levels>)(<expr>)
    
QUANTILE_DISC(level1, level2, ...)(<expr>)
```

## Arguments

| Arguments  | Description                                                                                                                                   |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `level(s)` | level(s) of quantile. Each level is constant floating-point number from 0 to 1. We recommend using a level value in the range of [0.01, 0.99] |
| `<expr>`   | Any numerical expression                                                                                                                      |

## Return Type

InputType or array of InputType based on level number.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE salary_data (
  id INT,
  employee_id INT,
  salary FLOAT
);

INSERT INTO salary_data (id, employee_id, salary)
VALUES (1, 1, 50000),
       (2, 2, 55000),
       (3, 3, 60000),
       (4, 4, 65000),
       (5, 5, 70000);
```

**Query Demo: Calculate 25th and 75th Percentile of Salaries**
```sql
SELECT QUANTILE_DISC(0.25, 0.75)(salary) AS salary_quantiles
FROM salary_data;
```

**Result**
```sql
|  salary_quantiles   |
|---------------------|
| [55000.0, 65000.0]  |
```