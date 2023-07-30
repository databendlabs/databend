---
title: QUANTILE_TDIGEST
---

Aggregate function.

The QUANTILE_TDIGEST() function computes an approximate quantile of a numeric data sequence using the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algorithm.

:::caution
NULL values are not counted.
:::

## Syntax

```sql
QUANTILE_TDIGEST(<levels>)(<expr>)
    
QUANTILE_TDIGEST(level1, level2, ...)(<expr>)
```

## Arguments

| Arguments   | Description                                                                                                                                     |
|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `<level(s)` | level(s) of quantile. Each level is constant floating-point number from 0 to 1. We recommend using a level value in the range of [0.01, 0.99]   |
| `<expr>`    | Any numerical expression                                                                                                                        |

## Return Type

Float64 or float64 array based on level number.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE sales_data (
  id INT,
  sales_person_id INT,
  sales_amount FLOAT
);

INSERT INTO sales_data (id, sales_person_id, sales_amount)
VALUES (1, 1, 5000),
       (2, 2, 5500),
       (3, 3, 6000),
       (4, 4, 6500),
       (5, 5, 7000);
```

**Query Demo: Calculate 50th Percentile (Median) of Sales Amount using Interpolation**
```sql
SELECT QUANTILE_TDIGEST(0.5)(sales_amount) AS median_sales_amount
FROM sales_data;
```

**Result**
```sql
|  median_sales_amount  |
|-----------------------|
|        6000.0         |
```

