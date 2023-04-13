---
title: KURTOSIS
---

Aggregate function.

The `KURTOSIS()` function returns the excess kurtosis of all input values.

## Syntax

```sql
KURTOSIS(expression)
```

## Arguments

| Arguments   | Description                     |
| ----------- | -----------                     |
| expression  | Any numerical expression        |

## Return Type

Nullable Float64.

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE stock_prices (
  id INT,
  stock_symbol VARCHAR,
  price FLOAT
);

INSERT INTO stock_prices (id, stock_symbol, price)
VALUES (1, 'AAPL', 150),
       (2, 'AAPL', 152),
       (3, 'AAPL', 148),
       (4, 'AAPL', 160),
       (5, 'AAPL', 155);
```

**Query Demo: Calculate Excess Kurtosis for Apple Stock Prices**

```sql
SELECT KURTOSIS(price) AS excess_kurtosis
FROM stock_prices
WHERE stock_symbol = 'AAPL';
```

**Result**

```sql
|     excess_kurtosis     |
|-------------------------|
| 0.06818181325581445     |
```
