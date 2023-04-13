---
title: AVG
---

Aggregate function.

The AVG() function returns the average value of an expression.

**Note:** NULL values are not counted.

## Syntax

```sql
AVG(expression)
```

## Arguments

| Arguments  | Description              |
|------------|--------------------------|
| expression | Any numerical expression |

## Return Type

double

## Examples

**Creating a Table and Inserting Sample Data**

Let's create a table named "sales" and insert some sample data:
```sql
CREATE TABLE sales (
  id INTEGER,
  product VARCHAR(50),
  price FLOAT
);

INSERT INTO sales (id, product, price)
VALUES (1, 'Product A', 10.5),
       (2, 'Product B', 20.75),
       (3, 'Product C', 30.0),
       (4, 'Product D', 15.25),
       (5, 'Product E', 25.5);
```

**Query: Using AVG() Function**

Now, let's use the AVG() function to find the average price of all products in the "sales" table:
```sql
SELECT AVG(price) AS avg_price
FROM sales;
```

The result should look like this:
```sql
| avg_price |
| --------- |
| 20.4      |
```