---
title: ARG_MAX
---

Calculates the `arg` value for a maximum `val` value. If there are several values of `arg` for maximum values of `val`, returns the first of these values encountered.

## Syntax

```sql
ARG_MAX(<arg>, <val>)
```

## Arguments

| Arguments | Description                                                                                       |
|-----------|---------------------------------------------------------------------------------------------------|
| `<arg>`   | Argument of [any data type that Databend supports](../../13-sql-reference/10-data-types/index.md) |
| `<val>`   | Value of [any data type that Databend supports](../../13-sql-reference/10-data-types/index.md)    |

## Return Type

`arg` value that corresponds to maximum `val` value.

 matches `arg` type.

## Example

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

**Query: Using ARG_MAX() Function**

Now, let's use the ARG_MAX() function to find the product that has the maximum price:
```sql
SELECT ARG_MAX(product, price) AS max_price_product
FROM sales;
```

The result should look like this:
```sql
| max_price_product |
| ----------------- |
| Product C         |
```