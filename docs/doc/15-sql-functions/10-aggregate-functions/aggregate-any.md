---
title: ANY
---

Aggregate function.

The ANY() function selects the first encountered (non-NULL) value, unless all rows have NULL values in that column. The query can be executed in any order and even in a different order each time, so the result of this function is indeterminate. To get a determinate result, you can use the ‘min’ or ‘max’ function instead of ‘any’.

## Syntax

```sql
ANY(expression)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression |

## Return Type

The first encountered (non-NULL) value, in the type of the value. If all values are NULL, the return value is NULL.

## Example

**Create a Table and Insert Sample Data*
```sql
CREATE TABLE product_data (
  id INT,
  product_name VARCHAR NULL,
  price FLOAT NULL
);

INSERT INTO product_data (id, product_name, price)
VALUES (1, 'Laptop', 1000),
       (2, NULL, 800),
       (3, 'Keyboard', NULL),
       (4, 'Mouse', 25),
       (5, 'Monitor', 150);
```

**Query Demo: Retrieve the First Encountered Non-NULL Product Name**
```sql
SELECT ANY(product_name) AS any_product_name
FROM product_data;
```

**Result**
```sql
| any_product_name |
|------------------|
| Laptop           |
```