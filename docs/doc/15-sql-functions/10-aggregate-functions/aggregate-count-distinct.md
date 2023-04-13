---
title: COUNT_DISTINCT
title_includes: uniq
---

Aggregate function.

The count(distinct ...) function calculates the uniq value of a set of values.

To obtain an estimated result from large data sets with little memory and time, consider using [APPROX_COUNT_DISTINCT](aggregate-approx-count-distinct.md).

:::caution
 NULL values are not counted.
:::

## Syntax

```sql
COUNT(distinct arguments ...)
UNIQ(arguments)
```

## Arguments

| Arguments   | Description |
| ----------- | ----------- |
| expression  | Any expression, size of the arguments is [1, 32] |

## Return Type

UInt64

## Example

**Create a Table and Insert Sample Data**
```sql
CREATE TABLE products (
  id INT,
  name VARCHAR,
  category VARCHAR,
  price FLOAT
);

INSERT INTO products (id, name, category, price)
VALUES (1, 'Laptop', 'Electronics', 1000),
       (2, 'Smartphone', 'Electronics', 800),
       (3, 'Tablet', 'Electronics', 600),
       (4, 'Chair', 'Furniture', 150),
       (5, 'Table', 'Furniture', 300);
```

**Query Demo: Count Distinct Categories**

```sql
SELECT COUNT(DISTINCT category) AS unique_categories
FROM products;
```

**Result**
```sql
| unique_categories |
|-------------------|
|         2         |
```