---
title: PIVOT
---

The `PIVOT` operation in Databend allows you to transform a table by rotating it and aggregating results based on specified columns. 

It is a useful operation for summarizing and analyzing large amounts of data in a more readable format. In this document, we will explain the syntax and provide an example of how to use the `PIVOT` operation.

**See also:**
[UNPIVOT](./05-query-unpivot.md)


## Syntax

```sql
SELECT ...
FROM ...
   PIVOT ( <aggregate_function> ( <pivot_column> )
            FOR <value_column> IN ( <pivot_value_1> [ , <pivot_value_2> ... ] ) )

[ ... ]
```

Where:
* `<aggregate_function>`: The aggregate function for combining the grouped values from `pivot_column`.
* `<pivot_column>`: The column that will be aggregated using the specified `<aggregate_function>`.
* `<value_column>`: The column whose unique values will become new columns in the pivoted result set.
* `<pivot_value_N>`: A unique value from the `<value_column>` that will become a new column in the pivoted result set.


## Examples

Let's say we have a table called monthly_sales that contains sales data for different employees across different months. We can use the `PIVOT` operation to summarize the data and calculate the total sales for each employee in each month.

### Creating and Inserting Data


```sql
-- Create the monthly_sales table
CREATE TABLE monthly_sales(
  empid INT, 
  amount INT, 
  month VARCHAR
);

-- Insert sales data
INSERT INTO monthly_sales VALUES
  (1, 10000, 'JAN'),
  (1, 400, 'JAN'),
  (2, 4500, 'JAN'),
  (2, 35000, 'JAN'),
  (1, 5000, 'FEB'),
  (1, 3000, 'FEB'),
  (2, 200, 'FEB'),
  (2, 90500, 'FEB'),
  (1, 6000, 'MAR'),
  (1, 5000, 'MAR'),
  (2, 2500, 'MAR'),
  (2, 9500, 'MAR'),
  (1, 8000, 'APR'),
  (1, 10000, 'APR'),
  (2, 800, 'APR'),
  (2, 4500, 'APR');
```

### Using PIVOT

Now, we can use the `PIVOT` operation to calculate the total sales for each employee in each month. We will use the `SUM` aggregate function to calculate the total sales, and the MONTH column will be pivoted to create new columns for each month.

```sql
SELECT * 
FROM monthly_sales
PIVOT(SUM(amount) FOR MONTH IN ('JAN', 'FEB', 'MAR', 'APR'))
ORDER BY EMPID;
```

Output:
```sql
+-------+-------+-------+-------+-------+
| empid | jan   | feb   | mar   | apr   |
+-------+-------+-------+-------+-------+
|     1 | 10400 |  8000 | 11000 | 18000 |
|     2 | 39500 | 90700 | 12000 |  5300 |
+-------+-------+-------+-------+-------+
```