---
title: GROUP BY CUBE
---

`GROUP BY CUBE` is an extension of the [GROUP BY](./06-query-group-by.md) clause similar to [GROUP BY ROLLUP](./09-query-group-by-rollup.md). In addition to producing all the rows of a `GROUP BY ROLLUP`, `GROUP BY CUBE` adds all the "cross-tabulations" rows. Sub-total rows are rows that further aggregate whose values are derived by computing the same aggregate functions that were used to produce the grouped rows.

A `CUBE` grouping is equivalent to a series of grouping sets and is essentially a shorter specification. The N elements of a CUBE specification correspond to `2^N GROUPING SETS`.

## Syntax

```sql
SELECT ...
FROM ...
[ ... ]
GROUP BY CUBE ( groupCube [ , groupCube [ , ... ] ] )
[ ... ]
```

Where:
```sql
groupCube ::= { <column_alias> | <position> | <expr> }
```

- `<column_alias>`: Column alias appearing in the query block’s SELECT list

- `<position>`: Position of an expression in the SELECT list

- `<expr>`: Any expression on tables in the current scope


## Examples

Let's assume we have a sales_data table with the following schema and sample data:

```sql
CREATE TABLE sales_data (
  region VARCHAR(255),
  product VARCHAR(255),
  sales_amount INT
);

INSERT INTO sales_data (region, product, sales_amount) VALUES
  ('North', 'WidgetA', 200),
  ('North', 'WidgetB', 300),
  ('South', 'WidgetA', 400),
  ('South', 'WidgetB', 100),
  ('West', 'WidgetA', 300),
  ('West', 'WidgetB', 200);
```

Now, let's use the `GROUP BY CUBE` clause to get the total sales amount for each region and product, along with all possible aggregations:

```sql
SELECT region, product, SUM(sales_amount) AS total_sales
FROM sales_data
GROUP BY CUBE (region, product);
```

The result will be:
```sql
+--------+---------+-------------+
| region | product | total_sales |
+--------+---------+-------------+
| South  | NULL    |         500 |
| NULL   | WidgetB |         600 |
| West   | NULL    |         500 |
| North  | NULL    |         500 |
| West   | WidgetB |         200 |
| NULL   | NULL    |        1500 |
| North  | WidgetB |         300 |
| South  | WidgetA |         400 |
| North  | WidgetA |         200 |
| NULL   | WidgetA |         900 |
| West   | WidgetA |         300 |
| South  | WidgetB |         100 |
+--------+---------+-------------+
```
