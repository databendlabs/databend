---
title: GROUP BY ROLLUP
---


`GROUP BY ROLLUP` is an extension of the [GROUP BY](./06-query-group-by.md) clause that produces sub-total rows (in addition to the grouped rows). Sub-total rows are rows that further aggregate whose values are derived by computing the same aggregate functions that were used to produce the grouped rows.

## Syntax

```sql
SELECT ...
FROM ...
[ ... ]
GROUP BY ROLLUP ( groupRollup [ , groupRollup [ , ... ] ] )
[ ... ]
```

Where:
```sql
groupRollup ::= { <column_alias> | <position> | <expr> }
```

- `<column_alias>`: Column alias appearing in the query block’s SELECT list

- `<position>`: Position of an expression in the SELECT list

- `<expr>`: Any expression on tables in the current scope


## Examples

Let's create a sample table named sales_data and insert some data:
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

Now, let's use the GROUP BY ROLLUP clause to get the total sales amount for each region and product, along with sub-totals for each region:
```sql
SELECT region, product, SUM(sales_amount) AS total_sales
FROM sales_data
GROUP BY ROLLUP (region, product);
```

The result will be:
```sql
+--------+---------+-------------+
| region | product | total_sales |
+--------+---------+-------------+
| South  | NULL    |         500 |
| West   | NULL    |         500 |
| North  | NULL    |         500 |
| West   | WidgetB |         200 |
| NULL   | NULL    |        1500 |
| North  | WidgetB |         300 |
| South  | WidgetA |         400 |
| North  | WidgetA |         200 |
| West   | WidgetA |         300 |
| South  | WidgetB |         100 |
+--------+---------+-------------+
```

In this example, the GROUP BY ROLLUP clause calculates the total sales for each region-product combination, each region, and the grand total.