---
title: GROUP BY GROUPING SETS
---

`GROUP BY GROUPING SETS` is a powerful extension of the [GROUP BY](./06-query-group-by.md) clause that allows computing multiple group-by clauses in a single statement. The group set is a set of dimension columns.

`GROUP BY GROUPING SETS` is equivalent to the UNION of two or more GROUP BY operations in the same result set:

- `GROUP BY GROUPING SETS((a))` is equivalent to the single grouping set operation `GROUP BY a`.

- `GROUP BY GROUPING SETS((a),(b))` is equivalent to `GROUP BY a UNION ALL GROUP BY b`.

## Syntax

```sql
SELECT ...
FROM ...
[ ... ]
GROUP BY GROUPING SETS ( groupSet [ , groupSet [ , ... ] ] )
[ ... ]
```

Where:
```sql
groupSet ::= { <column_alias> | <position> | <expr> }
```

- `<column_alias>`: Column alias appearing in the query block’s SELECT list

- `<position>`: Position of an expression in the SELECT list

- `<expr>`: Any expression on tables in the current scope


## Examples

Sample Data Setup:
```sql
-- Create a sample sales table
CREATE TABLE sales (
    id INT,
    sale_date DATE,
    product_id INT,
    store_id INT,
    quantity INT
);

-- Insert sample data into the sales table
INSERT INTO sales (id, sale_date, product_id, store_id, quantity)
VALUES (1, '2021-01-01', 101, 1, 5),
       (2, '2021-01-01', 102, 1, 10),
       (3, '2021-01-01', 101, 2, 15),
       (4, '2021-01-02', 102, 1, 8),
       (5, '2021-01-02', 101, 2, 12),
       (6, '2021-01-02', 103, 2, 20);
```

### GROUP BY GROUPING SETS with column aliases

```sql
SELECT product_id AS pid,
       store_id AS sid,
       SUM(quantity) AS total_quantity
FROM sales
GROUP BY GROUPING SETS((pid), (sid));
```

This query is equivalent to:

```sql
SELECT product_id AS pid,
       NULL AS sid,
       SUM(quantity) AS total_quantity
FROM sales
GROUP BY pid
UNION ALL
SELECT NULL AS pid,
       store_id AS sid,
       SUM(quantity) AS total_quantity
FROM sales
GROUP BY sid;
```

Output:
```sql
+------+------+----------------+
| pid  | sid  | total_quantity |
+------+------+----------------+
|  102 | NULL |             18 |
| NULL |    2 |             47 |
|  101 | NULL |             32 |
|  103 | NULL |             20 |
| NULL |    1 |             23 |
+------+------+----------------+
```

### GROUP BY GROUPING SETS with positions

```sql
SELECT product_id,
       store_id,
       SUM(quantity) AS total_quantity
FROM sales
GROUP BY GROUPING SETS((1), (2));
```

This query is equivalent to:

```sql
SELECT product_id,
       NULL AS store_id,
       SUM(quantity) AS total_quantity
FROM sales
GROUP BY product_id
UNION ALL
SELECT NULL AS product_id,
       store_id,
       SUM(quantity) AS total_quantity
FROM sales
GROUP BY store_id;
```

Output:
```sql
+------------+----------+----------------+
| product_id | store_id | total_quantity |
+------------+----------+----------------+
|        102 |     NULL |             18 |
|       NULL |        2 |             47 |
|        101 |     NULL |             32 |
|        103 |     NULL |             20 |
|       NULL |        1 |             23 |
+------------+----------+----------------+
```

