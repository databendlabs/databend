---
title: (draft) Aggregate Window Functions
---

An aggregate window function comes with an aggregate function and an OVER clause. It operates on a set of rows and returns a single value for each row from the underlying query. The OVER clause indicates how to partition the rows in the result set. 

When you use aggregate functions with GROUP BY, a single row is returned for each unique set of values in the grouped columns. Aggregate window functions do not collapse rows. All of the rows in the result set are returned. See [Examples](#examples) for a detailed comparison.

All the aggregate functions supported by Databend can be used as aggregate window functions. See [Aggregate Functions](/doc/reference/functions/aggregate-functions) for supported aggregate functions.

## Syntax

```sql
<aggregate-function> ( <arguments> ) 
OVER ([PARTITION BY expression1 [, expression2] ...]
     [ORDER BY expression1 [ASC | DESC]] [, expression2 [ASC | DESC]] ... )
```

## Examples

Imagine that we manage a bookstore with two branches in Toronto and Ottawa. We create a table to store the transactions for both cities from June 21 to June 23.

```sql
-- create a table
CREATE TABLE BookSold (
  id INTEGER PRIMARY KEY,
  date TEXT NOT NULL,
  city TEXT NOT NULL,
  amount INTEGER NOT NULL
);

-- insert some values
INSERT INTO BookSold VALUES (1, 'June 21', 'Toronto', 685);
INSERT INTO BookSold VALUES (2, 'June 21', 'Ottawa', 403);
INSERT INTO BookSold VALUES (3, 'June 22', 'Toronto', 679);
INSERT INTO BookSold VALUES (4, 'June 22', 'Ottawa', 230);
INSERT INTO BookSold VALUES (5, 'June 23', 'Toronto', 379);
INSERT INTO BookSold VALUES (6, 'June 23', 'Ottawa', 907);

-- show the table
SELECT * FROM BookSold;

1|June 21|Toronto|685
2|June 21|Ottawa|403
3|June 22|Toronto|679
4|June 22|Ottawa|230
5|June 23|Toronto|379
6|June 23|Ottawa|907
```

If we use the aggregate function (AVG) to calculate the average amount of books sold for each branch, the result will be grouped by date:

```sql
-- use aggrerate function with GROUP BY
SELECT date, AVG(amount) AS avg_amount_for_branch
FROM BookSold
GROUP BY date;

June 21|544.0
June 22|454.5
June 23|643.0
```

If we use the aggrerate window function, the result will include all the rows:

```sql
-- use aggrerate window function 
SELECT date, AVG(amount) over (partition by date) 
FROM BookSold

June 21|544.0
June 21|544.0
June 22|454.5
June 22|454.5
June 23|643.0
June 23|643.0
```
If we leave the OVER clause empty, it calculates the average of the total amount of three days.

```sql
-- use aggrerate window function without PARTITION BY in the OVER clause
SELECT date, AVG(amount) over () 
FROM BookSold

June 21|547.166666666667
June 21|547.166666666667
June 22|547.166666666667
June 22|547.166666666667
June 23|547.166666666667
June 23|547.166666666667
```