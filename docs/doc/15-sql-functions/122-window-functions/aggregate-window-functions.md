---
title: Window Functions
---
## What is a window function?

Window functions perform calculations across rows of the query result. They run after the HAVING clause but before the ORDER BY clause.

Invoking a window function requires special syntax using the OVER clause to specify the window. The window can be specified in two ways(see [Window Function Syntax](window-function-syntax.md) for details).

## Supported functions in window functions

### Aggregate functions
All the aggregate functions supported by Databend can be used as aggregate window functions. See [Aggregate Functions](../10-aggregate-functions/index.md) for supported aggregate functions.


### Ranking functions

| Function Name | What It Does                                                                                                                                                                                                                                                        | 
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ROW_NUMBER    | Returns a unique, sequential number for each row, starting with one, according to the ordering of rows within the window partition.                                                                                                                                 | 
| RANK          | Returns the rank of a value in a group of values. The rank is one plus the number of rows preceding the row that are not peer with the row. Thus, tie values in the ordering will produce gaps in the sequence. The ranking is performed for each window partition. | 
| DESEN_RANK    | Returns the rank of a value in a group of values. This is similar to RANK(), except that tie values do not produce gaps in the sequence.                                                                                                                            |
| CUME_DIST     | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810                                                                                                                                                               |
| PERCENT_RANK  | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810                                                                                                                                                                                                                                                               |
| NTILE(n)      | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810                                                                                                                                                                                                                                                               |


### Value functions(TODO)

| Function Name                    | What It Does                                                                                                                        | 
|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| FIRST_VALUE(x)                   | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810                               |
| LAST_VALUE(x)                    | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810|
| NTH_VALUE(x, offset)             | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810|
| LAG(x[,offset[,default_value]])  | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810|
| LEAD(x[,offset[,default_value]]) | Not implementation, as a good first issue, see: https://github.com/datafuselabs/databend/issues/10810|


## Examples

### Example 1:
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
-- use aggregate function with GROUP BY
SELECT date, AVG(amount) AS avg_amount_for_branch
FROM BookSold
GROUP BY date;

June 21|544.0
June 22|454.5
June 23|643.0
```

If we use the aggregate window function, the result will include all the rows:

```sql
-- use aggregate window function 
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
-- use aggregate window function without PARTITION BY in the OVER clause
SELECT date, AVG(amount) over () 
FROM BookSold

June 21|547.166666666667
June 21|547.166666666667
June 22|547.166666666667
June 22|547.166666666667
June 23|547.166666666667
June 23|547.166666666667
```

### Example 2:
Another example is find outliers in a time series.
```sql
select t, val, abs(val - (avg(val) over w))/(stddev(val) over w)
from measurement
window w as (
  partition by location
  order by time
  range between 5 preceding and 5 following)
```