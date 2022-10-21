---
title: Subquery Operators
description:
  A subquery is a query nested within another query.
---

A subquery is a query nested within another one. Databend supports the following subquery types:

- [Scalar Subquery](#scalar-subquery)
- [EXISTS / NOT EXISTS](#exists--not-exists)

## Scalar Subqueries

A scalar subquery selects only one column or expression and returns only one row at most. A SQL query can have scalar subqueries in any places where a column or expression is expected.

- If a scalar subquery returns 0 rows, Databend will use NULL as the subquery output.
- If a scalar subquery returns more than one row, Databend will throw an error.

### Example

```sql
CREATE TABLE t1 (a int);
CREATE TABLE t2 (a int);

INSERT INTO t1 VALUES (1);
INSERT INTO t1 VALUES (2);
INSERT INTO t1 VALUES (3);

INSERT INTO t2 VALUES (3);
INSERT INTO t2 VALUES (4);
INSERT INTO t2 VALUES (5);

SELECT * 
FROM   t1 
WHERE  t1.a < (SELECT Min(t2.a) 
               FROM   t2); 

--
+--------+
|      a |
+--------+
|      1 |
|      2 |
+--------+
```

## EXISTS / NOT EXISTS

An EXISTS subquery is a boolean expression that can appear in a WHERE clause:
* An EXISTS expression evaluates to TRUE if any rows are produced by the subquery.
* A NOT EXISTS expression evaluates to TRUE if no rows are produced by the subquery.

### Syntax

```sql
[ NOT ] EXISTS ( <query> )
```

:::note
* Correlated EXISTS subqueries are currently supported only in a WHERE clause.
:::

### Example

```sql
SELECT number FROM numbers(10) WHERE number>5 AND exists(SELECT number FROM numbers(5) WHERE number>4);
```
`SELECT number FROM numbers(5) WHERE number>4` no rows are produced, `exists(SELECT number FROM numbers(5) WHERE number>4)` is FALSE.

```sql
SELECT number FROM numbers(10) WHERE number>5 and exists(SELECT number FROM numbers(5) WHERE number>3);
+--------+
| number |
+--------+
|      6 |
|      7 |
|      8 |
|      9 |
+--------+
```

`EXISTS(SELECT NUMBER FROM NUMBERS(5) WHERE NUMBER>3)` is TRUE.

```sql
SELECT number FROM numbers(10) WHERE number>5 AND not exists(SELECT number FROM numbers(5) WHERE number>4);
+--------+
| number |
+--------+
|      6 |
|      7 |
|      8 |
|      9 |
+--------+
```

`not exists(SELECT number FROM numbers(5) WHERE number>4)` is TRUE.
