---
title: Subquery Operators
description:
  A subquery is a query nested within another query.
---

A subquery is a query nested within another one. Databend supports the following subquery types:

- [Scalar Subquery](#scalar-subquery)
- [EXISTS / NOT EXISTS](#exists--not-exists)
- [IN / NOT IN](#in--not-in)
- [ANY (SOME)](#any-some)
- [ALL](#all)

## Scalar Subquery

A scalar subquery selects only one column or expression and returns only one row at most. A SQL query can have scalar subqueries in any places where a column or expression is expected.

- If a scalar subquery returns 0 rows, Databend will use NULL as the subquery output.
- If a scalar subquery returns more than one row, Databend will throw an error.

### Examples

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

### Examples

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

## IN / NOT IN

By using IN or NOT IN, you can check whether an expression matches any value in a list returned by a subquery.

- When you use IN or NOT IN, the subquery must return a single column of values. 

### Syntax

```sql
[ NOT ] IN ( <query> )
```

### Examples

```sql
CREATE TABLE t1 (a int);
CREATE TABLE t2 (a int);

INSERT INTO t1 VALUES (1);
INSERT INTO t1 VALUES (2);
INSERT INTO t1 VALUES (3);

INSERT INTO t2 VALUES (3);
INSERT INTO t2 VALUES (4);
INSERT INTO t2 VALUES (5);

-- IN example
SELECT * 
FROM   t1 
WHERE  t1.a IN (SELECT *
               FROM   t2);

--
+--------+
|      a |
+--------+
|      3 |
+--------+

-- NOT IN example
SELECT * 
FROM   t1 
WHERE  t1.a NOT IN (SELECT *
               FROM   t2);

--
+--------+
|      a |
+--------+
|      1 |
|      2 |
+--------+
```

## ANY (SOME)

You can use ANY (or SOME) to check whether a comparison is true for any of the values returned by a subquery.

- The keyword ANY (or SOME) must follow a [comparison operator](../../15-sql-functions/02-comparisons-operators/index.md).
- If the subquery doesn't return any values, the comparison evaluates to false.
- SOME works the same way as ANY.

### Syntax

```sql
-- ANY
comparison_operator ANY ( <query> )

-- SOME
comparison_operator SOME ( <query> )
```

### Examples

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
WHERE  t1.a < ANY (SELECT * 
                   FROM   t2);

--
+--------+
|      a |
+--------+
|      1 |
|      2 |
|      3 |
+--------+
```

## ALL

You can use ALL to check whether a comparison is true for all of the values returned by a subquery.

- The keyword ALL must follow a [comparison operator](../../15-sql-functions/02-comparisons-operators/index.md).
- If the subquery doesn't return any values, the comparison evaluates to true.

### Syntax

```sql
comparison_operator ALL ( <query> )
```

### Examples

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
WHERE  t1.a < ALL (SELECT * 
                   FROM   t2);

--
+--------+
|      a |
+--------+
|      1 |
|      2 |
+--------+
```