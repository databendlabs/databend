---
title: Subquery Operators
description:
  A subquery is a query nested within another query.
---

This topic provides reference information about the subquery operators supported in Databend.

A subquery is a query nested within another query.

## [ NOT ] EXISTS

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
mysql> select number from numbers(10) where number>5 and exists(select number from numbers(5) where number>4);
Query OK, 0 rows affected
```
`select number from numbers(5) where number>4` no rows are produced, `exists(select number from numbers(5) where number>4)` is FALSE.

```sql
mysql> select number from numbers(10) where number>5 and exists(select number from numbers(5) where number>3);
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
mysql> select number from numbers(10) where number>5 and not exists(select number from numbers(5) where number>4);
+--------+
| number |
+--------+
|      6 |
|      7 |
|      8 |
|      9 |
+--------+
```

`not exists(select number from numbers(5) where number>4)` is TRUE.



