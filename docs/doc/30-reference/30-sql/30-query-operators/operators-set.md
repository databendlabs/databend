---
title: Set Operators
description:
  Set operators combine the results of two queries into a single result.
---

Set operators combine the results of two queries into a single result. Databend supports the following set operators:

* INTERSECT
* EXCEPT

## INTERSECT

Returns all distinct rows selected by both queries.

### Syntax

```sql
SELECT column1 , column2 ....
FROM table_names
WHERE condition

INTERSECT

SELECT column1 , column2 ....
FROM table_names
WHERE condition
```

### Example

```sql
create table t1(a int, b int);
create table t2(c int, d int);

insert into t1 values(1, 2), (2, 3), (3 ,4), (2, 3);
insert into t2 values(2,2), (3, 5), (7 ,8), (2, 3), (3, 4);

select * from t1 intersect select * from t2;
```

Output:

```sql
2|3
3|4

```

## EXCEPT

Returns All distinct rows selected by the first query but not the second.

### Syntax

```sql
SELECT column1 , column2 ....
FROM table_names
WHERE condition

EXCEPT

SELECT column1 , column2 ....
FROM table_names
WHERE condition
```

### Example

```sql
create table t1(a int, b int);
create table t2(c int, d int);

insert into t1 values(1, 2), (2, 3), (3 ,4), (2, 3);
insert into t2 values(2,2), (3, 5), (7 ,8), (2, 3), (3, 4);

select * from t1 except select * from t2;
```

Output:

```sql
1|2

```
