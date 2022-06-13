---
title: JOIN
---

A *join* allows you to combine columns from two or more tables into a single result set. Databend supports the following *join* types:

* Inner Join
* Natural Join
* Cross Join

## Inner Join

An *inner join* returns the rows that meet the join conditions in the result set.

### Syntax

```sql    
SELECT select_list
FROM table_a
	[INNER] JOIN table_b
		ON join_condition_1
	[[INNER] JOIN table_c
		ON join_condition_2]...
```
:::tip
The INNER keyword is optional.
:::

When you join two tables on a common column with the equal operator, you can use the USING keyword to simplify the syntax.

```sql    
SELECT select_list
FROM table_a
	JOIN table_b
		USING join_column_1
	[JOIN table_c
		USING join_column_2]...
```

### Examples

```sql    
create table t(a int);
insert into t values(1),(2),(3);
create table t1(a float);
insert into t1 values(1.0),(2.0),(3.0);
create table t2(c smallint unsigned null);
insert into t2 values(1),(2),(null);

select * from t inner join t2 on t.a = t2.c;
select * from t inner join t1 using (a);
```
Output:

```sql
1|1
2|2
1
2
3
```


## Natural Join

A *natural join* joins two tables based on all columns in the two tables that have the same name.

### Syntax

```sql    
SELECT select_list
FROM table_a
	NATURAL JOIN table_b
	[NATURAL JOIN table_c]...
```

### Examples

```sql    
create table t1(a int, b int);
insert into t1 values(7, 8), (3, 4), (5, 6);
drop table if exists t2;
create table t2(a int, d int);
insert into t2 values(1, 2), (3, 4), (5, 6);

select * from t1 natural join t2;
```
Output:

```sql
3|4|4
5|6|6
```

## Cross Join

A *cross join* returns a result set that includes each row from the first table joined with each row from the  second table.

### Syntax

```sql    
SELECT select_list
FROM table_a
	CROSS JOIN table_b
```

### Examples

```sql    
create table t1(a int, b int);
create table t2(c int, d int);
insert into t1 values(1, 2), (2, 3), (3 ,4);
insert into t2 values(2,2), (3, 5), (7 ,8);

select * from t1 cross join t2
```
Output:

```sql
1|2|2|2
1|2|3|5
1|2|7|8
2|3|2|2
2|3|3|5
2|3|7|8
3|4|2|2
3|4|3|5
3|4|7|8
```
