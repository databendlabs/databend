---
title: Set Operators
description:
  Set operators combine the results of two queries into a single result.
---

Set operators combine the results of two queries into a single result. Databend supports the following set operators:

* INTERSECT
* EXCEPT
* UNION [ALL]

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

## UNION [ALL]

Combines rows from two or more result sets. Each result set must return the same number of columns, and the corresponding columns must have the same or compatible data types. 

The command removes duplicate rows by default when combining result sets. To include duplicate rows, use **UNION ALL**.

### Syntax

```sql
SELECT column1 , column2 ...
FROM table_names
WHERE condition

UNION [ALL]

SELECT column1 , column2 ...
FROM table_names
WHERE condition

[UNION [ALL]

SELECT column1 , column2 ...
FROM table_names
WHERE condition]...

[ORDER BY ...]
```

### Example

```sql
CREATE TABLE support_team 
  ( 
     NAME   STRING, 
     salary UINT32 
  ); 

CREATE TABLE hr_team 
  ( 
     NAME   STRING, 
     salary UINT32 
  ); 

INSERT INTO support_team 
VALUES      ('Alice', 
             1000), 
            ('Bob', 
             3000), 
            ('Carol', 
             5000); 

INSERT INTO hr_team 
VALUES      ('Davis', 
             1000), 
            ('Eva', 
             4000); 

-- The following code returns the employees in both teams who are paid less than 2,000 dollars:

SELECT NAME AS SelectedEmployee, 
       salary 
FROM   support_team 
WHERE  salary < 2000 
UNION 
SELECT NAME AS SelectedEmployee, 
       salary 
FROM   hr_team 
WHERE  salary < 2000 
ORDER  BY selectedemployee DESC; 
```

Output:

```sql
Davis|1000
Alice|1000
```