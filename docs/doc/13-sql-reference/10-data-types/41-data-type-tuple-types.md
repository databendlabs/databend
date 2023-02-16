---
title: Tuple
description:  Tuple is a collection of ordered, immutable types.
---

## Tuple Data Types
| Name    |  Aliases  |  Values        | Description
|---------|-----------|--------------------------|----------------
| TUPLE   |           | ('2023-02-14 08:00:00','Valentine's Day') | Collection of ordered,immmutable,which requires the type of each element to be declared before being used.

A tuple is a collection of ordered, immutable, and heterogeneous elements, represented within parentheses () in most programming languages. In other words, a tuple is a finite ordered list of elements of different data types, and once created, its elements cannot be changed or modified.

Tuples are commonly used to store related data, such as the coordinates of a point in a 2D space (x, y), or a name and its corresponding address, or a date and its corresponding event, and so on.

> but it's not suggested to use it unless you really need it.

### Example

Create a table:
```sql
CREATE TABLE t_table(event tuple(datetime, varchar));
```

Insert a value with different type into the table:
```sql
insert into t_table values(('2023-02-14 8:00:00','Valentine\'s Day'));
```

Query the result:
```sql
SELECT * FROM t_table;
+---------------------------------------------------+
| event                                             |
+---------------------------------------------------+
| ('2023-02-14 08:00:00.000000','Valentine\'s Day') |
+---------------------------------------------------+
```

## Get by index

The elements of a Databend tuple can be accessed by their indices, **which start from 1**. 

### Example

```sql
mysql> select event.1 from t_table;
+----------------------------+
| event.1                    |
+----------------------------+
| 2023-02-14 08:00:00.000000 |
+----------------------------+
1 row in set (0.03 sec)
```
