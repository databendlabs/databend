---
title: JOIN
---

A *join* allows you to combine columns from two or more tables into a single result set. Databend supports the following *join* types:

* Inner Join
* Natural Join
* Cross Join

:::tip

To use JOIN, you must enable the new Databend planner first. To do so, perform the following command in the SQL client:

```sql
> set enable_planner_v2=1;
```
:::

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

Imagine we have the following tables:

Table "vip_info": This table stores the VIP client information.

| Client_ID 	| Region    	|
|-------------	|-----------	|
| 101         	| Toronto   	|
| 102         	| Quebec    	|
| 103         	| Vancouver 	|

Table "purchase_records": This table lists the purchase records for all the clients.

| Client_ID 	| Item      	| QTY 	|
|-------------	|-----------	|-----	|
| 100         	| Croissant 	| 2,000   	|
| 102         	| Donut     	| 3,000   	|
| 103         	| Coffee    	| 6,000   	|
| 106         	| Soda      	| 4,000   	|

The following command returns the purchase records of the VIP clients:

```sql    
select * from vip_info inner join purchase_records on vip_info.Client_ID = purchase_records.Client_ID;
```

Output:

```sql
102|Quebec|102|Donut|3000
103|Vancouver|103|Coffee|6000
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

Imagine we have the following tables:

Table "vip_info": This table stores the VIP client information.

| Client_ID 	| Region    	|
|-------------	|-----------	|
| 101         	| Toronto   	|
| 102         	| Quebec    	|
| 103         	| Vancouver 	|

Table "purchase_records": This table lists the purchase records for all the clients.

| Client_ID 	| Item      	| QTY 	|
|-------------	|-----------	|-----	|
| 100         	| Croissant 	| 2,000   	|
| 102         	| Donut     	| 3,000   	|
| 103         	| Coffee    	| 6,000   	|
| 106         	| Soda      	| 4,000   	|

The following command returns the purchase records of the VIP clients:

```sql    
select * from vip_info natural join purchase_records;
```

Output:

```sql
102|Quebec|Donut|3,000
103|Vancouver|Coffee|6,000
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

Imagine we have the following tables:

Table "vip_info": This table stores the VIP client information.

| Client_ID 	| Region    	|
|-------------	|-----------	|
| 101         	| Toronto   	|
| 102         	| Quebec    	|
| 103         	| Vancouver 	|

Table "gift": This table lists the gift options for the VIP clients.

| Gift      	|
|-----------	|
| Croissant 	|
| Donut     	|
| Coffee    	|
| Soda      	|

The following command returns a result set that assigns each gift option to each VIP client:

```sql    
select * from vip_info natural join gift;
```

Output:

```sql
101|Toronto|Croissant
101|Toronto|Donut
101|Toronto|Coffee
101|Toronto|Soda
102|Quebec|Croissant
102|Quebec|Donut
102|Quebec|Coffee
102|Quebec|Soda
103|Vancouver|Croissant
103|Vancouver|Donut
103|Vancouver|Coffee
103|Vancouver|Soda
```