---
title: JOIN
---

## Supported Join Types

The *join* combines columns from two or more tables into a single result set. Databend supports the following *join* types:

* [Inner Join](#inner-join)
* [Natural Join](#natural-join)
* [Cross Join](#cross-join)
* [Left Join](#left-join)
* [Right Join](#right-join)
* [Full Outer Join](#full-outer-join)
* [Left / Right Semi Join](#left--right-semi-join)
* [Left / Right Anti Join](#left--right-anti-join)

## Example Tables

Unless explicitly specified, the join examples on this page are created based on the following tables:

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

Table "gift": This table lists the gift options for the VIP clients.

| Gift      	|
|-----------	|
| Croissant 	|
| Donut     	|
| Coffee    	|
| Soda      	|

## Inner Join

The *inner join* returns the rows that meet the join conditions in the result set.

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
The keyword INNER is optional.
:::

When you join two tables on a common column with the equal operator, you can use the keyword USING to simplify the syntax.

```sql    
SELECT select_list
FROM table_a
	JOIN table_b
		USING join_column_1
	[JOIN table_c
		USING join_column_2]...
```

### Examples

The following example returns the purchase records of the VIP clients:

```sql    
SELECT purchase_records.client_id,
       purchase_records.item,
       purchase_records.qty
FROM   vip_info
       INNER JOIN purchase_records
               ON vip_info.client_id = purchase_records.client_id; 
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
|102|Donut|3000
|103|Coffee|6000
```

## Natural Join

The *natural join* joins two tables based on all columns in the two tables that have the same name.

### Syntax

```sql    
SELECT select_list
FROM table_a
	NATURAL JOIN table_b
	[NATURAL JOIN table_c]...
```

### Examples

The following example returns the purchase records of the VIP clients:

```sql    
SELECT purchase_records.client_id,
       purchase_records.item,
       purchase_records.qty
FROM   vip_info
       NATURAL JOIN purchase_records; 
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
|102|Donut|3,000
|103|Coffee|6,000
```

## Cross Join

The *cross join* returns a result set that includes each row from the first table joined with each row from the  second table.

### Syntax

```sql    
SELECT select_list
FROM table_a
	CROSS JOIN table_b
```

### Examples

The following example returns a result set that assigns each gift option to each VIP client:

```sql    
SELECT *
FROM   vip_info
       CROSS JOIN gift; 
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

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

## Left Join

The *left join* returns all records from the left table, and the matching records from the right table. The result is NULL records from the right side, if there is no match.

### Syntax

```sql    
SELECT select_list
FROM table_a
	LEFT [OUTER] JOIN table_b
		ON join_condition
```
:::tip
The keyword OUTER is optional.
:::

### Examples

The following example returns the purchase records of all VIP clients, the purchase records will be NULL if the VIP client has no purchases:

```sql    
SELECT vip_info.client_id,
       purchase_records.item,
       purchase_records.qty
FROM   vip_info
       LEFT JOIN purchase_records
              ON vip_info.client_id = purchase_records.client_id; 
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
|101|NULL|NULL
|102|Donut|3000
|103|Coffee|6000
```

## Right Join

The *right join* returns all records from the right table, and the matching records from the left table. The result is NULL records from the left side, if there is no match.

### Syntax

```sql    
SELECT select_list
FROM table_a
	RIGHT [OUTER] JOIN table_b
		ON join_condition
```

:::tip
The keyword OUTER is optional.
:::

### Examples

Imagine we have the following tables:

The following example returns all vip_info of all purchase_records, the vip_info will be NULL if purchase_record does not have the corresponding vip_info.

```sql    
SELECT vip_info.client_id,
       vip_info.region
FROM   vip_info
       RIGHT JOIN purchase_records
               ON vip_info.client_id = purchase_records.client_id; 
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
NULL|NULL
102|Quebec
103|Vancouver
NULL|NULL
```

## Full Outer Join

The *full outer join* returns all rows from both tables, matching up the rows wherever a match can be made and placing NULLs in the places where no matching row exists.

### Syntax

```sql
SELECT select_list
FROM   table_a
       FULL OUTER JOIN table_b
                    ON join_condition
```

:::tip
The keyword OUTER is optional.
:::

### Examples

The following example returns all matched and unmatched rows from both tables:

```sql
SELECT vip_info.region,
       purchase_records.item
FROM   vip_info
       FULL OUTER JOIN purchase_records
                    ON vip_info.client_id = purchase_records.client_id;
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
Toronto|NULL
Quebec|Donut
Vancouver|Coffee
NULL|Croissant
NULL|Soda
```

## Left / Right Semi Join

The *left semi join* returns rows from the left table that have a matching row in the right table. The *right semi join* returns rows from the right table that have a matching row in the left table.

### Syntax

```sql
-- Left Semi Join

SELECT select_list
FROM   table_a
       LEFT SEMI JOIN table_b
                    ON join_condition

-- Right Semi Join

SELECT select_list
FROM   table_a
       RIGHT SEMI JOIN table_b
                    ON join_condition
```

### Examples

The following example returns the VIP clients (Client_ID & Region) who have a purchase record:

```sql
SELECT *
FROM   vip_info
       LEFT SEMI JOIN purchase_records
                    ON vip_info.client_id = purchase_records.client_id;
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
102|Quebec
103|Vancouver
```

The following example returns the purchase records (Client_ID, Item, and QTY) of the VIP clients:

```sql
SELECT *
FROM   vip_info
       RIGHT SEMI JOIN purchase_records
                    ON vip_info.client_id = purchase_records.client_id;
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
|102|Donut|3000
|103|Coffee|6000
```

## Left / Right Anti Join

The *left anti join* returns rows from the left table that have NO matching row in the right table. The *right anti join* returns rows from the right table that have NO matching row in the left table.

### Syntax

```sql
-- Left Anti Join

SELECT select_list
FROM   table_a
       LEFT ANTI JOIN table_b
                    ON join_condition

-- Right Anti Join

SELECT select_list
FROM   table_a
       RIGHT ANTI JOIN table_b
                    ON join_condition
```

### Examples

The following example returns the VIP clients (Client_ID & Region) who have NO purchase records:

```sql
SELECT *
FROM   vip_info
       LEFT ANTI JOIN purchase_records
                    ON vip_info.client_id = purchase_records.client_id;
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
101|Toronto
```

The following example returns the purchase records (Client_ID, Item, and QTY) of non-VIP clients:

```sql
SELECT *
FROM   vip_info
       RIGHT ANTI JOIN purchase_records
                    ON vip_info.client_id = purchase_records.client_id;
```

For the definitions of the tables in the example, see [Example Tables](#example-tables).

Output:

```sql
|100|Croissant|2000
|106|Soda|4000
```