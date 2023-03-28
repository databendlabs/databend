---
title: SQL IDENTIFIERS
sidebar_label: SQL Identifiers
---

SQL identifiers are names given to various database objects like tables, views, and databases. 

The following are examples of SQL identifiers:

## Requirement

Unquoted object identifiers:

* Begin with a Unicode letter (A-Z, a-z) or an underscore (_). Subsequent characters can only be letters, underscores, digits (0-9), or dollar signs ($).

* In default, Are stored and resolved as lowercase characters (e.g. ID is stored and resolved as id).

Double-quoted object Identifiers:

* The identifier can contain and can even start with any ASCII character from the blank character (32) to the tilde (126).

* In default, The case of the identifier is preserved when storing and resolving the identifier (e.g. "Id" is stored and resolved as Id).

Examples:

```sql
databend :) create table " with""TestQuote""" (id int);

databend :) desc ` with""TestQuote""`;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

```

### Unquoted Identifiers

If an identifier is not enclosed in double quotes, it must begin with a letter or underscore (_) and cannot contain extended characters or blank spaces.

The following are all examples of valid identifiers; however, in default, the case of the characters in these identifiers would not be preserved:

```
myidentifier
MyIdentifier1
My$identifier
_my_identifier
```

### Double-quoted Identifiers

In default, Double-quoted identifiers are case-sensitive and can start with and contain any valid characters, including:

* Numbers

* Special characters (., ', !, @, #, $, %, ^, &, *, etc.)

* Extended ASCII and non-ASCII characters

* Blank spaces

```
"MyIdentifier"
"my.identifier"
"my identifier"
"My 'Identifier'"
"3rd_identifier"
"$Identifier"
"идентификатор"
```

Examples:
```sql
create table "BigTable" (a int);

show tables;
+--------------------+
| tables_in_default  |
+--------------------+
| BigTable           |
+--------------------+
    
desc "BigTable";
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| a     | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+
    
desc BigTable;
ERROR 1105 (HY000): Code: 1025, Text = Unknown table 'bigtable'.
```

## Identifier Resolution

By default, Databend applies the following rules for storing identifiers (at creation/definition time) and resolving them (in queries and other SQL statements):

* When an identifier is unquoted, it is stored and resolved in lowercase.

* When an identifier is double-quoted, it is stored and resolved exactly as entered, including case.

If you want to preserve the case of characters when use `unquoted identifier`, need set [unquoted_ident_case_sensitive](20-system-tables/system-settings.md) = 1.

Examples:

```sql
set unquoted_ident_case_sensitive=1;

create table Tt(id int);

desc Tt;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

create table tt(id1 int);
Query OK, 0 rows affected (0.08 sec)

desc tt;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id1   | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

```

If you do not want to preserve the case of characters when use `double identifier`, need set [quoted_ident_case_sensitive](20-system-tables/system-settings.md) = 0.

Examples:

```sql
set quoted_ident_case_sensitive=0;

create table "Test"(id int);

desc Test;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

desc test;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+
    
desc "Test";
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

desc "test";
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+
```

## Identifiers Case-insensitive

In Databend, SQL keywords and identifiers are not case-sensitive.

## String Identifiers

In general, if an item is a string (e.g. text and dates) must be surrounded by single quotes (`'`):
```sql
INSERT INTO weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');
```

```sql
select 'demo';
+--------+
| 'demo' |
+--------+
| demo   |
+--------+

select "demo";
ERROR 1105 (HY000): Code: 1065, Text = error:
  --> SQL:1:8
  |
1 | select "demo"
  |        ^^^^^^ column doesn't exist
```

By default, Databend SQL dialect is `PostgreSQL`:
```sql
show settings like '%sql_dialect%';
+-------------+------------+------------+---------+------------------------------------------------------------------------------------+--------+
| name        | value      | default    | level   | description                                                                        | type   |
+-------------+------------+------------+---------+------------------------------------------------------------------------------------+--------+
| sql_dialect | PostgreSQL | PostgreSQL | SESSION | SQL dialect, support "PostgreSQL" "MySQL" and "Hive", default value: "PostgreSQL". | String |
+-------------+------------+------------+---------+------------------------------------------------------------------------------------+--------+
```

You can change it to `MySQL` to enable double quotes (`"`):
```sql
set sql_dialect='MySQL';

select "demo";
+--------+
| 'demo' |
+--------+
| demo   |
+--------+
```
