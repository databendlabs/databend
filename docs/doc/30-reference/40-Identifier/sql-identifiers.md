---
title: SQL IDENTIFIERS
---

SQL identifiers is the name of the database objects.

Such as `table`, `view`, `database` these objects are examples of SQL identifiers:

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

## Resolution

By default, Databend applies the following rules for storing identifiers (at creation/definition time) and resolving them (in queries and other SQL statements):

* When an identifier is unquoted, it is stored and resolved in uppercase.

* When an identifier is double-quoted, it is stored and resolved exactly as entered, including case.

If want to preserve the case of characters when use `unquoted identifier`, need set [unquoted_ident_case_sensitive](../30-sql/70-system-tables/system-settings.md) = 1.

Examples:

```sql
databend :) set unquoted_ident_case_sensitive=1;

databend :) create table Tt(id int);

databend :) desc Tt;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

databend :) create table tt(id1 int);
Query OK, 0 rows affected (0.08 sec)

databend :) desc tt;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id1   | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

```

If do not want to preserve the case of characters when use `double identifier`, need set [quoted_ident_case_sensitive](../30-sql/70-system-tables/system-settings.md) = 0.

Examples:

```sql
databend :) set quoted_ident_case_sensitive=0;
Query OK, 0 rows affected (0.03 sec)

databend :) create table "Test"(id int);
Query OK, 0 rows affected (0.06 sec)

databend :) desc Test;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

databend :) desc test;
+-------+------+------+---------+-------+
| Field | Type | Null | Default | Extra |
+-------+------+------+---------+-------+
| id    | INT  | NO   | 0       |       |
+-------+------+------+---------+-------+

```

