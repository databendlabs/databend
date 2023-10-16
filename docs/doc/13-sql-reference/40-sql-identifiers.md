---
title: SQL Identifiers
sidebar_label: SQL Identifiers
---

SQL identifiers are names used for different elements within Databend, such as tables, views, and databases.

## Unquoted & Double-quoted Identifiers

Unquoted identifiers begin with a letter (A-Z, a-z) or underscore (“_”) and may consist of letters, underscores, numbers (0-9), or dollar signs (“$”).

```text title='Examples:'
mydatabend
MyDatabend1
My$databend
_my_databend
```

Double-quoted identifiers can include a wide range of characters, such as numbers (0-9), special characters (like period (.), single quote ('), exclamation mark (!), at symbol (@), number sign (#), dollar sign ($), percent sign (%), caret (^), and ampersand (&)), extended ASCII and non-ASCII characters, as well as blank spaces.

```text title='Examples:'
"MyDatabend"
"my.databend"
"my databend"
"My 'Databend'"
"1_databend"
"$Databend"
```

Note that using double backticks (``) or double quotes (") is equivalent:

```text title='Examples:'
`MyDatabend`
`my.databend`
`my databend`
`My 'Databend'`
`1_databend`
`$Databend`
```

## Identifier Casing Rules

Databend stores unquoted identifiers by default in lowercase and double-quoted identifiers as they are entered. In other words, Databend handles object names, such as databases, tables, and columns, as case-insensitive. If you want Databend to handle them as case-sensitive, double-quote them.

:::note
Databend allows you to have control over the casing sensitivity of identifiers. Two key settings are available:

- unquoted_ident_case_sensitive: When set to 1, this option preserves the case of characters for unquoted identifiers, ensuring they are case-sensitive. If left at the default value of 0, unquoted identifiers remain case-insensitive, converting to lowercase.

- quoted_ident_case_sensitive: By setting this option to 0, you can indicate that double-quoted identifiers should not preserve the case of characters, making them case-insensitive.
:::

This example demonstrates how Databend treats the casing of identifiers when creating and listing databases:

```sql
-- Create a database named "databend"
CREATE DATABASE databend;

-- Attempt to create a database named "Databend"
CREATE DATABASE Databend;

>> SQL Error [1105] [HY000]: DatabaseAlreadyExists. Code: 2301, Text = Database 'databend' already exists.

-- Create a database named "Databend"
CREATE DATABASE "Databend";

-- List all databases
SHOW DATABASES;

databases_in_default|
--------------------+
Databend            |
databend            |
default             |
information_schema  |
system              |
```

This example demonstrates how Databend handles identifier casing for table and column names, highlighting its case-sensitivity by default and the use of double quotes to differentiate between identifiers with varying casing:

```sql
-- Create a table named "databend"
CREATE TABLE databend (a INT);
DESC databend;

Field|Type|Null|Default|Extra|
-----+----+----+-------+-----+
a    |INT |YES |NULL   |     |

-- Attempt to create a table named "Databend"
CREATE TABLE Databend (a INT);

>> SQL Error [1105] [HY000]: TableAlreadyExists. Code: 2302, Text = Table 'databend' already exists.

-- Attempt to create a table with one column named "a" and the other one named "A"
CREATE TABLE "Databend" (a INT, A INT);

>> SQL Error [1105] [HY000]: BadArguments. Code: 1006, Text = Duplicated column name: a.

-- Double quote the column names
CREATE TABLE "Databend" ("a" INT, "A" INT);
DESC "Databend";

Field|Type|Null|Default|Extra|
-----+----+----+-------+-----+
a    |INT |YES |NULL   |     |
A    |INT |YES |NULL   |     |
```

## String Identifiers

In Databend, when managing string items like text and dates, it is essential to enclose them within single quotes (') as a standard practice.

```sql
INSERT INTO weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');

SELECT 'Databend';

'databend'|
----------+
Databend  |

SELECT "Databend";

>> SQL Error [1105] [HY000]: SemanticError. Code: 1065, Text = error: 
  --> SQL:1:73
  |
1 | /* ApplicationName=DBeaver 23.2.0 - SQLEditor <Script-12.sql> */ SELECT "Databend"
  |                                                                         ^^^^^^^^^^ column Databend doesn't exist, do you mean 'Databend'?
```

By default, Databend SQL dialect is `PostgreSQL`:

```sql
SHOW SETTINGS LIKE '%sql_dialect%';

name       |value     |default   |level  |description                                                                      |type  |
-----------+----------+----------+-------+---------------------------------------------------------------------------------+------+
sql_dialect|PostgreSQL|PostgreSQL|SESSION|Sets the SQL dialect. Available values include "PostgreSQL", "MySQL", and "Hive".|String|
```

You can change it to `MySQL` to enable double quotes (`"`):

```sql
SET sql_dialect='MySQL';

SELECT "demo";
+--------+
| 'demo' |
+--------+
| demo   |
+--------+
```