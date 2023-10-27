---
title: DROP TABLE
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.155"/>

Deletes a table.

**See also:**

- [CREATE TABLE](./10-ddl-create-table.md)
- [UNDROP TABLE](./21-ddl-undrop-table.md)
- [TRUNCATE TABLE](40-ddl-truncate-table.md)

## Syntax

```sql
DROP TABLE [IF EXISTS] [<database_name>.]<table_name> [ALL]
```

The optional "ALL" parameter determines whether the underlying data of the table is deleted. 

- If "ALL" is omitted, only the table schema is deleted from the metadata service, leaving the data intact. In this case, you can potentially recover the table using the [UNDROP TABLE](./21-ddl-undrop-table.md) command.

- Including "ALL" will result in the deletion of both the schema and the underlying data. While the [UNDROP TABLE](./21-ddl-undrop-table.md) command can recover the schema, it cannot restore the table's data.

## Examples

### Example 1: Deleting a Table

This example highlights the use of the DROP TABLE command to delete the "test" table. After dropping the table, any attempt to SELECT from it results in an "Unknown table" error. It also demonstrates how to recover the dropped "test" table using the UNDROP TABLE command, allowing you to SELECT data from it again.

```sql
CREATE TABLE test(a INT, b VARCHAR);
INSERT INTO test (a, b) VALUES (1, 'example');
SELECT * FROM test;

a|b      |
-+-------+
1|example|

-- Delete the table
DROP TABLE test;
SELECT * FROM test;
>> SQL Error [1105] [HY000]: UnknownTable. Code: 1025, Text = error: 
  --> SQL:1:80
  |
1 | /* ApplicationName=DBeaver 23.2.0 - SQLEditor <Script-12.sql> */ SELECT * FROM test
  |                                                                                ^^^^ Unknown table `default`.`test` in catalog 'default'

-- Recover the table
UNDROP TABLE test;
SELECT * FROM test;

a|b      |
-+-------+
1|example|
```

### Example 2: Deleting a Table with "ALL"

This example emphasizes the use of the DROP TABLE command with the "ALL" parameter to delete the "test" table, including both its schema and underlying data. After using DROP TABLE with "ALL," the table is entirely removed. It also demonstrates how to recover the previously dropped "test" table using the UNDROP TABLE command. However, since the table's data was deleted, the subsequent SELECT statement shows an empty result.

```sql
CREATE TABLE test(a INT, b VARCHAR);
INSERT INTO test (a, b) VALUES (1, 'example');
SELECT * FROM test;

a|b      |
-+-------+
1|example|

-- Delete the table with the ALL parameter
DROP TABLE test ALL;

-- Recover the table
UNDROP TABLE test;
SELECT * FROM test;

a|b|
-+-+

DESC test;

Field|Type   |Null|Default|Extra|
-----+-------+----+-------+-----+
a    |INT    |YES |NULL   |     |
b    |VARCHAR|YES |NULL   |     |
```