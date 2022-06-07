---
title: UNDROP TABLE
---

Restores the recent version of a dropped table.

**See also:**
- [CREATE TABLE](./10-ddl-create-table.md)
- [DROP TABLE](./20-ddl-drop-table.md)
- [SHOW TABLES](../../40-show/show-tables.md)

## Syntax

```sql
UNDROP TABLE [db.]name
```


:::tip
* If a table with the same name already exists, `UNDROP` will get the error: `ERROR 1105 (HY000): Code: 2308, displayText = Undrop Table 'test' already exists.`
* `UNDROP` relies on the Databend time travel feature, the table can be restored only within a retention period, default is 24 hours.

:::

## Examples

```sql
CREATE TABLE test(a INT, b VARCHAR);

-- insert data
INSERT INTO test VALUES(1, 'a');

-- check
SELECT * FROM test;
+------+------+
| a    | b    |
+------+------+
|    1 | a    |
+------+------+

-- drop table
DROP TABLE test;

SELECT * FROM test;
ERROR 1105 (HY000): Code: 1025, displayText = Unknown table 'test'.

-- un-drop table
UNDROP TABLE test;

-- check
SELECT * FROM test;
+------+------+
| a    | b    |
+------+------+
|    1 | a    |
+------+------+
```
