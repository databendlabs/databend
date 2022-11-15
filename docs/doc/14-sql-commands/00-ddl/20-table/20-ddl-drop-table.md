---
title: DROP TABLE
---

Deletes the table.

**See also:**
- [CREATE TABLE](./10-ddl-create-table.md)
- [UNDROP TABLE](./21-ddl-undrop-table.md)

## Syntax

```sql
DROP TABLE [IF EXISTS] [db.]name
```

:::caution

`DROP TABLE` only remove the table schema from meta service, we do not remove the underlying data from the storage.
If you want to delete the data and table all, please use:

`DROP TABLE <table_name> ALL;`

:::


## Examples

```sql
CREATE TABLE test(a INT, b VARCHAR);
DROP TABLE test;
```
