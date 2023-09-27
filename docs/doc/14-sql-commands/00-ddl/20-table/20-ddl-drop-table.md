---
title: DROP TABLE
---

Deletes the table.

**See also:**
- [CREATE TABLE](./10-ddl-create-table.md)
- [UNDROP TABLE](./21-ddl-undrop-table.md)
- [TRUNCATE TABLE](40-ddl-truncate-table.md)

## Syntax

```sql
DROP TABLE [IF EXISTS] [db.]name
```

:::caution

`DROP TABLE` only remove the table schema from meta service, we do not remove the underlying data from the storage.

:::


## Examples

```sql
CREATE TABLE test(a INT, b VARCHAR);
DROP TABLE test;
```
