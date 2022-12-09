---
title: FUSE_STATISTIC
---

Returns the estimated number of distinct values of each column in a table.

See Also:

- [OPTIMIZE TABLE](../../14-sql-commands/00-ddl/20-table/60-optimize-table.md)

## Syntax

```sql
FUSE_STATISTIC('<database_name>', '<table_name>')
```

## Examples

You're most likely to use this function together with `ANALYZE TABLE <table_name>` to generate and check the statistical information of a table. For more explanations and examples, see [OPTIMIZE TABLE](../../14-sql-commands/00-ddl/20-table/60-optimize-table.md).