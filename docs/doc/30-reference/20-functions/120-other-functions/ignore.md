---
title: ignore
---

By using insert ignore statement, the rows with invalid data that cause the error are ignored and the rows with valid data are inserted into the table.

## Syntax

```sql
insert ignore into table(column_list)
values( value_list),
      ( value_list),
      ...
```
