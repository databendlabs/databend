---
title: Memory Engine
---

## Syntax

```sql
CREATE TABLE table_name (
  column_name1 column_type1,
  column_name2 column_type2,
  ...
) ENGINE = Memory;
```

## Use cases

This engine is only for development and testing purposes. It is not recommended to use it in production.

While the Memory Engine provides several advantages, there are also some limitations:

- Limited storage capacity: The amount of data that can be stored is limited by the amount of memory available on the server. This makes the Memory Engine less suitable for large datasets.

- Data loss on server failure: Since all the data is stored in memory, if the server hosting the Databend instance fails, all the data stored in memory will be lost.

