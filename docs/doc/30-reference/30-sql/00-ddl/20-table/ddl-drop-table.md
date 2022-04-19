---
title: DROP TABLE
---

Deletes the table.

## Syntax

```sql
DROP TABLE [IF EXISTS] [db.]name
```

## Examples

```sql
CREATE TABLE test(a UInt64, b Varchar) Engine = Memory;
DROP TABLE test;
```
