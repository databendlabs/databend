---
title: SHOW CREATE TABLE
---

Shows the CREATE TABLE statement that creates the named table.

## Syntax

```
SHOW CREATE TABLE [database.]table_name
```

## Examples

:::note
numbers(N) – A table for test with the single `number` column (UInt64) that contains integers from 0 to N-1.
:::

```sql
mysql> SHOW CREATE TABLE system.numbers;
+---------+--------------------------------------------------------------------+
| Table   | Create Table                                                       |
+---------+--------------------------------------------------------------------+
| numbers | CREATE TABLE `numbers` (
  `number` UInt64,
) ENGINE=SystemNumbers |
+---------+--------------------------------------------------------------------+
```
