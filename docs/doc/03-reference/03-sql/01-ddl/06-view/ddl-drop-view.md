---
title: DROP VIEW
---

Drop the view.

## Syntax

```sql
DROP VIEW [IF EXISTS] [db.]view_name
```

## Examples

```sql
mysql> DROP VIEW IF EXISTS tmp_view;

mysql> select * from tmp_view;
ERROR 1105 (HY000): Code: 1025, displayText = Unknown table 'tmp_view'.
```
