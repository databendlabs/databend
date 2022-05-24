---
title: DROP VIEW
description: Drop an existing view ---
---

Drop the view.

## Syntax

```sql
DROP VIEW [IF EXISTS] [db.]view_name
```

## Examples

```sql
DROP VIEW IF EXISTS tmp_view;

SELECT * from tmp_view;
ERROR 1105 (HY000): Code: 1025, displayText = Unknown table 'tmp_view'.
```
