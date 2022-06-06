---
title: DROP FUNCTION
description:
  Drop an existing user-defined function.
---

Drop an existing user-defined function.

## Syntax

```sql
DROP FUNCTION [IF EXISTS] <name>
```

## Examples

```sql
DROP FUNCTION a_plus_3;

SELECT a_plus_3(2);
ERROR 1105 (HY000): Code: 2602, displayText = Unknown Function a_plus_3 (while in analyze select projection).
```
