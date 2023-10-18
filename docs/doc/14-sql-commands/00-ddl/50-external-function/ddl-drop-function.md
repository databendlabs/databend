---
title: DROP FUNCTION
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.116"/>

Drops an external function.

## Syntax

```sql
DROP FUNCTION [IF EXISTS] <function_name>
```

## Examples

```sql
DROP FUNCTION a_plus_3;

SELECT a_plus_3(2);
ERROR 1105 (HY000): Code: 2602, Text = Unknown Function a_plus_3 (while in analyze select projection).
```