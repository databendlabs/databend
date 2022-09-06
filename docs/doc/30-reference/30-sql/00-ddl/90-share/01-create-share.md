---
title: CREATE SHARE
---

Creates a share.

For more information about how to work with shares in Databend Cloud, see [SHARE](index.md).

## Syntax

```sql
CREATE SHARE [IF NOT EXISTS] <share_name> [ COMMENT = '<string_literal>' ];
```

## Examples

The following example creates a share named `myshare`:

```sql
CREATE SHARE myshare;
```