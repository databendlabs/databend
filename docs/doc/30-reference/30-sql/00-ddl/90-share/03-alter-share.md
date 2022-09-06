---
title: ALTER SHARE
---

Adds / removes one or more organizations by their tenant IDs to / from a share. 

For more information about how to work with shares in Databend Cloud, see [SHARE](index.md).

## Syntax

```sql
ALTER SHARE [IF EXISTS] <share_name> {ADD | REMOVE} TENANTS = <tenant_id> [, <tenant_id>, ...]
```

## Examples

The following example adds organizations by the tenant IDs `x` and `y` to the share `myshare`:

```sql
ALTER SHARE myshare ADD TENANTS = x, y;
```