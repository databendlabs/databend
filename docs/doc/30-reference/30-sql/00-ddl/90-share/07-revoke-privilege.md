---
title: REVOKE <privilege> from SHARE
---

Revokes privileges on a database object from a share. 

For more information about how to work with shares in Databend Cloud, see [SHARE](index.md).

## Syntax

```sql
REVOKE { USAGE | SELECT | REFERENCE_USAGE } ON <object_name> FROM SHARE <share_name>;
```

For information about the privileges you can revoke from a share, see [GRANT `<privilege>` to SHARE](06-grant-privilege.md).

## Examples

The following example revokes the SELECT privilege on the table `table1` from the share `myshare`:

```sql
REVOKE SELECT ON db1.table1 FROM SHARE myshare;
```