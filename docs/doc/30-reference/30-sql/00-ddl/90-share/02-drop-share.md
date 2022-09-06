---
title: DROP SHARE
---

Deletes a share. When you delete a share, the organizations added to the share will no longer have access to the database created from the share.

For more information about how to work with shares in Databend Cloud, see [SHARE](index.md).

## Syntax

```sql
DROP SHARE [IF EXISTS] <share_name>;
```

## Examples

The following example deletes a share named `myshare`:

```sql
DROP SHARE myshare;
```