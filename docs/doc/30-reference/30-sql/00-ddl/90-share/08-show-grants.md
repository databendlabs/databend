---
title: SHOW GRANTS
---

Lists the privileges granted on a specified shared object or lists the tenants that are added to a specified share.

For more information about how to work with shares in Databend Cloud, see [SHARE](index.md).

## Syntax

```sql
-- List the privileges granted on a specified shared object
SHOW GRANTS ON { DATABASE <db_name> | TABLE <db_name>.<table_name> }

-- List the tenants that are added to a specified share
SHOW GRANTS OF SHARE <share_name>
```

## Examples

The following example shows the privileges granted the database `default`:

```sql
SHOW GRANTS ON DATABASE default;

---
| Granted_on                        | Privilege | Share_name |
|-----------------------------------|-----------|------------|
| 2022-09-06 18:15:18.204575814 UTC | Usage     | myshare    |
```

The following example shows the tenants that are added to the share `myshare`:

```sql
SHOW GRANTS OF SHARE myshare;

---
| Granted_on                        | Account |
|-----------------------------------|---------|
| 2022-09-06 17:52:57.786357418 UTC | x       |
| 2022-09-06 17:52:57.786357418 UTC | y       |
```