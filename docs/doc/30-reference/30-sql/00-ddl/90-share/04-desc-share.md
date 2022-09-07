---
title: DESC SHARE
---

Lists the shared objects in a share.

## Syntax

```sql
{ DESC | DESCRIBE } SHARE <share_name>;
```

## Examples

The following example lists the shared objects in the share `myshare`:

```sql
DESC SHARE myshare;

---
+----------+--------------------------------------+-------------------------------+
| Kind     | Name                                 | Shared_on                     |
|----------+--------------------------------------+-------------------------------|
| DATABASE | tenant1.db1                          | 2022-08-11 18:04:17.642 -0700 |
| TABLE    | tenant1.db1.table1                   | 2022-08-11 18:04:17.749 -0700 |
+----------+--------------------------------------+-------------------------------+
```