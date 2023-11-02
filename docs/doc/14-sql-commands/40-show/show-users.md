---
title: SHOW USERS
---

Shows the list of user accounts.

## Syntax

```sql
SHOW USERS
```

## Examples

```sql
SHOW USERS;
+------+----------+----------------------+---------------+
| name | hostname | auth_type            | is_configured |
+------+----------+----------------------+---------------+
| root | %        | double_sha1_password | YES           |
+------+----------+----------------------+---------------+
| test | %        | double_sha1_password | NO            |
+------+----------+----------------------+---------------+
```
