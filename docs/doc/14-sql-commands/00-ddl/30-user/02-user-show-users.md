---
title: SHOW USERS
description: Lists all the users in the system
---

Lists all the users in the system.

## Syntax

```sql
SHOW USERS;
```

## Examples

```sql
SHOW USERS;

---
| name                      | hostname | auth_type            | auth_string                              | is_configured |
|---------------------------|----------|----------------------|------------------------------------------|---------------|
| sqluser_johnappleseed     | %        | double_sha1_password | 147dee8f648a745805ee8dda80bb8e277559f55b | NO            |
| johnappleseed@example.com | %        | jwt                  |                                          | NO            |
```