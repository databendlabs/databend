---
title: 'Show Privileges Granted to User or Role'
sidebar_label: 'SHOW GRANTS'
description: Show Privileges Granted to User or Role
---

Lists all the privileges that have been explicitly granted to a user or a role.

## Syntax

```sql
-- Lists privileges granted to a user
SHOW GRANTS FOR <user_name>;

-- Lists privileges granted to a role
SHOW GRANTS FOR ROLE <role_name>;
```

## Examples

```sql
SHOW GRANTS FOR user1;

---
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
+-----------------------------------------+
```

```sql
SHOW GRANTS FOR ROLE role1;

---
+-------------------------------------+
| Grants                              |
+-------------------------------------+
| GRANT SELECT ON 'mydb'.* TO 'role1' |
+-------------------------------------+
```