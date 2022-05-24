---
title: REVOKE ROLE
description: Removes a role from a user.
---

Removes a role from a user.

## Syntax

```sql
REVOKE ROLE <role_name> FROM { USER <user_name> }
```

## Examples

### Grant Privileges to a User


Create a user:
```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant the `ALL` privilege on all existing tables in the `default` database to the user `user1`:

```sql
GRANT ALL ON default.* TO user1;
```
```sql
SHOW GRANTS FOR user1;
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
+-----------------------------------------+
```

### Grant Privileges to a Role

Grant the `SELECT` privilege on all existing tables in the `mydb` database to the role `role1`:

Create role:
```sql
CREATE ROLE role1;
```

Grant privileges to the role:
```sql
GRANT SELECT ON mydb.* TO ROLE role1;
```

Show the grants for the role:
```sql
SHOW GRANTS FOR ROLE role1;
+-------------------------------------+
| Grants                              |
+-------------------------------------+
| GRANT SELECT ON 'mydb'.* TO 'role1' |
+-------------------------------------+
```

### Grant a Role to a User

User `user1` grants are:
```sql
SHOW GRANTS FOR user1;
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
+-----------------------------------------+
```

Role `role1` grants are:
```sql
SHOW GRANTS FOR ROLE role1;
+-------------------------------------+
| Grants                              |
+-------------------------------------+
| GRANT SELECT ON 'mydb'.* TO 'role1' |
+-------------------------------------+
```

Grant role `role1` to user `user1`:
```sql
 GRANT ROLE role1 TO user1;
```

Now, user `user1` grants are:
```sql
SHOW GRANTS FOR user1;
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
| GRANT SELECT ON 'mydb'.* TO 'role1'     |
+-----------------------------------------+
```

### Revoke Role From a User

```sql
REVOKE ROLE role1 FROM USER user1;
```

```sql
SHOW GRANTS FOR user1;
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
+-----------------------------------------+
```
