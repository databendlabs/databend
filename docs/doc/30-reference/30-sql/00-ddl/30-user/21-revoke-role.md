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
```sql title='mysql>'
create user user1 identified by 'abc123';
```

Grant the `ALL` privilege on all existing tables in the `default` database to the user `user1`:
 
```sql title='mysql>'
grant all on default.* to user1;
```
```sql title='mysql>'
show grants for user1;
```
```
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
+-----------------------------------------+
```

### Grant Privileges to a Role

Grant the `SELECT` privilege on all existing tables in the `mydb` database to the role `role1`:

Create role:
```sql tile='mysql>'
create role role1;
```

Grant privileges to the role:
```sql title='mysql>'
grant select on mydb.* to role role1;
```

Show the grants for the role:
```sql ext title='mysql>'
show grants for role role1;
```

```text
+-------------------------------------+
| Grants                              |
+-------------------------------------+
| GRANT SELECT ON 'mydb'.* TO 'role1' |
+-------------------------------------+
```

### Grant a Role to a User

User `user1` grants are:
```sql title='mysql>'
show grants for user1;
```
```
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
+-----------------------------------------+
```

Role `role1` grants are:
```sql title='mysql>'
show grants for role role1;
```

```text
+-------------------------------------+
| Grants                              |
+-------------------------------------+
| GRANT SELECT ON 'mydb'.* TO 'role1' |
+-------------------------------------+
```

Grant role `role1` to user `user1`:
```sql title='mysql>'
 grant role role1 to user1;
```

Now, user `user1` grants are:
```sql title='mysql>'
show grants for user1;
```

```
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
| GRANT SELECT ON 'mydb'.* TO 'role1'     |
+-----------------------------------------+
```

### Revoke Role From a User

```sql title='mysql>'
revoke role role1 from user user1;
```

```sql title='mysql>'
show grants for user1;
```

```
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
+-----------------------------------------+
```

