---
title: CREATE USER
---

Create a user.

The CREATE USER statement creates new Databend accounts. It enables authentication, resource-limit, password-management, for new accounts. 

## Syntax

```sql
CREATE USER <name> IDENTIFIED [WITH auth_type ] BY 'auth_string'
```

**Where:**

```
auth_type: {
    plaintext_password
  | double_sha1_password
  | sha256_password
}

auth_type default is double_sha1_password
```

## Examples

### Create default auth type user

```sql title='mysql>'
create user 'user-a'@'%' IDENTIFIED BY 'password';
```

```sql title='mysql>'
show users;
```

```sql
+--------+----------+----------------------+------------------------------------------+
| name   | hostname | auth_type            | auth_string                              |
+--------+----------+----------------------+------------------------------------------+
| user-a | %        | double_sha1_password | 2470c0c06dee42fd1618bb99005adca2ec9d1e19 |
+--------+----------+----------------------+------------------------------------------+
```


### Create specified auth type user
```sql title='mysql>'
create user 'user-b'@'localhost' IDENTIFIED WITH sha256_password BY 'password';
```

```sql title='mysql>'
show users;
```

```sql
+--------+-----------+----------------------+------------------------------------------------------------------+
| name   | hostname  | auth_type            | auth_string                                                      |
+--------+-----------+----------------------+------------------------------------------------------------------+
| user-b | localhost | sha256_password      | 5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 |
+--------+-----------+----------------------+------------------------------------------------------------------+
```