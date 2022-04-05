---
title: ALTER USER
---

Modifies the properties for an existing user.

## Syntax

```sql
ALTER USER <name> IDENTIFIED [WITH auth_type ] BY 'auth_string'
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


```sql title='mysql>'
alter user 'user-a' IDENTIFIED WITH sha256_password BY 'password';
```

```sql title='mysql>'
show users;
```

```sql
+--------+----------+-----------------+------------------------------------------------------------------+
| name   | hostname | auth_type       | auth_string                                                      |
+--------+----------+-----------------+------------------------------------------------------------------+
| user-a | %        | sha256_password | 5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 |
+--------+----------+-----------------+------------------------------------------------------------------+
```