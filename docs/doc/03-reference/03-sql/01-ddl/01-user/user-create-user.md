---
title: CREATE USER
---

Create a user.

The CREATE USER statement creates new Databend accounts. It enables authentication, resource-limit, password-management, for new accounts. 

## Syntax

```sql
CREATE USER IDENTIFIED [WITH auth_type ] BY 'auth_string'

auth_type: {
    plaintext_password
  | double_sha1_password
  | sha256_password
}

auth_type default is sha256_password
```

## Examples

```sql
mysql> CREATE USER 'user-a'@'%' IDENTIFIED BY 'password';
mysql> CREATE USER 'user-b'@'localhost' IDENTIFIED WITH sha256_password BY 'password';
```
