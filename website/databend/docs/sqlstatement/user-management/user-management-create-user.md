---
title: CREATE USER
---

Create a database.
The CREATE USER statement creates new Databend accounts. It enables authentication, resource-limit, password-management, for new accounts. 

## Syntax

```sql
CREATE USER IDENTIFIED [WITH auth_plugin] BY 'auth_string'

auth_plugin: {
    plaintext_password
  | double_sha1_password
  | sha256_password
}

auth_plugin default is sha256_password
```

## Examples

```sql
mysql> CREATE USER 'user-a'@'%' IDENTIFIED BY 'password';
mysql> CREATE USER 'user-b'@'localhost' IDENTIFIED WITH sha256_password BY 'password';
```
