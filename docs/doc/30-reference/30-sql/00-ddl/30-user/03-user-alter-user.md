---
title: ALTER USER
description: Modifies the properties for an existing user.
---

Modifies the properties for an existing user.

## Syntax

```sql
ALTER USER <name> IDENTIFIED [WITH auth_type ] BY 'auth_string'
```

**Where:**

```
auth_type: {
    double_sha1_password
  | sha256_password
}
```
auth_type default is **double_sha1_password**.

## Examples


```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

```sql
SHOW USERS;
+-----------+----------+----------------------+------------------------------------------+
| name      | hostname | auth_type            | auth_string                              |
+-----------+----------+----------------------+------------------------------------------+
| user1     | %        | double_sha1_password | 6691484ea6b50ddde1926a220da01fa9e575c18a |
+-----------+----------+----------------------+------------------------------------------+
```


```sql
ALTER USER user1 IDENTIFIED WITH sha256_password BY '123abc';
```

```sql
SHOW USERS;
+-------+----------+-----------------+------------------------------------------------------------------+
| name  | hostname | auth_type       | auth_string                                                      |
+-------+----------+-----------------+------------------------------------------------------------------+
| user1 | %        | sha256_password | dd130a849d7b29e5541b05d2f7f86a4acd4f1ec598c1c9438783f56bc4f0ff80 |
+-------+----------+-----------------+------------------------------------------------------------------+
```