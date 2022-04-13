---
title: CREATE USER
description: Create a new user.
---

The CREATE USER statement creates new Databend accounts. It enables authentication, resource-limit, password-management, for new accounts. 

**See also:**
 - [GRANT PRIVILEGES TO USER](./10-grant-privileges.md)
 - [GRANT ROLE TO USER](./20-grant-role.md)

## Syntax

```sql
CREATE USER <name> IDENTIFIED [WITH auth_type ] BY 'password_string'
```

**Where:**

```
auth_type: {
    double_sha1_password
  | sha256_password
}
```
auth_type default is **double_sha1_password**.

:::tip

In order to make MySQL client/drivers existing tools easy to connect to Databend, we support two authentication plugins which is same as MySQL server did:
* double_sha1_password
   * mysql_native_password is one of MySQL authentication plugin(long time ago), this plugin uses double_sha1_password to store the password(SHA1(SHA1(password)).
    
* sha256_password
  * caching_sha2_password is a new default authentication plugin starting with MySQL-8.0.4, it uses sha256 to transform the password.

More of the MySQL authentication plugin, please see [A Tale of Two Password Authentication Plugins](https://dev.mysql.com/blog-archive/a-tale-of-two-password-authentication-plugins/).
:::

## Examples

### Create Default auth_type User

```sql title='mysql>'
create user user1 identified by 'abc123';
```

```sql title='mysql>'
show users;
```

```sql
+-----------+----------+----------------------+------------------------------------------+
| name      | hostname | auth_type            | auth_string                              |
+-----------+----------+----------------------+------------------------------------------+
| user1     | %        | double_sha1_password | 6691484ea6b50ddde1926a220da01fa9e575c18a |
+-----------+----------+----------------------+------------------------------------------+

```

### Create a `sha256_password` auth_type User

```sql title='mysql>'
create user user1 identified with sha256_password BY 'abc123';
```

```sql title='mysql>'
show users;
```

```sql
+-----------+----------+----------------------+------------------------------------------------------------------+
| name      | hostname | auth_type            | auth_string                                                      |
+-----------+----------+----------------------+------------------------------------------------------------------+
| user1     | %        | sha256_password      | 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 |
+-----------+----------+----------------------+------------------------------------------------------------------+
```

### Grant Privileges to the User

```text title='mysql>'
grant all on *.* to user1;
```

```text
show grants for user1;
```

```text
+---------------------------------+
| Grants                          |
+---------------------------------+
| GRANT ALL ON *.* TO 'user1'@'%' |
+---------------------------------+
```