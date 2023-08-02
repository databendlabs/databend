---
title: CREATE USER
description: Create a new user.
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.30"/>

Create a new user account in Databend, specifying the user's name, authentication type, password, and [network policy](../101-network-policy/index.md). 

See also:
 - [CREATE NETWORK POLICY](../101-network-policy/ddl-create-policy.md)
 - [GRANT PRIVILEGES TO USER](./10-grant-privileges.md)
 - [GRANT ROLE TO USER](./20-grant-role.md)

## Syntax

```sql
CREATE USER <name> IDENTIFIED [WITH auth_type ] BY '<password>' [WITH SET NETWORK POLICY='<network_policy>']
```

*auth_type* can be `double_sha1_password` (default), `sha256_password` or `no_password`.

:::tip

In order to make MySQL client/drivers existing tools easy to connect to Databend, we support two authentication plugins which is same as MySQL server did:
* double_sha1_password
   * mysql_native_password is one of MySQL authentication plugin(long time ago), this plugin uses double_sha1_password to store the password(SHA1(SHA1(password)).
    
* sha256_password
  * caching_sha2_password is a new default authentication plugin starting with MySQL-8.0.4, it uses sha256 to transform the password.

For more information about MySQL authentication plugins, see [A Tale of Two Password Authentication Plugins](https://dev.mysql.com/blog-archive/a-tale-of-two-password-authentication-plugins/).
:::

## Examples

### Creating User with Default auth_type

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';

SHOW USERS;
+-----------+----------+----------------------+------------------------------------------+---------------+
| name      | hostname | auth_type            | auth_string                              | is_configured |
+-----------+----------+----------------------+------------------------------------------+---------------+
| user1     | %        | double_sha1_password | 6691484ea6b50ddde1926a220da01fa9e575c18a | NO            |
+-----------+----------+----------------------+------------------------------------------+---------------+
```

### Creating User with sha256_password auth_type

```sql
CREATE USER user1 IDENTIFIED WITH sha256_password BY 'abc123';

SHOW USERS;
+-----------+----------+----------------------+------------------------------------------------------------------+---------------+
| name      | hostname | auth_type            | auth_string                                                      | is_configured |
+-----------+----------+----------------------+------------------------------------------------------------------+---------------+
| user1     | %        | sha256_password      | 6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090 | NO            |
+-----------+----------+----------------------+------------------------------------------------------------------+---------------+
```

### Creating User with Network Policy

```sql
CREATE USER user1 IDENTIFIED BY 'abc123' WITH SET NETWORK POLICY='test_policy';

SHOW USERS;
+-----------+----------+----------------------+------------------------------------------+---------------+
| name      | hostname | auth_type            | auth_string                              | is_configured |
+-----------+----------+----------------------+------------------------------------------+---------------+
| user1     | %        | double_sha1_password | 6691484ea6b50ddde1926a220da01fa9e575c18a | NO            |
+-----------+----------+----------------------+------------------------------------------+---------------+
```