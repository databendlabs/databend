---
title: ALTER USER
description: Modifies the properties for an existing user.
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.30"/>

Modifies a user account in Databend, allowing changes to the user's password and authentication type, as well as setting or unsetting a [network policy](../101-network-policy/index.md).

## Syntax

```sql
-- Modify password / authentication type
ALTER USER <name> IDENTIFIED [WITH auth_type ] BY '<password>'

-- Set a network policy
ALTER USER <name> WITH SET NETWORK POLICY='<network_policy>'

-- Unset a network policy
ALTER USER <name> WITH UNSET NETWORK POLICY
```

*auth_type* can be `double_sha1_password` (default), `sha256_password` or `no_password`.

## Examples

### Changing Password & Authentication Type

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';

SHOW USERS;
+-----------+----------+----------------------+------------------------------------------+---------------+
| name      | hostname | auth_type            | auth_string                              | is_configured |
+-----------+----------+----------------------+------------------------------------------+---------------+
| user1     | %        | double_sha1_password | 6691484ea6b50ddde1926a220da01fa9e575c18a | NO            |
+-----------+----------+----------------------+------------------------------------------+---------------+

ALTER USER user1 IDENTIFIED WITH sha256_password BY '123abc';

SHOW USERS;
+-------+----------+-----------------+------------------------------------------------------------------+---------------+
| name  | hostname | auth_type       | auth_string                                                      | is_configured |
+-------+----------+-----------------+------------------------------------------------------------------+---------------+
| user1 | %        | sha256_password | dd130a849d7b29e5541b05d2f7f86a4acd4f1ec598c1c9438783f56bc4f0ff80 | NO            |
+-------+----------+-----------------+------------------------------------------------------------------+---------------+

ALTER USER 'user1' IDENTIFIED WITH no_password;

show users;
+-------+----------+-------------+-------------+---------------+
| name  | hostname | auth_type   | auth_string | is_configured |
+-------+----------+-------------+-------------+---------------+
| user1 | %        | no_password |             | NO            |
+-------+----------+-------------+-------------+---------------+
```

### Setting & Unsetting Network Policy

```sql
SHOW NETWORK POLICIES;

Name        |Allowed Ip List          |Blocked Ip List|Comment    |
------------+-------------------------+---------------+-----------+
test_policy |192.168.10.0,192.168.20.0|               |new comment|
test_policy1|192.168.100.0/24         |               |           |

CREATE USER user1 IDENTIFIED BY 'abc123';

ALTER USER user1 WITH SET NETWORK POLICY='test_policy';

ALTER USER user1 WITH SET NETWORK POLICY='test_policy1';

ALTER USER user1 WITH UNSET NETWORK POLICY;
```