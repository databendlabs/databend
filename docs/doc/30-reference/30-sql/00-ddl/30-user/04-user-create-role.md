---
title: CREATE ROLE
description: Create a new role.
---

Create a new role.

After creating roles, you can grant object privileges to the role, enable access control security for objects in the system.

**See also:**
 - [GRANT PRIVILEGES TO ROLE](./10-grant-privileges.md)
 - [GRANT ROLE TO USER](./20-grant-role.md)

## Syntax

```sql
CREATE ROLE <role_name> [ COMMENT = '<string_literal>' ]
```
## Examples

```sql title='mysql>'
create role role1;
```