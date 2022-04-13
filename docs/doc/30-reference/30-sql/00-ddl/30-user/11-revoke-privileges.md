---
title: 'REVOKE <privileges> From a User or Role'
sidebar_label: 'REVOKE <privileges>'
description: Revoke one or more access privileges from a user or a role.
---

Removes one or more privileges on a securable object from a user or role. The privileges that can be granted are grouped into the following categories:
* Privileges for schema objects (databases, tables, views, stages, UDFs)

## Syntax

```sql
REVOKE { 
        schemaObjectPrivileges | ALL [ PRIVILEGES ] ON <privileges_level>
      }
TO [ROLE <role_name>] [<user>]
```

**Where:**

```sql
schemaObjectPrivileges ::=
-- For TABLE
  { SELECT | INSERT }
  
-- For SCHEMA
  { CREATE | DROP | ALTER }
  
-- For USER
  { CREATE USER }
  
-- For ROLE
  { CREATE ROLE}
  
-- For STAGE
  { CREATE STAGE}
```

```sql
privileges_level ::=
    *.*
  | db_name.*
  | db_name.tbl_name
```

## Examples

### Revoke Privileges from a User


Create a user:
```sql title='mysql>'
create user user1 identified by 'abc123';
```

Grant the `SELECT,INSERT` privilege on all existing tables in the `default` database to the user `user1`:
 
```sql title='mysql>'
grant select,insert on default.* to user1;
```
```sql title='mysql>'
show grants for user1;
```
```
+---------------------------------------------------+
| Grants                                            |
+---------------------------------------------------+
| GRANT SELECT,INSERT ON 'default'.* TO 'user1'@'%' |
+---------------------------------------------------+
```

Revoke `INSERT` privilege from user `user1`:
```sql title='mysql>'
revoke insert on default.* from user1;
```

```sql title='mysql>'
show grants for user1;
```
```text
+--------------------------------------------+
| Grants                                     |
+--------------------------------------------+
| GRANT SELECT ON 'default'.* TO 'user1'@'%' |
+--------------------------------------------+
```

### Revoke Privileges from a Role

Grant the `SELECT,INSERT` privilege on all existing tables in the `mydb` database to the role `role1`:

Create role:
```sql tile='mysql>'
create role role1;
```

Grant privileges to the role:
```sql title='mysql>'
grant select,insert on mydb.* to role role1;
```

Show the grants for the role:
```sql title='mysql>'
show grants for role role1;
```

```text
+--------------------------------------------+
| Grants                                     |
+--------------------------------------------+
| GRANT SELECT,INSERT ON 'mydb'.* TO 'role1' |
+--------------------------------------------+
```

Revoke `INSERT` privilege from role `role1`:
```sql title='mysql>'
revoke insert on mydb.* from role role1;
```

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
