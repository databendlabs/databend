---
title: 'GRANT <privileges> To a User or Role'
sidebar_label: 'GRANT <privileges>'
description: Grants one or more access privileges to a user or a role.
---

Grants one or more access privileges to a user or role. The privileges that can be granted are grouped into the following categories:
* Privileges for schema objects (databases, tables, views, stages, UDFs)

## Syntax

```sql
GRANT { 
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

### Grant Privileges to a User

Create a user:
```sql title='mysql>'
create user user1 identified by 'abc123';
```

Grant the `ALL` privilege on all existing tables in the `default` database to the user `user1`:
 
```sql title='mysql>'
grant all on default.* to user1;
```
```sql title='mysql>'
show grants for user1;
```
```
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
+-----------------------------------------+
```

Grant the `ALL` privilege to all the database to the user `user1`:

```sql title='mysql>'
grant all on *.* to 'user1';
```
```sql title='mysql>'
show grants for user1;
```
```
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
| GRANT ALL ON *.* TO 'user1'@'%'         |
+-----------------------------------------+
```

### Grant Privileges to a Role

Grant the `SELECT` privilege on all existing tables in the `mydb` database to the role `role1`:

Create role:
```sql tile='mysql>'
create role role1;
```

Grant privileges to the role:
```sql title='mysql>'
grant SELECT on mydb.* to role role1;
```

Show the grants for the role:
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