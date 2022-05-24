---
title: 'GRANT <privileges> to a User or Role'
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
```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant the `ALL` privilege on all existing tables in the `default` database to the user `user1`:

```sql
GRANT ALL ON default.* TO user1;
```

```sql
SHOW GRANTS FOR user1;
+-----------------------------------------+
| Grants                                  |
+-----------------------------------------+
| GRANT ALL ON 'default'.* TO 'user1'@'%' |
+-----------------------------------------+
```

Grant the `ALL` privilege to all the database to the user `user1`:

```sql
GRANT ALL ON *.* TO 'user1';
```
```sql
SHOW GRANTS FOR user1;
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
```sql 
CREATE ROLE role1;
```

Grant privileges to the role:
```sql
GRANT SELECT ON mydb.* TO ROLE role1;
```

Show the grants for the role:
```sql
SHOW GRANTS FOR ROLE role1;
+-------------------------------------+
| Grants                              |
+-------------------------------------+
| GRANT SELECT ON 'mydb'.* TO 'role1' |
+-------------------------------------+
```