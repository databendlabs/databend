---
title: SHARE
sidebar_position: 1
slug: ./
---

## What is a Share?

In Databend Cloud, you can create shares to share the following types of database objects across organizations:

- Table
- View
- UDF

When you create a share, you grant appropriate privileges to the other organizations for the data you want to share. Sharing data using a share does not physically copy to transfer the data to the organizations you want to share with. The shared data is READ-ONLY to the organizations. Users from the organizations can only query the shared data. Updating, inserting, and deleting the shared data are not allowed.

The following sections explain how to create and access a share in Databend Cloud:

## Creating a Share

To create a share to share your data across organizations, follow the steps below:

1. Create an empty share using the [CREATE SHARE](01-create-share.md) command. The following example creates a share named `myshare`:

```sql
CREATE SHARE myshare;
```

2. Grant appropriate privileges to the share you created using the [GRANT `<privilege>` to SHARE](06-grant-privilege.md) command. Before granting privileges on the objects you want to share, you must grant privileges for the database that contains the objects. For more information about the available privileges, see [GRANT `<privilege>` to SHARE](06-grant-privilege.md). The following example grants the USAGE privilege to the share `myshare` for the database `db1`, then grants the SELECT privilege for the table `table1` in the database `db1`:

```sql
GRANT USAGE ON DATABASE db1 TO SHARE myshare;
GRANT SELECT ON TABLE db1.table1 TO SHARE myshare;
```

3. Add the organizations you want to share with to the share using the [ALTER SHARE](03-alter-share.md) command. Each organization is assigned a unique tenant ID when they sign up in Databend Cloud. When you share across organizations, you specify organizations by their tenant IDs. The following example adds the tenants `x` and `y` to the share `myshare`:

```sql
ALTER SHARE myshare ADD TENANTS = x, y;
```

## Accessing a Share

Before accessing the data in a share, you need to create a database from the share using the [CREATE DATABASE](../10-database/ddl-create-database.md) command. The following example creates a database named `db2` from the share `myshare`:

```sql
CREATE DATABASE db2 FROM SHARE myshare;
```

To access the shared data, code a SELECT statement like this:

```sql
SELECT * FROM db2.table1 ...
```