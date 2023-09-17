---
title: SHARE
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';

A "Share" in Databend refers to a feature that enables the sharing of various types of database objects, such as tables, views, and user-defined functions (UDFs), across different tenants. 

:::note
Sharing data via a share does not involve physically copying or transferring the data to the recipient tenants. Instead, the shared data remains in a read-only state for the recipient tenants. Users belonging to these tenants can only execute queries on the shared data; they are not allowed to perform updates, inserts, or deletions on the shared data.
:::

## Getting Started with Share

The section describes how to share data via a share and access the shared data across tenants:

### Step 1. Creating a Share

These steps create a share for sharing data with another tenant and grant privileges for the shared data. Perform these steps within the tenant where the data is to be shared.

1. Create an empty share using the [CREATE SHARE](01-create-share.md) command.

```sql
-- Create a share named "myshare"
CREATE SHARE myshare;
```

2. Grant appropriate privileges to the share you created using the [GRANT `<privilege>` to SHARE](06-grant-privilege.md) command. Before granting privileges on the objects you want to share, you must grant privileges for the database that contains the objects.

```sql
-- Grant the USAGE privilege on the database "db1"
GRANT USAGE ON DATABASE db1 TO SHARE myshare;

-- Grant the SELECT privilege on the table "table1" in the database "db1"
GRANT SELECT ON TABLE db1.table1 TO SHARE myshare;
```

3. Add the tenants you want to share with to the share using the [ALTER SHARE](03-alter-share.md) command. When sharing data with another organization, you specify the organization by its tenant ID.

```sql
-- Add tenant B to the share "myshare"
ALTER SHARE myshare ADD TENANTS = B;
```

### Step 2. Accessing Shared Data

These steps create a share endpoint and a database using the share created in [Step 1](#step-1-creating-a-share). Perform these steps within the tenant where you intend to access the shared data.

1. Create a [SHARE ENDPOINT](../90-share-endpoint/index.md).

```sql
-- Create a share endpoint named "to_share"
CREATE SHARE ENDPOINT to_share URL = 'http://<shared-tenant-endpoint>:<port>' TENANT = <shared-tenant-name>;
```

2. Create a database from the share using the [CREATE DATABASE](../10-database/ddl-create-database.md) command.

```sql
-- Create a database named "db2" using the share "myshare"
CREATE DATABASE db2 FROM SHARE myshare;
```

3. To access the shared data, run a SELECT statement like this:

```sql
SELECT * FROM db2.table1 ...
```

## Managing Shares

To manage shares on a tenant, use the following commands:

<IndexOverviewList />