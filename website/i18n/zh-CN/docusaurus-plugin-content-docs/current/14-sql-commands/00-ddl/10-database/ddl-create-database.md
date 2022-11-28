---
title: CREATE DATABASE
---

Create a database.

Please note that creating database from a share is part of the Share feature in Databend Cloud. For more information, see [Share](../90-share/index.md).

## Syntax

```sql
CREATE DATABASE [IF NOT EXISTS] <database_name> [FROM SHARE <share_name>]
```

## Examples

The following example creates a database named `test`:

```sql
CREATE DATABASE test;
```

The following example creates a database named `test` from the share `myshare`:

```sql
CREATE DATABASE test FROM SHARE t;
```