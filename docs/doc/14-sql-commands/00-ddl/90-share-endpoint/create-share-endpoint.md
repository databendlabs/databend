---
title: CREATE SHARE ENDPOINT
---

Creates a share endpoint.

## Syntax

```sql
CREATE SHARE ENDPOINT [IF NOT EXISTS] <share_endpoint_name>
    URL = '<share_endpoint_url>'
    TENANT = <shared_tenant_name>
    [COMMENT = <comment_string>]
```

## Examples

The following example creates a share endpoint named "to_share" that defines the URL to access the shared Databend tenant "toronto":

```sql
CREATE SHARE ENDPOINT to_share URL = 'http://127.0.0.1:23003' TENANT = toronto;
```