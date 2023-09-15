---
title: SHOW SHARE ENDPOINT
---

Shows all the created share endpoints.

## Syntax

```sql
SHOW SHARE ENDPOINT
```

## Examples

The following example shows all the created share endpoints on the tenant:

```sql
SHOW SHARE ENDPOINT;

| Endpoint | URL                     | To Tenant | Args | Comment | Created On                        |
|----------|-------------------------|-----------|------|---------|-----------------------------------|
| to_share | http://127.0.0.1:23003/ | toronto   | {}   |         | 2023-09-14 03:13:50.014931007 UTC |
```