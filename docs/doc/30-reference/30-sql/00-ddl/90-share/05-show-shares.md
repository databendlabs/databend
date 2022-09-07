---
title: SHOW SHARES
---

Shows the shares you created and the ones shared with you.

## Syntax

```sql
SHOW SHARES;
```

## Examples

The following example shows that the share `myshare` shares the database `db1` to the tenants `x` and `y`:

```sql
SHOW SHARES;

--The Kind column in the result indicates the data sharing direction:
--OUTBOUND: The share was created by your organization and shared with other organizations.
--INBOUND: The share was created by another organization and shared with your organization.

---
| Created_on                        | Kind     | Name    | Database_name | From      | To  | Comment |
|-----------------------------------|----------|---------|---------------|-----------|-----|---------|
| 2022-09-06 17:46:16.686281294 UTC | OUTBOUND | myshare | default       | tn44grr46 | x,y |         |
```