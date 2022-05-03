---
title: current_user
---

Returns the user name and host name combination for the account that the server used to authenticate the current client. This account determines your access privileges. The return value is a string in the utf8 character set.

## Syntax

```
SELECT current_user()
```

## Examples

```sql
SELECT current_user();
+--------------------+
| current_user()     |
+--------------------+
| 'root'@'127.0.0.1' |
+--------------------+
```
