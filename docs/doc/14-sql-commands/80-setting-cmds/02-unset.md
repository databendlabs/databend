---
title: UNSET
---

Sets one or more system settings back to their default values. The settings will also be reset to the initial SESSION level if they were set to GLOBAL level. See [Examples](#examples) for how to reset a GLOBAL setting with UNSET. For more information about the setting levels, see [Managing Settings](../../10-deploy/06-manage-settings.md). To show all the current settings, use [SHOW SETTINGS](../40-show/show-settings.md).

See also: [SET](01-set-global.md)

## Syntax

```sql
UNSET <setting_name> | ( <setting_name> [, <setting_name> ...])
```

## Examples

This example assigns new values to some system settings, changes their levels to GLOBAL, then resets them to their defaults:

```sql
---Show default values
SELECT name, value, default, level from system.settings where name in ('sql_dialect', 'timezone');

| name                          | value      | default    | level   |
|-------------------------------|------------|------------|---------|
| sql_dialect                   | PostgreSQL | PostgreSQL | SESSION |
| timezone                      | UTC        | UTC        | SESSION |

---Set new values
SET GLOBAL sql_dialect='MySQL';
SET GLOBAL timezone='Asia/Shanghai';

SELECT name, value, default, level from system.settings where name in ('sql_dialect', 'timezone');

| name                          | value         | default    | level  |
|-------------------------------|---------------|------------|--------|
| sql_dialect                   | MySQL         | PostgreSQL | GLOBAL |
| timezone                      | Asia/Shanghai | UTC        | GLOBAL |

---Reset to default values
UNSET (timezone, sql_dialect);

SELECT name, value, default, level from system.settings where name in ('sql_dialect', 'timezone');

| name                          | value      | default    | level   |
|-------------------------------|------------|------------|---------|
| sql_dialect                   | PostgreSQL | PostgreSQL | SESSION |
| timezone                      | UTC        | UTC        | SESSION |
```