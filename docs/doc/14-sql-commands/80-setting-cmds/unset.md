---
title: UNSET
---

Sets one or more settings back to their default values. The settings will also be reset to the initial SESSION level if they were set to GLOBAL level. See [Examples](#examples) for how to reset a GLOBAL setting with UNSET.

See also: [SET](set-global.md)

## Syntax

```sql
UNSET <setting_name> | ( <setting_name> [, <setting_name> ...])
```

## Examples

This example assigns new values to some settings, changes their levels to GLOBAL, then resets them to their defaults:

```sql
---Show default values
SELECT name, value, default, level from system.settings where name in ('sql_dialect', 'timezone', 'wait_for_async_insert_timeout');

| name                          | value      | default    | level   |
|-------------------------------|------------|------------|---------|
| sql_dialect                   | PostgreSQL | PostgreSQL | SESSION |
| timezone                      | UTC        | UTC        | SESSION |
| wait_for_async_insert_timeout | 100        | 100        | SESSION |

---Set new values
SET GLOBAL sql_dialect='MySQL';
SET GLOBAL timezone='Asia/Shanghai';
SET GLOBAL wait_for_async_insert_timeout=20000;

SELECT name, value, default, level from system.settings where name in ('sql_dialect', 'timezone', 'wait_for_async_insert_timeout');

| name                          | value         | default    | level  |
|-------------------------------|---------------|------------|--------|
| sql_dialect                   | MySQL         | PostgreSQL | GLOBAL |
| timezone                      | Asia/Shanghai | UTC        | GLOBAL |
| wait_for_async_insert_timeout | 20000         | 100        | GLOBAL |

---Reset to default values
UNSET (timezone, wait_for_async_insert_timeout);
UNSET sql_dialect;

SELECT name, value, default, level from system.settings where name in ('sql_dialect', 'timezone', 'wait_for_async_insert_timeout');

| name                          | value      | default    | level   |
|-------------------------------|------------|------------|---------|
| sql_dialect                   | PostgreSQL | PostgreSQL | SESSION |
| timezone                      | UTC        | UTC        | SESSION |
| wait_for_async_insert_timeout | 100        | 100        | SESSION |
```