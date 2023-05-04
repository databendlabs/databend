---
title: SET_VAR
---

The SET_VAR is an optimizer hint that can be specified within a single SQL statement to provide finer control over statement execution plans. This allows you to configure settings temporarily (only for the duration of the SQL statement) with a Key=Value pair. For the available settings you can configure with the SET_VAR, see [SHOW SETTINGS](../40-show/show-settings.md).

See also: [SET](01-set-global.md)

## Syntax

```sql
/*+ SET_VAR(key=value) SET_VAR(key=value) ... */
```

- The hint must immediately follow an **INSERT**, **UPDATE**, **DELETE**, **SELECT**, or **REPLACE** keyword that begins the SQL statement.
- A SET_VAR can include only one Key=Value pair, which means you can configure only one setting with one SET_VAR. However, you can use multiple SET_VAR hints to configure multiple settings.
    - If multiple SET_VAR hints containing a same key, the first Key=Value pair will be applied.
    - If a key fails to parse or bind, all hints will be ignored.

## Examples

```sql
root@localhost> SELECT TIMEZONE();

SELECT
  TIMEZONE();

┌────────────┐
│ timezone() │
│   String   │
├────────────┤
│ UTC        │
└────────────┘

1 row in 0.011 sec. Processed 1 rows, 1B (91.23 rows/s, 91B/s)

root@localhost> SELECT /*+SET_VAR(timezone='America/Toronto') */ TIMEZONE();

SELECT
  /*+SET_VAR(timezone='America/Toronto') */
  TIMEZONE();

┌─────────────────┐
│    timezone()   │
│      String     │
├─────────────────┤
│ America/Toronto │
└─────────────────┘

1 row in 0.023 sec. Processed 1 rows, 1B (43.99 rows/s, 43B/s)

root@localhost> SELECT TIMEZONE();

SELECT
  TIMEZONE();

┌────────────┐
│ timezone() │
│   String   │
├────────────┤
│ UTC        │
└────────────┘

1 row in 0.010 sec. Processed 1 rows, 1B (104.34 rows/s, 104B/s)
```