---
title: SET
---

Changes the value of a session-level or cluster-level setting. To show all the current settings, use [SHOW SETTINGS](show-settings.md). 

## Syntax

```sql
SET [GLOBAL] <setting_name> = <new_value>;
```

`GLOBAL`: Include this option when you change the value of a global-level setting. If you include this option preceding a session-level setting, the setting will become a cluster-level(or global-level) setting.

:::note
A cluster-level setting is a cluster setting and the value will be stored in the meta service.
:::

## Examples

The following example sets the `max_threads` setting to `4`:

```sql
SET max_threads = 4;
```

The following example sets the `max_threads` setting to `4` and changes it to be a cluster-level setting:

```sql
SET GLOBAL max_threads = 4;
```
