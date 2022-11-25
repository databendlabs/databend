---
title: SET
---

Changes the value of a setting. To show all the current settings, use [SHOW SETTINGS](show-settings.md).

See also: [UNSET](unset.md)

## Syntax

```sql
SET [GLOBAL] <setting_name> = <new_value>;
```

`GLOBAL`: If you include this option preceding a session-level setting, the setting will become a cluster-level (global-level) setting.

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