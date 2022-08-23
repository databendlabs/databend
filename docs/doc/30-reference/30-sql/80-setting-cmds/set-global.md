---
title: SET
---

Changes the value of a session-level or global-level setting. To show all the current settings, use [SHOW SETTINGS](show-settings.md). 

## Syntax

```sql
SET [GLOBAL] <setting_name> = <new_value>;
```

`GLOBAL`: Include this option when you change the value of a global-level setting. If you include this option preceding a session-level setting, the setting will become a global-level setting.

## Examples

The following example sets the `skip_header` setting to 1:

```sql
SET skip_header = 1;
```

The following example sets the `skip_header` setting to 1 and changes it to be a global-level setting:

```sql
SET GLOBAL skip_header = 1;
```