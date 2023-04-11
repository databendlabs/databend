---
title: Managing Settings
sidebar_label: Managing Settings
description:
  Managing Databend settings
---

Databend provides a variety of system settings that enable you to control how Databend works. For example, you can set a timezone in which Databend works and the SQL dialect you prefer. 

System settings in Databend are categorized into two levels: **Session** and **Global**. A session-level setting applies to the current node only, while a global-level setting affects the entire cluster. A session-level setting can be converted to a global-level one, and it's also possible the other way around.

All the settings come with default values out of the box. To show the available system settings and their default values, see [SHOW SETTINGS](../14-sql-commands/40-show/show-settings.md). To update a setting, use the [SET](../14-sql-commands/80-setting-cmds/01-set-global.md) or [UNSET](../14-sql-commands/80-setting-cmds/02-unset.md) command.

After deploying Databend, it is a good idea to go through all the system settings, and tune up the levels and values before working with Databend, so that Databend can work better for you.

Please note that some Databend behaviors cannot be changed through the system settings; you must take them into consideration while working with Databend. For example, 

- Databend encodes strings to the UTF-8 charset.
- Databend uses a 1-based numbering convention for arrays.