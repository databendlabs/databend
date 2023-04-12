---
title: Managing Settings
sidebar_label: Managing Settings
description:
  Managing Databend settings
---

Databend provides a variety of system settings that enable you to control how Databend works. For example, you can set a timezone in which Databend works and the SQL dialect you prefer. 

In Databend, the system settings are divided into two levels: **Session** and **Global**. Session-level settings only apply to the current session, while global-level settings affect all the clusters of a tenant. It is possible to convert a session-level setting to a global-level setting, and vice versa. However, it's important to note that when session-level and global-level settings are not consistent, the session-level setting takes precedence and overrides the global setting.

All the settings come with default values out of the box. To show the available system settings and their default values, see [SHOW SETTINGS](../14-sql-commands/40-show/show-settings.md). To update a setting, use the [SET](../14-sql-commands/80-setting-cmds/01-set-global.md) or [UNSET](../14-sql-commands/80-setting-cmds/02-unset.md) command.

After deploying Databend, it is a good idea to go through all the system settings, and tune up the levels and values before working with Databend, so that Databend can work better for you.

Please note that some Databend behaviors cannot be changed through the system settings; you must take them into consideration while working with Databend. For example, 

- Databend encodes strings to the UTF-8 charset.
- Databend uses a 1-based numbering convention for arrays.