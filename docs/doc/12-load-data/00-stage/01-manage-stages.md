---
title: Managing Stages
---

This topic only covers the available tools, APIs, and commands for managing stages. It does not provide detailed syntax or examples. If you need more information, please refer to the relevant pages linked throughout this topic.

There are a variety of commands available in Databend to help you manage stages:

- [CREATE STAGE](../../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md): Creates a stage. 
- [DROP STAGE](../../14-sql-commands/00-ddl/40-stage/02-ddl-drop-stage.md): Removes a stage.
- [DESC STAGE](../../14-sql-commands/00-ddl/40-stage/03-ddl-desc-stage.md): Shows the properties of a stage.
- [SHOW STAGES](../../14-sql-commands/00-ddl/40-stage/06-ddl-show-stages.md): Returns a list of the created stages.

In addition, these commands can help you manage staged files in a stage, such as listing or removing them. 

- [LIST FILES](../../14-sql-commands/00-ddl/40-stage/04-ddl-list-stage.md): Returns a list of the staged files in a stage.
- [REMOVE FILES](../../14-sql-commands/00-ddl/40-stage/05-ddl-remove-stage.md): Removes staged files from a stage.

Please note that some of the commands above do not apply to the user stage. See the table below for details:

| Stage          | CREATE STAGE | DROP STAGE | DESC STAGE | LIST FILES | REMOVE FILES | SHOW STAGES |
|----------------|--------------|------------|------------|------------|--------------|-------------|
| User Stage     | No           | No         | Yes        | Yes        | Yes          | No          |
| Internal Stage | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |
| External Stage | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |