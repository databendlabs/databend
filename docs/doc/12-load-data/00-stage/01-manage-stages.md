---
title: Managing Stages
---

This topic only covers the available tools, APIs, and commands for managing stages and staged files. It does not provide detailed syntax or examples. If you need more information, please refer to the relevant pages linked throughout this topic.

There are a variety of commands available in Databend to help you manage stages:

- [CREATE STAGE](../../14-sql-commands/00-ddl/40-stage/01-ddl-create-stage.md): Creates a stage. 
- [DROP STAGE](../../14-sql-commands/00-ddl/40-stage/02-ddl-drop-stage.md): Removes a stage.
- [DESC STAGE](../../14-sql-commands/00-ddl/40-stage/03-ddl-desc-stage.md): Shows the properties of a stage.
- [SHOW STAGES](../../14-sql-commands/00-ddl/40-stage/06-ddl-show-stages.md): Returns a list of the created stages.

You can use the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md) to stage a file by uploading a local file to a stage. This API can be called using curl or other HTTP client tools. Alternatively, you can also upload files directly to the folder in your bucket that maps to a stage using a web browser. Once uploaded, Databend can recognize them as staged files. 

In addition, these commands can help you manage staged files in a stage, such as listing or removing them. 

- [LIST FILES](../../14-sql-commands/00-ddl/40-stage/04-ddl-list-stage.md): Returns a list of the staged files in a stage.
- [REMOVE FILES](../../14-sql-commands/00-ddl/40-stage/05-ddl-remove-stage.md): Removes staged files from a stage.

Please note that some of the commands above do not apply to the user stage. See the table below for details:

| Stage          | CREATE STAGE | DROP STAGE | DESC STAGE | LIST FILES | REMOVE FILES | SHOW STAGES |
|----------------|--------------|------------|------------|------------|--------------|-------------|
| User Stage     | No           | No         | Yes        | Yes        | Yes          | No          |
| Internal Stage | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |
| External Stage | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |