---
title: Managing Stages
---

This topic only covers the available tools, APIs, and commands for managing stages. It does not provide detailed syntax or examples. If you need more information, please refer to the relevant pages linked throughout this topic.

There are a variety of commands available in Databend to help you manage stages:

- [CREATE STAGE](https://databend.rs/doc/sql-commands/ddl/stage/ddl-create-stage): Creates a stage. 
- [DROP STAGE](https://databend.rs/doc/sql-commands/ddl/stage/ddl-drop-stage): Removes a stage.
- [DESC STAGE](https://databend.rs/doc/sql-commands/ddl/stage/ddl-desc-stage): Shows the properties of a stage.
- [SHOW STAGES](https://databend.rs/doc/sql-commands/ddl/stage/ddl-show-stages): Returns a list of the created stages.

In addition, these commands can help you manage staged files in a stage, such as listing or removing them. For how to upload files to a stage, see [Staging Files with API](https://databend.rs/doc/load-data/stage/stage-files).

- [LIST FILES](https://databend.rs/doc/sql-commands/ddl/stage/ddl-list-stage): Returns a list of the staged files in a stage.
- [REMOVE FILES](https://databend.rs/doc/sql-commands/ddl/stage/ddl-remove-stage): Removes staged files from a stage.

Please note that some of the commands above do not apply to the user stage. See the table below for details:

| Stage          | CREATE STAGE | DROP STAGE | DESC STAGE | LIST FILES | REMOVE FILES | SHOW STAGES |
|----------------|--------------|------------|------------|------------|--------------|-------------|
| User Stage     | No           | No         | Yes        | Yes        | Yes          | No          |
| Internal Stage | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |
| External Stage | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |