---
title: Stage
sidebar_position: 1
slug: ./
---

A stage stores data files that can be loaded into tables.

## Stage Types

Databend supports the following stage types:

* [User Stage](#usage-stage)
* [Named Internal / External Stage](#named-internal--external-stage)

### User Stage

In Databend, each user comes with a default stage called User Stage. Data files uploaded to a user stage are stored in `/<bucket_name>/<tenant_id>/stage/user/<user_name>`. To reference the user stage, use `@~`. See the examples below:

```sql
-- Lists the staged files in the user stage
LIST @~;

-- Deletes all the files from the user stage
REMOVE @~;
```

The following limitations apply when you work with the user stage:

- The user stage is available out of the box. There is no need to create it before use, and you cannot change or drop it.

- Data files stored in your user stage are not accessible to other users.

- You cannot set format options for the user stage. Instead, you can set them in the [COPY INTO](./../../10-dml/dml-copy-into-table.md) command when you load data.

### Named Internal / External Stage

In addition to the [User Stage](#user-stage), you can create named stages to store data files. Named stages give you more control over the data loading:

- Named stages enable other users with appropriate privileges to access the staged data files and load them into tables.

- A named stage can be internal or external:

    - Internal stages store data files in the storage backend you specify in `databend-query.toml`.
    - When you create an external stage with the command [CREATE STAGE](01-ddl-create-stage.md), you specify the stage location in the command.

## Managing Stages

Use the following commands to manage stages in Databend:

- [CREATE STAGE](01-ddl-create-stage.md): Creates a stage. 
- [DROP STAGE](02-ddl-drop-stage.md): Removes a stage.
- [DESC STAGE](03-ddl-desc-stage.md): Shows the properties of a stage.
- [LIST FILES](04-ddl-list-stage.md): Returns a list of the staged files in a stage.
- [REMOVE FILES](05-ddl-remove-stage.md): Removes staged files from a stage.
- [SHOW STAGES](06-ddl-show-stages.md): Returns a list of the created stages.

Some of the commands above do not apply to the [User Stage](#user-stage). See the table below for details:

| Stage          | CREATE STAGE | DROP STAGE | DESC STAGE | LIST FILES | REMOVE FILES | SHOW STAGES |
|----------------|--------------|------------|------------|------------|--------------|-------------|
| User Stage     | No           | No         | Yes        | Yes        | Yes          | No          |
| Named Internal | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |
| Named External | Yes          | Yes        | Yes        | Yes        | Yes          | Yes         |