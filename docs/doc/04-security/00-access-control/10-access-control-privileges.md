---
title: Access Control Privileges
sidebar_label: Privileges
description:
  Databend Access Control Privileges
---

This topic describes the privileges that are available in the Databend access control model.

## All Privileges

| Privilege | Object Type | Description |
| :--                 | :--                  | :--                  |
| ALL   |  All    | Grants all the privileges for the specified object type. |
| Alter   |    Database, Table, View   | Privilege to alter databases or tables. |
| Create   |     Database, Table    | Privilege to create databases or tables. |
| Delete   |  Table   | Privilege to delete or truncate rows in a table. |
| Drop       |    Database, Table, View    | Privilege to drop databases or tables or views and undrop databases or tables. |
| Insert       |   Table       | Privilege to insert rows into tables. |
| Select       |    Table      | Privilege to select rows from tables . |
| Update       |      Table    | Privilege to update rows in a table |
| Grant       |     Global    | Privilege to Grant/Revoke privileges to users or roles |
| Super       |      Global   | Privilege to Kill query, Set global configs, OptimizeTable. |
| Usage       |    Global     | UsagePrivilege is a synonym for “no privileges” |
| Create Role   |      Global    | Not used |
| Create Stage   |    Not used     | Not used |
| Create User   |     Global     | Not used |

## Global Privileges

| Privilege | Usage |
| :--                 | :--                  |
| ALL   |  Grants all the privileges for the specified object type. |
| Grant   |    Add/Drop table Column, Alter table cluster key, Re-cluster table |
| CreateRole   |     Create table    |
| CreateUser   |  Delete rows in a table, Truncate table |
| Super       |    Kill query, Set configs |
| Usage       |   Only can connect to databend query, but no privileges |


## Table Privileges

| Privilege | Usage |
| :--                 | :--                  |
| ALL   |  Grants all the privileges for the specified object type. |
| Alter   |    Add/Drop table Column, Alter table cluster key, Re-cluster table |
| Create   |     Create table    |
| Delete   |  Delete rows in a table, Truncate table |
| Drop       |    Drop table, Undrop table(restores the recent version of a dropped table) |
| Insert       |   Insert rows into table |
| Select       |    Select rows from tables |
| Update       |      Update rows in a table |
| Super       |    Optimize table need super privilege |

## View Privileges

| Privilege | Usage |
| :--                 | :--                  |
| ALL   |  Grants all the privileges for the specified object type |
| Alter   |    Create/Drop view, Alter the existing view by using another `QUERY` |
| Drop       |    Drop view |

## Database Privileges

| Privilege | Usage |
| :--                 | :--                  |
| Alter   |    Rename database |
| Create   |     Create database    |
| Drop       |    Drop database, Undrop database((restores the recent version of a dropped database)) |


## Session Policy Privileges


| Privilege | Usage |
| :--                 | :--                  |
| Super       |    Kill query, Set configs |
| ALL   |  Grants all the privileges for the specified object type. |
