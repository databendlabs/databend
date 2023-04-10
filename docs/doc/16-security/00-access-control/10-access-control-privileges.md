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
| ALTER   |   Global, Database, Table, View   | Privilege to alter databases or tables, Alter user/UDF. |
| CREATE   |     Global, Database, Table    | Privilege to create databases or tables or udf. |
| DELETE   |  Table   | Privilege to delete or truncate rows in a table. |
| DROP       |    Global, Database, Table, View    | Privilege to drop databases or tables or views and undrop databases or tables, Drop UDF. |
| INSERT       |   Table       | Privilege to insert rows into tables. |
| SELECT       |    Database, Table      | Privilege to select rows from tables, show or use databases. |
| UPDATE       |      Table    | Privilege to update rows in a table |
| GRANT       |     Global    | Privilege to Grant/Revoke privileges to users or roles |
| SUPER       |      Global, Table   | Privilege to Kill query, Set global configs, Optimize table, Analyze table, Operator stage/catalog/share. |
| USAGE       |    Global     | UsagePrivilege is a synonym for “no privileges” |
| CREATE ROLE   |      Global    | Privilege to create a role |
| DROP ROLE   |      Global    | Privilege to drop a role |
| CREATE USER   |     Global     | Privilege to create a sql user |
| CREATE USER   |     Global     | Privilege to drop a sql user |

## Global Privileges

| Privilege | Usage |
| :--                 | :--                  |
| ALL   |  Grants all the privileges for the specified object type. |
| GRANT   |    Add/Drop table Column, Alter table cluster key, Re-cluster table |
| CREATEROLE   |     Create a new role.    |
| DROPUSER   |  Drop a new user. |
| CREATEUSER   |  Create a new user. |
| DROPROLE   |  Drop a new role. |
| SUPER       |    Kill query, Set/Unset settings, operator stage/catalog/share, Call function, Copy into stage |
| USAGE       |   Only can connect to databend query, but no privileges |
| CREATE       |   Create UDF |
| DROP       |   Drop UDF |
| ALTER       |   Alter UDF, ALter sql user |


## Table Privileges

| Privilege | Usage |
| :--                 | :--                  |
| ALL   |  Grants all the privileges for the specified object type. |
| ALTER   |    Add/Drop table Column, Alter table cluster key, Re-cluster table, Revert table |
| CREATE   |     Create table    |
| DELETE   |  Delete rows in a table, Truncate table |
| DROP       |    Drop table, Undrop table(restores the recent version of a dropped table) |
| INSERT       |   Insert rows into table, Copy into table |
| SELECT       |    Select rows from tables, Show create table, Describe table |
| UPDATE       |      Update rows in a table |
| SUPER       |    Optimize/Analyze table need super privilege |

## View Privileges

| Privilege | Usage |
| :--                 | :--                  |
| ALL   |  Grants all the privileges for the specified object type |
| ALTER   |    Create/Drop view, Alter the existing view by using another `QUERY` |
| DROP       |    Drop view |

## Database Privileges

| Privilege | Usage |
| :--                 | :--                  |
| Alter   |    Rename database |
| CREATE   |     Create database    |
| DROP       |    Drop database, Undrop database(restores the recent version of a dropped database) |
| SELECT       |    Show create database, Use database, |


## Session Policy Privileges

| Privilege | Usage |
| :--                 | :--                  |
| SUPER       |    Kill query, Set/Unset settings |
| ALL   |  Grants all the privileges for the specified object type. |

## Stage Privileges

| Privilege | Usage |
| :--                 | :--                  |
| SUPER       |  List Stage, Create Stage, Drop Stage, Remove Stage |
| ALL   |  Grants all the privileges for the specified object type. |

## Catalog Privileges

| Privilege | Usage |
| :--                 | :--                  |
| SUPER       |  Show create catalog, Create catalog, Drop catalog |
| ALL   |  Grants all the privileges for the specified object type. |

## Catalog Privileges

| Privilege | Usage |
| :--                 | :--                  |
| SUPER       |  Create share, Drop share, Desc share, Show shares |
| ALL   |  Grants all the privileges for the specified object type. |
