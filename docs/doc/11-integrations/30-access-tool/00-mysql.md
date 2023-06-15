---
title: MySQL-Compatible Clients
sidebar_label: MySQL-Compatible Clients
description:
  Connect Databend from MySQL-compatible clients.
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.57"/>

Databend provides support for MySQL compatible CLI and GUI clients where you can connect to Databend and execute queries from.

:::note
Databend is not a complete implementation of the MySQL protocol, so certain programs such as some ORM frameworks may not be able to connect to Databend.
:::

To connect to Databend with MySQL compatible clients, you will need to connect to port 3307 with a SQL user created in Databend. 

This example connects to a remote Databend with a SQL user named "databend":

```shell
mysql -h172.20.0.2 -udatabend -P3307 -pdatabend
```

This example connects to a local Databend with the root user:

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

**Related video:**

<iframe width="853" height="505" className="iframe-video" src="https://www.youtube.com/embed/3cFmGvtU-ws" title="YouTube video player" frameBorder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowFullScreen></iframe>

## Built-in Accounts

Databend comes with four preconfigured root users that can be accessed from a local machine without requiring a password:

- `root@127.0.0.1`
- `root@localhost`
- `default@127.0.0.1`
- `default@localhost`

For safety purposes, you can set or update passwords for these accounts with the [ALTER USER](../../14-sql-commands/00-ddl/30-user/03-user-alter-user.md) command. See [Example 1: Connect from MySQL Workbench](#example-1-connect-from-mysql-workbench).

## Example 1: Connect from MySQL Workbench

This example illustrates the process of connecting to a local Databend with MySQL Workbench and setting a password for the root user. Initially, the root user, represented as `root@127.0.0.1`, connects to Databend without a password. However, once the password is set to "databend", the root user must provide this password for authentication when connecting to Databend again.

```shell
eric@ericdeMacBook ~ % mysql -h127.0.0.1 -P3307 -uroot
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 28
Server version: 8.0.26-v1.1.57-nightly-53966cba22dd8a63f21e7e77b306da40e276c843(rust-1.70.0-nightly-2023-06-12T16:14:02.668881000Z) 0

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> alter user root@'127.0.0.1' identified by 'databend';
Query OK, 0 rows affected (0.01 sec)

mysql> quit
Bye
eric@ericdeMacBook ~ % mysql -h127.0.0.1 -P3307 -uroot
ERROR 1698 (28000): Authenticate failed, user: "root", auth_plugin: "mysql_native_password"
eric@ericdeMacBook ~ % mysql -h127.0.0.1 -P3307 -uroot -pdatabend
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 30
Server version: 8.0.26-v1.1.57-nightly-53966cba22dd8a63f21e7e77b306da40e276c843(rust-1.70.0-nightly-2023-06-12T16:14:02.668881000Z) 0

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.
```

## Example 2: Connect from DBeaver

DBeaver is a universal database management tool for everyone who needs to work with data in a professional way. With DBeaver you are able to manipulate with your data like in a regular spreadsheet, create analytical reports based on records from different data storages, export information in an appropriate format.

The following steps show how to establish a connection to Databend with DBeaver.

1. Create a SQL user in Databend.

```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
GRANT ALL ON *.* TO user1;
```

2. In DBeaver, choose a driver for the new connection. Select `MySQL`, then click **Next**.

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/ingegration-dbeaver-connection-1.png" width="500"/>
</p>

3. On the **Connection Settings** screen, configure your connection settings, including:
  * Host
  * Port
  * Database name
  * Username and password

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/ingegration-dbeaver-connection-2.png" width="500"/>
</p>

4. Click **Test Connection...** to see if the connection works.