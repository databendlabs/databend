---
title: MySQL-Compatible Clients
sidebar_label: MySQL-Compatible Clients
description:
  Connect Databend from MySQL-compatible clients.
---

Databend provides support for MySQL compatible CLI and GUI clients where you can connect to Databend and execute queries from.

:::note
Databend is not a complete implementation of the MySQL protocol, so certain programs such as some ORM frameworks may not be able to connect to Databend.
:::

To connect to Databend with MySQL compatible clients, you will need to connect to port 3307 with a SQL user created in Databend. 

This example connects to a remote Databend with a SQL user named `databend`:

```shell
mysql -h172.20.0.2 -udatabend -P3307 -pdatabend
```

This example connects to a local Databend with the user `root`:

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

**Related video:**

<iframe width="853" height="505" className="iframe-video" src="https://www.youtube.com/embed/3cFmGvtU-ws" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

## Built-in Accounts

There are four built-in accounts in Databend that can be accessed from a local machine without requiring a password. These accounts are immutable and possess the highest level of privileges, meaning their settings cannot be altered or removed. Additionally, it is not possible to change or update their passwords.

- `root@127.0.0.1`
- `root@localhost`
- `default@127.0.0.1`
- `default@localhost`

## Example: Connect from DBeaver

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
