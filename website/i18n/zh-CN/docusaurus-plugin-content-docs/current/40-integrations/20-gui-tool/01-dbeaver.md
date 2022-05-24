---
title: Connecting Databend With DBeaver
sidebar_label: DBeaver
description: Connecting Databend with DBeaver. ---
---


<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/integration-gui-dbeaver.png" width="550"/>
</p>

## What is [DBeaver](https://dbeaver.com/)?

DBeaver is a universal database management tool for everyone who needs to work with data in a professional way. With DBeaver you are able to manipulate with your data like in a regular spreadsheet, create analytical reports based on records from different data storages, export information in an appropriate format.

 -- From [About DBeaver](https://dbeaver.com/docs/wiki/)

## DBeaver

### Create a Databend User

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

Create a user:
```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant privileges for the user:
```sql
GRANT ALL ON *.* TO user1;
```

See also [How To Create User](../../30-reference/30-sql/00-ddl/30-user/01-user-create-user.md).

### Install DBeaver

Please refer [DBeaver Installation](https://dbeaver.com/docs/wiki/Installation/)

### Create DBeaver Connection

1. Choose a driver for the new connection: click the `MySQL` type in the gallery, then click Next.
<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/ingegration-dbeaver-connection-1.png" width="500"/>
</p>

2. In the Connection Settings screen, on the General tab, set the primary connection settings:
<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/ingegration-dbeaver-connection-2.png" width="500"/>
</p>

For most drivers required settings include:
* Host
* Port
* Database name
* Username and password

3. Query Data
<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/ingegration-dbeaver-connection-3.png" width="500"/>
</p>
