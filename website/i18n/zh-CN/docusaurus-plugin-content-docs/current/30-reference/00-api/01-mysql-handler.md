---
title: MySQL Handler
sidebar_label: MySQL Handler
description: Databend is MySQL and MariaDB wire protocol-compatible. ---
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/api/api-handler-mysql.png" width="200"/>
</p>

## Overview

Databend is MySQL and MariaDB wire protocol-compatible, it supports similar SQL syntax as MySQL, allow you to connect to Databend server with MySQL/MariaDB client or MySQL connectors(like JDBC), make it easier for users/developers to use Databend.


## Developer

If you are a developer and want to connect to Databend with MySQL JDBC driver, please refer to [develop](/doc/develop/).

## Client

Databend supports MySQL/MariaDB client to connect(Default port is 3307, By `mysql_handler_port` config), it is same as you connect to a MySQL server.

```shell
mysql -h127.0.0.1 -uroot -P3307 
```
