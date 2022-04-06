---
title: ClickHouse Handler
sidebar_label: ClickHouse Handler
description:
  Databend is ClickHouse wire protocol-compatible.
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/api/api-handler-clickhouse.png" width="200"/>
</p>

## Overview

Databend is ClickHouse wire protocol-compatible, allow you to connect to Databend server with Clickhouse client, make it easier for users/developers to use Databend.

## Client

Databend supports ClickHouse client to connect(Default port is 9000), it is same as you connect to a ClickHouse server.

```shell
clickhouse-client --host 127.0.0.1 --port 9000
```
