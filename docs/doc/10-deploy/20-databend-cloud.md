---
title: Databend Cloud (beta)
sidebar_label: Databend Cloud (beta)
description: Databend Cloud (beta)
---

[Databend Cloud](https://app.databend.com) is a powerful data cloud for everyone, which built on top of the open-source project Databend with **Simplicity**, **Elasticity**, **Security** and **Low Cost**.


## Create a Databend Cloud Account

Databend Cloud is now in private beta, we invite you to participate in the Databend Cloud beta here: https://www.databend.com/apply

## Log In to Your Account

https://app.databend.com


## Databend Cloud Overview

### Warehouses

Your virtual warehouse, warehouses can be automatically suspend when there's no activity after a specified period of time.

### Databases


### Stage

The location the data is stored in is known as a stage, you can upload CSV/JSON/Parquet files from your local file system to Databend Cloud stage, and do data analysis.


### Worksheet

Worksheets is a powerful SQL editor for running your SQL queries. For example, you can do [Conversion Funnel Analysis](../90-learn/04-analyze-funnel-with-databend.md) more easily.

### Connect to a Serverless Warehouse

Databend Cloud also provides a connection string for your app to connect to Databend Cloud:
```shell
https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/
```
To do a `SELECT 1` query with curl:
```shell
curl --header 'X-Clickhouse-User: <sql-user>' --header 'X-Clickhouse-Key: <sql-user-password>' https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/?query=SELECT%201
```


