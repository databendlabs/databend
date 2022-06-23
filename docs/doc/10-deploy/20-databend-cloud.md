---
title: Databend Cloud (beta)
sidebar_label: Databend Cloud (beta)
description: Databend Cloud (beta)
---

[Databend Cloud](https://app.databend.com) is a powerful data cloud for everyone, which is built on top of the open-source project Databend with **Simplicity**, **Elasticity**, **Security**, and **Low Cost**.


## Create a Databend Cloud Account

Databend Cloud is now in private beta, and we invite you to participate in the Databend Cloud beta here(for free): https://www.databend.com/apply

## Log In to Your Account

https://app.databend.com

<img src="/img/cloud/databend_cloud_login.png"/>


## Databend Cloud Overview

### Warehouses

Your virtual warehouse, warehouses can be automatically suspended when there's no activity after a specified period of time.

<img src="/img/cloud/databend_cloud_warehouse.png"/>

### Databases

Your database list.

<img src="/img/cloud/databend_cloud_database.png"/>

### Stages

The location of the data is stored in is known as a stage, and you can upload CSV/JSON/Parquet files from your local file system to Databend Cloud, and do data analysis.

<img src="/img/cloud/databend_cloud_stage.png"/>

<img src="/img/cloud/databend_cloud_stage_file.png"/>


### Worksheets

Worksheets is a powerful SQL editor for running your SQL queries. For example, you can do [Conversion Funnel Analysis](../90-learn/04-analyze-funnel-with-databend.md) online.

<img src="/img/cloud/databend_cloud_worksheet_demo.png"/>

### Connect to a Serverless Warehouse

Databend Cloud also provides a connection string for your applications to connect to it:
```shell
https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/
```

Run `SELECT 1` query with curl:
```shell
curl --header 'X-Clickhouse-User: <sql-user>' --header 'X-Clickhouse-Key: <sql-user-password>' https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/?query=SELECT%201
```
