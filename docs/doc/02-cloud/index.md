---
title: Try Databend Cloud (Beta) Free
sidebar_label: Databend Cloud
---

[Databend Cloud](https://app.databend.com) is a powerful data cloud for everyone, which is built on top of the open-source project Databend with **Simplicity**, **Elasticity**, **Security**, and **Low Cost**.


## Create a Databend Cloud Account

Databend Cloud is now in private beta. To create a Databend Cloud account, go to https://www.databend.com/apply to apply for beta access.

## Log in to Your Account

To log in to your account, go to https://app.databend.com.

<img src="/img/cloud/databend_cloud_login.png"/>


## Databend Cloud Overview

### Warehouses

Serverless warehouses can be automatically suspended in case of no activities for a specific period.

<img src="/img/cloud/databend_cloud_warehouse.png"/>


### Databases

This page shows a list of your databases:

<img src="/img/cloud/databend_cloud_database.png"/>

### Stages

The stage is the location where your data is stored. You can upload your local CSV, JSON, or Parquet files for data analytics.

<img src="/img/cloud/databend_cloud_stage.png"/>

<img src="/img/cloud/databend_cloud_stage_file.png"/>

### Worksheets

Worksheets is a powerful SQL editor where you can run SQL queries. For example, you can now do [Conversion Funnel Analysis](../90-learn/04-analyze-funnel-with-databend.md) online.

<img src="/img/cloud/databend_cloud_worksheet_demo.png"/>

### Connect to a Serverless Warehouse on Databend Cloud

Databend Cloud provides a connection string for your applications to connect to it:
<img src="/img/cloud/databend_cloud_warehouse_detail.png"/>
<img src="/img/cloud/databend_cloud_warehouse_connect.png"/>

```shell
https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/
```

Run `SELECT 1` query with curl:
```shell
curl --header 'X-Clickhouse-User: <sql-user>' --header 'X-Clickhouse-Key: <sql-user-password>' https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/?query=SELECT%201
```

## Community

- [Slack](https://link.databend.rs/join-slack) (For live discussion with the Community)
- [Github](https://github.com/datafuselabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/Datafuse_Labs) (Get the news fast)
- [Weekly](https://weekly.databend.rs/) (A weekly newsletter about Databend)
- [I'm feeling lucky](https://link.databend.rs/i-m-feeling-lucky) (Pick up a good first issue now!)