---
title: Try Databend Cloud Free
sidebar_label: Databend Cloud
---

[Databend Cloud](https://www.databend.com) is a powerful data cloud for everyone, which is built on top of the open-source project Databend with **Simplicity**, **Elasticity**, **Security**, and **Low Cost**.

## Databend Cloud Architecture

<div align="center">
<img src="https://user-images.githubusercontent.com/172204/221402796-646f5bc7-40b9-4e42-b837-2e60e1ba4583.png" alt="Databend Cloud Architecture"  width="70%"/>
</div>


## Create a Databend Cloud Account

Databend Cloud is now in private beta. To create a Databend Cloud account, go to https://www.databend.com/apply to apply for beta access.

## Logging into Your Account

To log into your account, go to https://app.databend.com.

<img src="/img/cloud/databend-cloud-login.png"/>


## Databend Cloud Overview

### Warehouses

Databend Cloud offers serverless warehouses that can be automatically suspended if there is no activity for a specific period.

A demonstration of how Databend Cloud's warehouses work is shown below.

<img src="/img/cloud/databend-cloud-warehouses.gif"/>


### Databases

This page shows a list of your databases in Databend Cloud:

<img src="/img/cloud/databend-cloud-data.png"/>

### Worksheets

Worksheets is a powerful SQL editor where you can run SQL queries. For example, you can now do [Conversion Funnel Analysis](../21-use-cases/04-analyze-funnel-with-databend.md) online.

<img src="/img/cloud/databend-cloud-worksheet.png"/>

### Connect

Databend Cloud provides a connection string for your applications to connect to it:

<img src="/img/cloud/databend-cloud-connect.gif"/>

```shell
https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/
```

Run query with curl:
```shell
curl --user 'cloudapp:password' --data-binary 'SHOW TABLES' 'https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com?database=default'
```

## Databend Cloud Documentation

 - [Databend Cloud Documentation](https://docs.databend.com/)