---
title: Try Databend Cloud (Beta) Free
sidebar_label: Databend Cloud
---

[Databend Cloud](https://www.databend.com) is a powerful data cloud for everyone, which is built on top of the open-source project Databend with **Simplicity**, **Elasticity**, **Security**, and **Low Cost**.

- [Get Started for Free](https://www.databend.com/apply)
- [Databend Cloud Documentation](https://www.databend.com/docs)
- [Architecture Overview](https://www.databend.com/docs)
- [Organizations & Users](https://www.databend.com/docs/organizations-users/manage-your-organization/)
- [Working with Warehouses](https://www.databend.com/docs/working-with-warehouses/understanding-warehouse)
- [Connecting to BI Tools](https://www.databend.com/docs/connecting-to-bi-tools/about-this-guide)
- [Developing with Databend Cloud](https://www.databend.com/docs/developing-with-databend-cloud/about-this-guide)


## Create a Databend Cloud Account

Databend Cloud is now in private beta. To create a Databend Cloud account, go to https://www.databend.com/apply to apply for beta access.

## Log in to Your Account

To log in to your account, go to https://app.databend.com.

<img src="/img/cloud/databend-cloud-login.png"/>


## Databend Cloud Overview

### Warehouses

Serverless warehouses can be automatically suspended in case of no activities for a specific period.

<img src="/img/cloud/databend-cloud-warehouses.gif"/>


### Databases

This page shows a list of your databases:

<img src="/img/cloud/databend-cloud-data.png"/>

### Worksheets

Worksheets is a powerful SQL editor where you can run SQL queries. For example, you can now do [Conversion Funnel Analysis](../21-use-cases/04-analyze-funnel-with-databend.md) online.

<img src="/img/cloud/databend-cloud-worksheet.png"/>

### Connect to a Serverless Warehouse on Databend Cloud

Databend Cloud provides a connection string for your applications to connect to it:
<img src="/img/cloud/databend-cloud-connect.gif"/>

```shell
https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com/
```

Run query with curl:
```shell
curl --user 'cloudapp:password' --data-binary 'SHOW TABLES' 'https://<tenant>--<warehouse>.ch.aws-us-east-2.default.databend.com?database=default'
```

## Community

- [Slack](https://link.databend.rs/join-slack) (For live discussion with the Community)
- [Github](https://github.com/datafuselabs/databend) (Feature/Bug reports, Contributions)
- [Twitter](https://twitter.com/DatabendLabs) (Get the news fast)
- [Weekly](https://weekly.databend.rs/) (A weekly newsletter about Databend)
- [I'm feeling lucky](https://link.databend.rs/i-m-feeling-lucky) (Pick up a good first issue now!)