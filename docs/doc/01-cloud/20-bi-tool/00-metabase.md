---
title: Connect Databend Cloud To Metabase
sidebar_label: Metabase
description: 
  Connect Databend Cloud To Metabase
---

[Metabase](https://www.metabase.com/) is a simple but powerful analytics tool that enables everyone to make business decisions using their data without requiring any technical skills.

Before connecting to Metabase, sign in to your account on [Databend Cloud](../../10-deploy/20-databend-cloud.md).

## Install Metabase

Metabase is a Java application that can be executed by simply downloading the [JAR file](https://www.metabase.com/start/oss/jar). To install Metabase, simply run this command:

> Note: Metabase requires that you have Java 8 or higher versions installed on your system.

```shell
java -jar metabase.jar
```

This will launch a Metabase server on port 3000 by default.

## Download the ClickHouse plugin for Metabase

Because Databend is compatible with the [ClickHouse protocol](../../30-reference/00-api/02-clickhouse-handler.md), we can use ClickHouse plugin to help Databend connect to Metabase.

1. In the folder where you saved the file metabase.jar, create a subfolder called plugins.

2. Download the [latest version](https://github.com/enqueue/metabase-clickhouse-driver/releases/latest) of the ClickHouse Metabase plugin to the folder you created in step 1. The plugin comes with a JAR file called clickhouse.metabase-driver.jar.

3. Run the command `java -jar metabase.jar` to restart Metabase to load the driver properly.

4. Access Metabse at http://127.0.0.1:3000.

<img src="/img/cloud/bi/start_metabase.png"/>

## Connect Metabase to Databend

1. Click on the gear icon in the top-right corner and select `Admin Settings` to visit the Metabase admin page.

<img src="/img/cloud/bi/metabase_admin.png"/>

2. Click on `Add database`. Alternatively, you can click on the `Databases` tab and select the `Add database` button.

<img src="/img/cloud/bi/metabase_add_database.png"/>

3. Enter the connection details of your Databend database. The host is provided by [Databend Cloud](../../10-deploy/20-databend-cloud.md#connect-to-a-serverless-warehouse-on-databend-cloud). For example:

<img src="/img/cloud/bi/metabase_conn_databend.png"/>

4. Click the Save button and Metabase will scan your metadata in Databend.

5. Click on `Exit admin`.

## Run query

1. Click on the `+ New` in the top-right corner and select `SQL query` to view the Databend data.

<img src="/img/cloud/bi/start_sql_query.png"/>

2. Select the database name you created when connecting to Databend.

<img src="/img/cloud/bi/select_database.png"/>

3. Run a query.

<img src="/img/cloud/bi/run_query.png"/>
