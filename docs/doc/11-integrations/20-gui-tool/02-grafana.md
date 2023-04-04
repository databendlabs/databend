---
title: Connecting Databend With Grafana
sidebar_label: Grafana
description:
  Connecting Databend with Grafana.
---

## What is [Grafana](https://grafana.com/)?

The open-source platform for monitoring and observability.

Grafana allows you to query, visualize, alert on and understand your metrics no matter where they are stored. Create, explore, and share dashboards with your team and foster a data-driven culture.

-- From [Grafana Project](https://github.com/grafana/grafana)

## Grafana

### Create a Databend User

Connect to Databend server with [BendSQL](https://github.com/databendcloud/bendsql/):

```shell
❯ bendsql connect
Connected to Databend on Host: localhost
Version: DatabendQuery v0.9.46-nightly-67b0fe6(rust-1.68.0-nightly-2023-02-22T03:47:09.491571Z)

❯ bendsql query
Connected with driver databend (DatabendQuery v0.9.46-nightly-67b0fe6(rust-1.68.0-nightly-2023-02-22T03:47:09.491571Z))
Type "help" for help.

dd:root@localhost/default=>
```

Create a user:

```sql
CREATE USER grafana IDENTIFIED BY 'grafana_password';
```

Grant privileges for the user:
```sql
GRANT SELECT ON *.* TO grafana;
```

See also [How To Create User](../../14-sql-commands/00-ddl/30-user/01-user-create-user.md).


### Install Grafana

Please refer [Grafana Installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)


### Install Grafana Datasource Plugin

1. Go to grafana plugin page: https://grafana.yourdomain.com/plugins.

2. Search for `Altinity plugin for ClickHouse`, click install.

3. Go to grafana datasource page: https://grafana.yourdomain.com/datasources.

4. Click `Add data source`, select previously installed source type `Altinity plugin for ClickHouse`.

5. Configure the datasource:
    > Necessary fields:
    > * `HTTP -> URL` Your databend query clickhouse endpoint, for example: `http://localhost:8124`
    > * `Auth -> Basic auth` enabled
    > * `Basic Auth Details -> User, Password` previously created grafana user
    > * `Additional -> Use POST method to send queries` enabled

    :::tip
    For Databend Cloud users, use the endpoint in `connect`, for example:

    `https://tnxxx--small-xxx.ch.aws-us-east-2.default.databend.com`
    :::

<p align="center">
<img src="/img/integration/integration-gui-grafana-plugin-config.png" width="500px"/>
</p>

6. click `Save & Test` to verify datasource working.


### Graphing with Databend

Here we use an existing `nginx.access_logs` as an example:

```sql
CREATE TABLE `access_logs` (
  `timestamp` TIMESTAMP,
  `client` VARCHAR,
  `method` VARCHAR,
  `path` VARCHAR,
  `protocol` VARCHAR,
  `status` INT,
  `size` INT,
  `referrer` VARCHAR,
  `agent` VARCHAR,
  `request` VARCHAR
);
```

1. Create a new dashboard with a new panel, select the Datasource created in previous step.

2. Select `FROM` with database & table and click `Go to Query`:

<p align="center">
<img src="/img/integration/integration-gui-grafana-panel-config.png" width="500px"/>
</p>

3. Input the query with some template variables:

    ```sql
    SELECT
        (to_int64(timestamp) div 1000000 div $interval * $interval) * 1000 as t, status,
        count() as qps
    FROM $table
    WHERE timestamp >= to_datetime($from) AND timestamp <= to_datetime($to)
    GROUP BY t, status
    ORDER BY t
    ```

    :::tip
    You can click `Show Help` for available macros, some frequently used are:

    * `$interval` replaced with selected "Group by time interval" value (as a number of seconds)
    * `$table` replaced with selected table name from Query Builder
    * `$from` replaced with (timestamp with ms)/1000 value of UI selected "Time Range:From"
    * `$to` replaced with (timestamp with ms)/1000 value of UI selected "Time Range:To"
    :::

    Then you should be able to see the graph showing:

<p align="center">
<img src="/img/integration/integration-gui-grafana-query.png" width="100%" width="500px"/>
</p>

Adding more panels with this step, we could then have an example dashboard:

<p align="center">
<img src="/img/integration/integration-gui-grafana-dashboard.png" width="100%"/>
</p>
