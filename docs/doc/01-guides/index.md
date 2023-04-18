---
title: Quick Start Resources
---

Welcome to our Quick Start Resources page! Whether you're a newbie to Databend or looking to refresh your knowledge, this page is designed to help you get up and running quickly 🚀. We've compiled a list of key documents to help you navigate our documentation and learn the essentials of Databend. From deployment guides to usecases, this page will provide you with everything you need to begin using Databend effectively.

## Set up Databend 🛠️

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="Deploy" label="Deploy" default>

* [Understanding Deployment Modes](../10-deploy/00-understanding-deployment-modes.md)
* [Deploying a Standalone Databend](../10-deploy/02-deploying-databend.md)
* [Expanding a Standalone Databend](../10-deploy/03-expanding-to-a-databend-cluster.md)
* [Deploying a Query Cluster on Kubernetes](../10-deploy/04-deploying-databend-on-kubernetes.md)
* [Deploying a Local Databend (for Non-Production Use)](../10-deploy/05-deploying-local.md)
* [Databend Cloud (Beta)](../02-cloud/index.md)

</TabItem>

<TabItem value="Connect" label="Connect">

* [How to Connect Databend with MySQL Client](../11-integrations/00-api/01-mysql-handler.md)
* [How to Connect Databend with ClickHouse HTTP Handler](../11-integrations/00-api/02-clickhouse-handler.md)
* [How to Connect Databend with REST API](../11-integrations/00-api/00-rest.md)
* [How to Connect Databend with MySQL-Compatible Clients](../11-integrations/30-access-tool/00-mysql.md)
* [How to Connect Databend with bendsql](../11-integrations/30-access-tool/01-bendsql.md)
* [How to Execute Queries in Python](../03-develop/01-python.md)
* [How to Query Databend in Jupyter Notebooks](../11-integrations/20-gui-tool/00-jupyter.md)
* [How to Execute Queries in Golang](../03-develop/00-golang.md)
* [How to Execute Queries in Node.js](../03-develop/02-nodejs.md)

</TabItem>

<TabItem value="Manage" label="Manage">

* [Manage Databend Settings](../10-deploy/06-manage-settings.md)
* [How to Back Up Meta Data](../10-deploy/06-metasrv/30-metasrv-backup-restore.md)
* [How to Back Up Databases](../10-deploy/09-upgrade/10-backup-and-restore-schema.md)
* [Upgrade Databend](../10-deploy/09-upgrade/50-upgrade.md)

</TabItem>
</Tabs>

## Load and Unload Data 📥

* [How to Load Data from Databend Stages](../12-load-data/00-stage.md)
* [How to Load Data from Amazon S3](../12-load-data/01-s3.md)
* [How to Load Data from Local File System](../12-load-data/02-local.md)
* [How to Load Data from Remote Files](../12-load-data/04-http.md)
* [Querying Data in Staged Files](../12-load-data/05-querying-stage.md)
* [Transforming Data During a Load](../12-load-data/06-data-load-transform.md)
* [How to Unload Data from Databend](../12-unload-data/index.md)

## Manage Data and User Access 🗄️

<Tabs>
<TabItem value="Data" label="Data" default>

* [COPY](../14-sql-commands/10-dml/dml-copy-into-table.md)
* [INSERT](../14-sql-commands/10-dml/dml-insert.md)
* [DELETE](../14-sql-commands/10-dml/dml-delete-from.md)
* [UPDATE](../14-sql-commands/10-dml/dml-update.md)
* [REPLACE](../14-sql-commands/10-dml/dml-replace.md)

</TabItem>

<TabItem value="Database" label="Database" >

* [How to Create a Database](../14-sql-commands/00-ddl/10-database/ddl-create-database.md)
* [How to Drop a Database](../14-sql-commands/00-ddl/10-database/ddl-drop-database.md)

</TabItem>

<TabItem value="Table" label="Table" >

* [How to Create a Table](../14-sql-commands/00-ddl/20-table/10-ddl-create-table.md)
* [How to Drop a Table](../14-sql-commands/00-ddl/20-table/20-ddl-drop-table.md)
* [How to Rename a Table](../14-sql-commands/00-ddl/20-table/30-ddl-rename-table.md)
* [How to Truncate a Table](../14-sql-commands/00-ddl/20-table/40-ddl-truncate-table.md)
* [How to Add/Drop Table Column](../14-sql-commands/00-ddl/20-table/90-alter-table-column.md)

</TabItem>

<TabItem value="View" label="View" >

* [How to Create a View](../14-sql-commands/00-ddl/60-view/ddl-create-view.md)
* [How to Drop a View](../14-sql-commands/00-ddl/60-view/ddl-drop-view.md)
* [How to Alter a View](../14-sql-commands/00-ddl/60-view/ddl-alter-view.md)

</TabItem>

<TabItem value="Function" label="Function" >

* [How to Create a User-Defined Function](../14-sql-commands/00-ddl/50-udf/ddl-create-function.md)
* [How to Drop a User-Defined Function](../14-sql-commands/00-ddl/50-udf/ddl-drop-function.md)
* [How to Alter a User-Defined Function](../14-sql-commands/00-ddl/50-udf/ddl-alter-function.md)
* [Generating SQL with AI](../15-sql-functions/61-ai-functions/01-ai-to-sql.md)
* [Creating Embedding Vectors](../15-sql-functions/61-ai-functions/02-ai-embedding-vector.md)
* [Text Completion with AI](../15-sql-functions/61-ai-functions/03-ai-text-completion.md)
* [Computing Text Similarities](../15-sql-functions/61-ai-functions/04-ai-cosine-distance.md)

</TabItem>

<TabItem value="User Access" label="User Access" >

* [How to Create a User](../14-sql-commands/00-ddl/30-user/01-user-create-user.md)
* [How to Grant Privileges to a User](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)
* [How to Revoke Privileges from a User](../14-sql-commands/00-ddl/30-user/11-revoke-privileges.md)
* [How to Create a Role](../14-sql-commands/00-ddl/30-user/04-user-create-role.md)
* [How to Grant Privileges to a Role](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)
* [How to Grant Role to a User](../14-sql-commands/00-ddl/30-user/20-grant-role.md)
* [How to Revoke Role from a User](../14-sql-commands/00-ddl/30-user/21-revoke-role.md)

</TabItem>
</Tabs>

## Ecosystem with Databend 📊

<Tabs>
<TabItem value="APIs" label="APIs" default>

* [HTTP Handler](../11-integrations/00-api/00-rest.md)
* [MySQL Handler](../11-integrations/00-api/01-mysql-handler.md)
* [ClickHouse Handler](../11-integrations/00-api/02-clickhouse-handler.md)
* [Streaming Load API](../11-integrations/00-api/03-streaming-load.md)
* [File Upload API](../11-integrations/00-api/10-put-to-stage.md)

</TabItem>

<TabItem value="Languages" label="Languages">

* [Golang](../03-develop/00-golang.md)
* [Python](../03-develop/01-python.md)
* [Node.js](../03-develop/02-nodejs.md)
* [Java](../03-develop/03-java.md)

</TabItem>

<TabItem value="Visualizations" label="Visualizations">

* [Jupyter Notebook](../11-integrations/20-gui-tool/00-jupyter.md)
* [Grafana](../11-integrations/20-gui-tool/02-grafana.md)
* [Metabase](../11-integrations/20-gui-tool/03-metabase.md)
* [Redash](../11-integrations/20-gui-tool/04-redash.md)

</TabItem>
</Tabs>