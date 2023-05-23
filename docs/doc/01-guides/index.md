---
title: Quick Start Resources
---

Welcome to our Quick Start Resources page! 

Whether you're a newbie to Databend or looking to refresh your knowledge, this page is designed to help you get up and running quickly 🚀. We've compiled a list of key documents to help you navigate our documentation and learn the essentials of Databend. From deployment guides to usecases, this page will provide you with everything you need to begin using Databend effectively.

## Databend Setup

Learn various deployment modes and connection options with Databend to customize your setup.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="Deploy" label="Deploy" default>

* [Understanding Deployment Modes](../10-deploy/00-understanding-deployment-modes.md)
* [Deploying a Standalone Databend](../10-deploy/02-deploying-databend.md)
* [Expanding a Standalone Databend](../10-deploy/03-expanding-to-a-databend-cluster.md)
* [Deploying a Query Cluster on Kubernetes](../10-deploy/04-deploying-databend-on-kubernetes.md)
* [Local and Docker Deployments (Non-Production Use)](../10-deploy/05-deploying-local.md)
* [Databend Cloud (Beta)](../02-cloud/index.md)

</TabItem>

<TabItem value="Connect" label="Connect">

* [Connecting to Databend with BendSQL](../11-integrations/30-access-tool/01-bendsql.md)
* [Connecting to Databend with JDBC](../11-integrations/30-access-tool/02-jdbc.md)
* [Connecting to Databend with MySQL-Compatible Clients](../11-integrations/30-access-tool/00-mysql.md)

</TabItem>

<TabItem value="Manage" label="Manage">

* [Managing Databend Settings](../13-sql-reference/42-manage-settings.md)
* [Backing Up and Restoring a Meta Service Cluster](../10-deploy/06-metasrv/30-metasrv-backup-restore.md)
* [Backing Up and Restoring Schema Data](../10-deploy/09-upgrade/10-backup-and-restore-schema.md)
* [Upgrading Databend](../10-deploy/09-upgrade/50-upgrade.md)

</TabItem>
</Tabs>

## Data Load & Unload

Databend makes it easy to load data from various sources, stage, Amazon S3, local and remote files, and so on.

<Tabs>
<TabItem value="Stage" label="Stage" default>

* [Understanding Stages](../12-load-data/00-stage/00-whystage.md)
* [Managing Stages](../12-load-data/00-stage/01-manage-stages.md)
* [Staging Files with API](../12-load-data/00-stage/01-manage-stages.md)

</TabItem>

<TabItem value="Load" label="Load">

* [Loading from Stage](../12-load-data/01-load/00-stage.md)
* [Loading from Bucket](../12-load-data/01-load/01-s3.md)
* [Loading from Local File](../12-load-data/01-load/02-local.md)
* [Loading from Remote File](../12-load-data/01-load/04-http.md)

</TabItem>

<TabItem value="Unload" label="Unload">

* [Unloading Data](../12-load-data/09-unload.md)

</TabItem>

</Tabs>


## Data & User Management

To make the most of Databend, learn how to manage your database by inserting, updating, and deleting data, creating and dropping databases and tables, and managing user-defined functions and views. Explore advanced features like generating SQL with AI and managing users, roles, and privileges for fine-grained control.

<Tabs>
<TabItem value="Data" label="Data" default>

* [How to Insert Data into a Table](../14-sql-commands/10-dml/dml-insert.md)
* [How to Update Data in a Table](../14-sql-commands/10-dml/dml-update.md)
* [How to Replace a Row in a Table](../14-sql-commands/10-dml/dml-replace.md)
* [How to Delete One or More Rows from a Table](../14-sql-commands/10-dml/dml-delete-from.md)

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
* [How to Flash Back a Table](../14-sql-commands/00-ddl/20-table/70-flashback-table.md)

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

<TabItem value="User" label="User" >

* [How to Create a User](../14-sql-commands/00-ddl/30-user/01-user-create-user.md)
* [How to Grant Privileges to a User](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)
* [How to Revoke Privileges from a User](../14-sql-commands/00-ddl/30-user/11-revoke-privileges.md)
* [How to Create a Role](../14-sql-commands/00-ddl/30-user/04-user-create-role.md)
* [How to Grant Privileges to a Role](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)
* [How to Grant Role to a User](../14-sql-commands/00-ddl/30-user/20-grant-role.md)
* [How to Revoke Role from a User](../14-sql-commands/00-ddl/30-user/21-revoke-role.md)

</TabItem>
</Tabs>

## Integrations

Databend's rich ecosystem offers a range of powerful tools and integrations, allowing you to work more efficiently and effectively.

<Tabs>
<TabItem value="Visualizations" label="Visualizations" default>

* [Jupyter Notebook](../11-integrations/20-gui-tool/00-jupyter.md)
* [Grafana](../11-integrations/20-gui-tool/02-grafana.md)
* [Metabase](../11-integrations/20-gui-tool/03-metabase.md)
* [Redash](../11-integrations/20-gui-tool/04-redash.md)

</TabItem>

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
* [Java](../03-develop/03-jdbc.md)
* [Rust](../03-develop/04-rust.md)

</TabItem>
</Tabs>

## Next Steps

Congratulations on completing the Quick Start materials 👏👏👏! 

We hope you found them helpful in getting up and running with Databend. To continue your journey with Databend, we encourage you to check out our documentation, which provides in-depth information on Databend's features and capabilities. You can also join our [community](../00-overview/index.md#community) to connect with other Databend users and get help with any questions or issues you may have.