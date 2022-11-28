---
title: 开始教程
---

这些教程旨在帮助你开始使用 Databend ：

## 部署 Databend

* [理解部署模式](../10-deploy/00-understanding-deployment-modes.md)
* [部署单节点 Databend](../10-deploy/02-deploying-databend.md)
* [扩展单节点 Databend 到集群](../10-deploy/03-expanding-to-a-databend-cluster.md)
* [在 Kubernetes 部署 Databend 集群](../10-deploy/04-deploying-databend-on-kubernetes.md)
* [部署本地 Databend (非生产环境)](../10-deploy/05-deploying-local.md)
* [Databend Cloud (Beta)](../02-cloud/index.md)

## 连接 Databend

* [如何使用 MySQL 客户端连接 Databend](../11-integrations/00-api/01-mysql-handler.md)
* [如何使用 ClickHouse 客户端连接 Databend](../11-integrations/00-api/02-clickhouse-handler.md)
* [如何使用 REST API 连接 Databend](../11-integrations/00-api/00-rest.md)
* [如何使用 DBeaver SQL IDE 连接 Databend](../11-integrations/20-gui-tool/01-dbeaver.md)
* [如何在 Python 中执行查询](../20-develop/01-python.md)
* [如何在 Jupyter Notebook 中查询 Databend](../11-integrations/20-gui-tool/00-jupyter.md)
* [如何在 Golang 中执行查询](../20-develop/00-golang.md)
* [如何在 Node.js 中执行查询](../20-develop/02-nodejs.md)

## 加载数据到 Databend

* [如何从 Databend Stages 加载数据](../12-load-data/00-stage.md)
* [如何从 Amazon S3 加载数据](../12-load-data/01-s3.md)
* [如何从本地文件系统加载数据](../12-load-data/02-local.md)
* [如何从远程文件加载数据](../12-load-data/04-http.md)

## 管理用户

* [如何创建用户](../14-sql-commands/00-ddl/30-user/01-user-create-user.md)
* [如何授予用户权限](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)
* [如何撤销用户权限](../14-sql-commands/00-ddl/30-user/11-revoke-privileges.md)
* [如何创建角色](../14-sql-commands/00-ddl/30-user/04-user-create-role.md)
* [如何授予角色权限](../14-sql-commands/00-ddl/30-user/10-grant-privileges.md)
* [如何授予用户角色](../14-sql-commands/00-ddl/30-user/20-grant-role.md)
* [如何撤销用户角色](../14-sql-commands/00-ddl/30-user/21-revoke-role.md)

## 管理数据库

* [如何创建数据库](../14-sql-commands/00-ddl/10-database/ddl-create-database.md)
* [如何删除数据库](../14-sql-commands/00-ddl/10-database/ddl-drop-database.md)

## 管理表

* [如何创建表](../14-sql-commands/00-ddl/20-table/10-ddl-create-table.md)
* [如何删除表](../14-sql-commands/00-ddl/20-table/20-ddl-drop-table.md)
* [如何重命名表](../14-sql-commands/00-ddl/20-table/30-ddl-rename-table.md)
* [如何截断表](../14-sql-commands/00-ddl/20-table/40-ddl-truncate-table.md)

## 管理视图

* [如何创建视图](../14-sql-commands/00-ddl/60-view/ddl-create-view.md)
* [如何删除视图](../14-sql-commands/00-ddl/60-view/ddl-drop-view.md)
* [如何重命名视图](../14-sql-commands/00-ddl/60-view/ddl-alter-view.md)

## 管理用户定义函数

* [如何创建用户定义函数](../14-sql-commands/00-ddl/50-udf/ddl-create-function.md)
* [如何删除用户定义函数](../14-sql-commands/00-ddl/50-udf/ddl-drop-function.md)
* [如何重命名用户定义函数](../14-sql-commands/00-ddl/50-udf/ddl-alter-function.md)

## 备份 & 恢复

* [如何备份元数据](../10-deploy/06-metasrv/30-metasrv-backup-restore.md)
* [如何备份数据库](../10-deploy/08-backup-restore/10-backup-and-restore-schema.md)

## 使用案例

* [如何使用 Databend 分析 Nginx 访问日志](../21-use-cases/02-analyze-nginx-logs-with-databend-and-vector.md)
* [如何使用 Databend 进行用户留存分析](../21-use-cases/03-analyze-user-retention-with-databend.md)
* [如何使用 Databend 进行漏斗分析](../21-use-cases/04-analyze-funnel-with-databend.md)

## 性能

* [如何对 Databend 进行基准测试](../21-use-cases/01-analyze-ontime-with-databend-on-ec2-and-s3.md)