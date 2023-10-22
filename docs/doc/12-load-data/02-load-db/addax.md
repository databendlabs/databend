---
title: Addax
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced: v1.1.70"/>

[Addax](https://github.com/wgzhao/Addax), originally derived from Alibaba's [DataX](https://github.com/alibaba/DataX), is a versatile open-source ETL (Extract, Transform, Load) tool. It excels at seamlessly transferring data between diverse RDBMS (Relational Database Management Systems) and NoSQL databases, making it an optimal solution for efficient data migration.

For information about the system requirements, download, and deployment steps for Addax, refer to Addax's [Getting Started Guide](https://github.com/wgzhao/Addax#getting-started). The guide provides detailed instructions and guidelines for setting up and using Addax.

See also: [DataX](datax.md)

## DatabendReader & DatabendWriter

DatabendReader and DatabendWriter are integrated plugins of Addax, allowing seamless integration with Databend.

The DatabendReader plugin enables reading data from Databend. Databend provides compatibility with the MySQL client protocol, so you can also use the [MySQLReader](https://wgzhao.github.io/Addax/develop/reader/mysqlreader/) plugin to retrieve data from Databend. For more information about DatabendReader, see https://wgzhao.github.io/Addax/develop/reader/databendreader/

## Tutorial: Data Loading from MySQL

In this tutorial, you will load data from MySQL to Databend with Addax. Before you start, make sure you have successfully set up Databend, MySQL, and Addax in your environment.

1. In MySQL, create a SQL user that you will use for data loading and then create a table and populate it with sample data.

```sql title='In MySQL:'
mysql> create user 'mysqlu1'@'%' identified by '123';
mysql> grant all on *.* to 'mysqlu1'@'%';
mysql> create database db;
mysql> create table db.tb01(id int, col1 varchar(10));
mysql> insert into db.tb01 values(1, 'test1'), (2, 'test2'), (3, 'test3');
```

2. In Databend, create a corresponding target table.

```sql title='In Databend:'
databend> create database migrated_db;
databend> create table migrated_db.tb01(id int null, col1 String null);
```

3. Copy and paste the following code to a file, and name the file as *mysql_demo.json*:

:::note
For the available parameters and their descriptions, refer to the documentation provided at the following link: https://wgzhao.github.io/Addax/develop/writer/databendwriter/#_2
:::

```json title='mysql_demo.json'
{
  "job": {
    "setting": {
      "speed": {
        "channel": 4
      }
    },
    "content": {
      "writer": {
        "name": "databendwriter",
        "parameter": {
          "preSql": [
            "truncate table @table"
          ],
          "postSql": [
          ],
          "username": "u1",
          "password": "123",
          "database": "migrate_db",
          "table": "tb01",
          "jdbcUrl": "jdbc:mysql://127.0.0.1:3307/migrated_db",
          "loadUrl": ["127.0.0.1:8000","127.0.0.1:8000"],
          "fieldDelimiter": "\\x01",
          "lineDelimiter": "\\x02",
          "column": ["*"],
          "format": "csv"
        }
      },
      "reader": {
        "name": "mysqlreader",
        "parameter": {
          "username": "mysqlu1",
          "password": "123",
          "column": [
            "*"
          ],
          "connection": [
            {
              "jdbcUrl": [
                "jdbc:mysql://127.0.0.1:3306/db"
              ],
              "driver": "com.mysql.jdbc.Driver",
              "table": [
              "tb01"
              ]
            }
          ]
        }
      }
    }
  }
}
```

4. Run Addax:

```shell
cd {YOUR_ADDAX_DIR_BIN}
./addax.sh -L debug ./mysql_demo.json 
```

You're all set! To verify the data loading, execute the query in Databend:

```sql
databend> select * from migrated_db.tb01;
+------+-------+
| id   | col1  |
+------+-------+
|    1 | test1 |
|    2 | test2 |
|    3 | test3 |
+------+-------+
```