---
title: Loading Data with Tools
slug: ./
---

Databend offers a comprehensive selection of connectors and plugins that seamlessly integrate with popular data import tools. These connectors facilitate smooth data transfer and synchronization between Databend and various external platforms. The table below highlights the supported tools along with their corresponding Databend connectors and plugins:

:::info
These connectors and plugins also work with Databend Cloud. When connecting to Databend Cloud, you will need the connection information obtained from one of your warehouses. For guidance on obtaining your warehouse information and establishing the connection, please refer to the following link: [Connecting to a Warehouse](https://docs.databend.com/using-databend-cloud/warehouses/#connecting)
:::

| Tool      	| Plugin / Connector                                                                                                                                                 	|
|-----------	|--------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| Addax     	| [DatabendReader](https://wgzhao.github.io/Addax/develop/reader/databendreader/) &  [DatabendWriter](https://wgzhao.github.io/Addax/develop/writer/databendwriter/) 	|
| Airbyte   	| [datafuselabs/destination-databend:alpha](https://hub.docker.com/r/airbyte/destination-databend)                                                                   	|
| DataX     	| [DatabendWriter](https://github.com/alibaba/DataX/blob/master/databendwriter/doc/databendwriter.md)                                                                	|
| dbt       	| [dbt-databend-cloud](https://github.com/databendcloud/dbt-databend)                                                                                                	|
| Debezium       	| [debezium-server-databend](https://github.com/databendcloud/debezium-server-databend)                                                                                    	|
| Flink CDC 	| [Flink SQL connector for Databend](https://github.com/databendcloud/flink-connector-databend)                                                                      	|
| Kafka     	| [bend-ingest-kafka](https://github.com/databendcloud/bend-ingest-kafka)                                                                                            	|
| Vector    	| [Databend sink](https://vector.dev/docs/reference/configuration/sinks/databend/)                                                                                   	|

Click on the tool names in the left navigation lane to explore detailed information about Databend's support for each tool, along with tutorials or examples.