---
title: CREATE CATALOG
---

import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.83"/>

Defines and establishes a new catalog in the Databend query engine. 

:::note
To read data from HDFS in Databend, you need to set the following environment variables before starting Databend. These environment variables ensure that Databend can access the necessary Java and Hadoop dependencies to interact with HDFS effectively. Make sure to replace "/path/to/java" and "/path/to/hadoop" with the actual paths to your Java and Hadoop installations, and adjust the CLASSPATH to include all the required Hadoop JAR files.

```sql
export JAVA_HOME=/path/to/java
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
export HADOOP_HOME=/path/to/hadoop
export CLASSPATH=/all/hadoop/jar/files
```
:::

## Syntax

```sql
CREATE CATALOG <catalog_name>
TYPE = <catalog_type>
CONNECTION = (
    METASTORE_ADDRESS = '<hive_metastore_address>'
    URL = '<data_storage_path>'
    <connection_parameter> = '<connection_parameter_value>'
    <connection_parameter> = '<connection_parameter_value>'
    ...
)
```

| Parameter             | Required? | Description                                                                                                               | 
|-----------------------|-----------|---------------------------------------------------------------------------------------------------------------------------| 
| TYPE                  | Yes       | Type of the catalog: 'HIVE' for Hive catalog or 'ICEBERG' for Iceberg catalog.                                      | 
| METASTORE_ADDRESS     | No        | Hive Metastore address. Required for Hive catalog only.| 
| URL                   | Yes       | Location of the external storage linked to this catalog. This could be a bucket or a folder within a bucket. For example, 's3://databend-toronto/'.                       | 
| connection_parameter  | Yes       | Connection parameters to establish connections with external storage. The required parameters vary based on the specific storage service and authentication methods. Refer to [Connection Parameters](../../../13-sql-reference/51-connect-parameters.md) for detailed information. |


## Examples

This example demonstrates the creation of a catalog configured to interact with the Hive Metastore and access data stored on Amazon S3, located at 's3://databend-toronto/'.

```sql
CREATE CATALOG hive_ctl 
TYPE = HIVE 
CONNECTION =(
    METASTORE_ADDRESS = '127.0.0.1:9083' 
    URL = 's3://databend-toronto/' 
    AWS_KEY_ID = '<your_key_id>' 
    AWS_SECRET_KEY = '<your_secret_key>' 
);
```

This example demonstrates the creation of a catalog configured to interact with an Iceberg data storage located in MinIO at 's3://databend/iceberg/'.

```sql
CREATE CATALOG iceberg_ctl
TYPE = ICEBERG
CONNECTION = (
    URL = 's3://databend/iceberg/'
    AWS_KEY_ID = 'minioadmin'
    AWS_SECRET_KEY = 'minioadmin'
    ENDPOINT_URL = 'http://127.0.0.1:9000'
);
```