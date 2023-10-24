---
title: Apache Hive
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.83"/>

Databend supports the integration of an [Apache Hive](https://hive.apache.org/) catalog, enhancing its compatibility and versatility for data management and analytics. This extends Databend's capabilities by seamlessly incorporating the powerful metadata and storage management capabilities of Apache Hive into the platform.

## Datatype Mapping to Databend

This table maps data types between Apache Hive and Databend. Please note that Databend does not currently support Hive data types that are not listed in the table.

| Apache Hive         | Databend             |
| ------------------- | -------------------- |
| BOOLEAN             | [BOOLEAN](../../../13-sql-reference/10-data-types/00-data-type-logical-types.md)              |
| TINYINT             | [TINYINT (INT8)](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#integer-data-types)       |
| SMALLINT            | [SMALLINT (INT16)](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#integer-data-types)     |
| INT                 | [INT (INT32)](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#integer-data-types)          |
| BIGINT              | [BIGINT (INT64)](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#integer-data-types)       |
| DATE                | [DATE](../../../13-sql-reference/10-data-types/20-data-type-time-date-types.md)                 |
| TIMESTAMP           | [TIMESTAMP](../../../13-sql-reference/10-data-types/20-data-type-time-date-types.md)            |
| FLOAT               | [FLOAT (FLOAT32)](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#floating-point-data-types)      |
| DOUBLE              | [DOUBLE (FLOAT64)](../../../13-sql-reference/10-data-types/10-data-type-numeric-types.md#floating-point-data-types)     |
| VARCHAR             | [VARCHAR (STRING)](../../../13-sql-reference/10-data-types/30-data-type-string-types.md)     |
| DECIMAL             | [DECIMAL](../../../13-sql-reference/10-data-types/11-data-type-decimal-types.md)              |
| ARRAY&lt;TYPE&gt;    | [ARRAY](../../../13-sql-reference/10-data-types/40-data-type-array-types.md), supports nesting |
| MAP&lt;KEYTYPE, VALUETYPE&gt; | [MAP](../../../13-sql-reference/10-data-types/42-data-type-map.md)             |

## Managing Apache Hive Catalogs

Databend provides you the following commands to manage Apache Hive catalogs:

- [CREATE CATALOG](#create-catalog)
- [SHOW CREATE CATALOG](#show-create-catalog)

### CREATE CATALOG

Defines and establishes a new catalog in the Databend query engine.

#### Syntax

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

:::note
To read data from HDFS, you need to set the following environment variables before starting Databend. These environment variables ensure that Databend can access the necessary Java and Hadoop dependencies to interact with HDFS effectively. Make sure to replace "/path/to/java" and "/path/to/hadoop" with the actual paths to your Java and Hadoop installations, and adjust the CLASSPATH to include all the required Hadoop JAR files.
```shell
export JAVA_HOME=/path/to/java
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
export HADOOP_HOME=/path/to/hadoop
export CLASSPATH=/all/hadoop/jar/files
```
:::

### SHOW CREATE CATALOG

Returns the detailed configuration of a specified catalog, including its type and storage parameters.

#### Syntax

```sql
SHOW CREATE CATALOG <catalog_name>;
```

## Usage Examples

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

SHOW CREATE CATALOG hive_ctl;

┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Catalog │  Type  │                                                          Option                                                          │
├──────────┼────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ hive_ctl │ hive   │ METASTORE ADDRESS\n127.0.0.1:9083\nSTORAGE PARAMS\ns3 | bucket=databend-toronto,root=/,endpoint=https://s3.amazonaws.com │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```