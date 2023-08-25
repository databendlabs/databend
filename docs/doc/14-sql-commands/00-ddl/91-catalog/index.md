---
title: Catalog
---
import IndexOverviewList from '@site/src/components/IndexOverviewList';

### What is Catalog?

A catalog in Databend is a fundamental organizational concept that assists in efficiently managing and accessing your data sources. It serves as a central repository for metadata information about your data, enabling you to query data from external catalogs without loading the data into Databend. 

Databend provides support for two primary catalog types: [Apache Iceberg](https://iceberg.apache.org/) and [Apache Hive](https://hive.apache.org/). You can learn more about them on their official websites. Please note that, Databend currently only supports the Parquet format for both Iceberg and Hive dataset files.

### Managing Catalogs

Databend allows you to create catalogs, connecting Databend to external catalogs stored in a variety of commonly used storage systems. This makes it easy to link and manage different data sources using these commands:

<IndexOverviewList />

### Usage Example

Here are examples of creating Iceberg and Hive catalogs in Databend:

```sql
-- Create a Hive catalog
CREATE CATALOG hive_ctl 
TYPE = HIVE 
CONNECTION =(
    METASTORE_ADDRESS = '127.0.0.1:9083' 
    URL = 's3://databend-toronto/' 
    AWS_KEY_ID = '<your_key_id>' 
    AWS_SECRET_KEY = '<your_secret_key>' 
);

SHOW CREATE CATALOG hive_ctl;

Name   |Value                                                                                                                |
-------+---------------------------------------------------------------------------------------------------------------------+
Catalog|hive_ctl                                                                                                             |
Type   |hive                                                                                                                 |
Option |METASTORE ADDRESS¶127.0.0.1:9083¶STORAGE PARAMS¶s3 | bucket=databend-toronto,root=/,endpoint=https://s3.amazonaws.com|

-- Create an Iceberg catalog
CREATE CATALOG iceberg_ctl
TYPE = ICEBERG
CONNECTION = (
    URL = 's3://databend/iceberg/'
    AWS_KEY_ID = 'minioadmin'
    AWS_SECRET_KEY = 'minioadmin'
    ENDPOINT_URL = 'https://127.0.0.1:9000'
);

SHOW CREATE CATALOG iceberg_ctl;

Name   |Value                                                                             |
-------+----------------------------------------------------------------------------------+
Catalog|iceberg_ctl                                                                       |
Type   |iceberg                                                                           |
Option |STORAGE PARAMS¶s3 | bucket=databend,root=/iceberg/,endpoint=https://127.0.0.1:9000|
```