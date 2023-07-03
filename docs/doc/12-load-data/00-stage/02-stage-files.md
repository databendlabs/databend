---
title: Staging Files
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Databend recommends using the Presigned URL method to upload files to the stage. This method provides a secure and efficient way to transfer data by generating a time-limited URL with a signature. By generating a Presigned URL, the client can directly upload the file to the designated stage without the need to route the traffic through Databend servers. This helps in offloading network traffic from the Databend infrastructure and can lead to improved performance and scalability. It also reduces the latency for file uploads, as the data can be transferred directly between the client and the storage destination without intermediaries.

See also: [PRESIGN](../../14-sql-commands/00-ddl/80-presign/presign.md)

## Examples

### Uploading with Presigned URL

The following examples demonstrate how to upload a sample file ([books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)) to the user stage, an internal stage, and an external stage with presigned URLs.

<Tabs groupId="presign">

<TabItem value="user" label="Upload to User Stage">

```sql
PRESIGN UPLOAD @~/books.parquet;

Name   |Value                                                                                                                                                                                                                                                                                                                                                       |
-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
method |PUT                                                                                                                                                                                                                                                                                                                                                         |
headers|{"host":"s3.us-east-2.amazonaws.com"}                                                                                                                                                                                                                                                                                                                       |
url    |https://s3.us-east-2.amazonaws.com/databend-toronto/stage/user/root/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230627%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230627T153448Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=84f1c280bff52f33c1914d64b2091d19650ad4882137013601fc44d26b607933|
```
```shell
curl -X PUT -T books.parquet "https://s3.us-east-2.amazonaws.com/databend-toronto/stage/user/root/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230627%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230627T153448Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=84f1c280bff52f33c1914d64b2091d19650ad4882137013601fc44d26b607933"
```

Check the staged file:

```sql
LIST @~;

name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-06-27 16:03:51.000 +0000|       |
```
</TabItem>

<TabItem value="internal" label="Upload to Internal Stage">

```sql
CREATE STAGE my_internal_stage;
```
```sql
PRESIGN UPLOAD @my_internal_stage/books.parquet;

Name   |Value                                                                                                                                                                                                                                                                                                                                                                        |
-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
method |PUT                                                                                                                                                                                                                                                                                                                                                                          |
headers|{"host":"s3.us-east-2.amazonaws.com"}                                                                                                                                                                                                                                                                                                                                        |
url    |https://s3.us-east-2.amazonaws.com/databend-toronto/stage/internal/my_internal_stage/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230628%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230628T022951Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=9cfcdf3b3554280211f88629d60358c6d6e6a5e49cd83146f1daea7dfe37f5c1|
```

```shell
curl -X PUT -T books.parquet "https://s3.us-east-2.amazonaws.com/databend-toronto/stage/internal/my_internal_stage/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIASTQNLUZWP2UY2HSN%2F20230628%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20230628T022951Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=9cfcdf3b3554280211f88629d60358c6d6e6a5e49cd83146f1daea7dfe37f5c1"
```

Check the staged file:

```sql
LIST @my_internal_stage;

name                               |size  |md5                               |last_modified                |creator|
-----------------------------------+------+----------------------------------+-----------------------------+-------+
books.parquet                      |   998|"88432bf90aadb79073682988b39d461c"|2023-06-28 02:32:15.000 +0000|       |
```
</TabItem>
<TabItem value="external" label="Upload to External Stage">

```sql
CREATE STAGE my_external_stage url = 's3://databend' CONNECTION =(ENDPOINT_URL= 'http://127.0.0.1:9000' aws_key_id='ROOTUSER' aws_secret_key='CHANGEME123');
```

```sql
PRESIGN UPLOAD @my_external_stage/books.parquet;

Name   |Value                                                                                                                                                                                                                                                                                                      |
-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
method |PUT                                                                                                                                                                                                                                                                                                        |
headers|{"host":"127.0.0.1:9000"}                                                                                                                                                                                                                                                                                  |
url    |http://127.0.0.1:9000/databend/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ROOTUSER%2F20230628%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230628T040959Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=697d608750fdcfe4a0b739b409cd340272201351023baa823382bf8c3718a4bd|
```
```shell
curl -X PUT -T books.parquet "http://127.0.0.1:9000/databend/books.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ROOTUSER%2F20230628%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230628T040959Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=697d608750fdcfe4a0b739b409cd340272201351023baa823382bf8c3718a4bd"
```

Check the staged file:

```sql
LIST @my_external_stage;

name         |size|md5                               |last_modified                |creator|
-------------+----+----------------------------------+-----------------------------+-------+
books.parquet| 998|"88432bf90aadb79073682988b39d461c"|2023-06-28 04:13:15.178 +0000|       |
```
</TabItem>
</Tabs>