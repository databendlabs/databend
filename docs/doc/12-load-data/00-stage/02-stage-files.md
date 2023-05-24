---
title: Staging Files with API
---

You can use the [File Upload API](../../11-integrations/00-api/10-put-to-stage.md) to stage a file by uploading a local file to a stage. This API can be called using curl or other HTTP client tools. Alternatively, you can also upload files directly to the folder in your bucket that maps to a stage using a web browser. Once uploaded, Databend can recognize them as staged files. 

## Examples

The following examples demonstrate how to upload a sample file ([books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)) to the User Stage, an internal stage, and an external stage with the File Upload API.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">

<TabItem value="user" label="Upload to User Stage">

Use cURL to make a request to the File Upload API:

```shell title='Put books.parquet to stage'
curl -u root: -H "stage_name:~" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

```shell title='Response'
{"id":"bf2574bd-a467-4690-82b9-12549a1875d4","stage_name":"~","state":"SUCCESS","files":["books.parquet"]}
```

Check the staged file:
```sql
LIST @~;

name         |size|md5|last_modified                |creator|
-------------+----+---+-----------------------------+-------+
books.parquet| 998|   |2023-04-20 20:55:03.100 +0000|       |
```
</TabItem>

<TabItem value="internal" label="Upload to Internal Stage">

1. Create a named internal stage:
```sql
CREATE STAGE my_internal_stage;
```
2. Use cURL to make a request to the File Upload API:

```shell title='Put books.parquet to stage'
curl -u root: -H "stage_name:my_internal_stage" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

```shell title='Response'
{"id":"a3b21915-b3a3-477f-8e31-b676074539ea","stage_name":"my_internal_stage","state":"SUCCESS","files":["books.parquet"]}
```

Check the staged file:
```sql
LIST @my_internal_stage;

name         |size|md5|last_modified                |creator|
-------------+----+---+-----------------------------+-------+
books.parquet| 998|   |2023-04-19 19:34:51.303 +0000|       |
```
</TabItem>
<TabItem value="external" label="Upload to External Stage">

1. Create a named external stage:

```sql
CREATE STAGE my_external_stage url = 's3://databend' CONNECTION =(ENDPOINT_URL= 'http://127.0.0.1:9000' aws_key_id='ROOTUSER' aws_secret_key='CHANGEME123');
```
2. Use cURL to make a request to the File Upload API:

```shell title='Put books.parquet to stage'
curl  -u root: -H "stage_name:my_external_stage" -F "upload=@books.parquet" -XPUT "http://127.0.0.1:8000/v1/upload_to_stage"
```

```shell title='Response'
{"id":"a21844fc-4c06-4b95-85a0-d57c28b9a142","stage_name":"my_external_stage","state":"SUCCESS","files":["books.parquet"]}
```

Check the staged file:
```sql
LIST @my_external_stage;

+-------------------+------+------------------------------------+-------------------------------+---------+
|       name        | size |                md5                 |         last_modified         | creator |
+-------------------+------+------------------------------------+-------------------------------+---------+
| books.parquet     |  998 | "88432bf90aadb79073682988b39d461c" | 2023-04-24 04:57:35.447 +0000 | NULL    |
+-------------------+------+------------------------------------+-------------------------------+---------+
```
</TabItem>
</Tabs>