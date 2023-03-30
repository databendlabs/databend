---
title: File Upload API
sidebar_label: File Upload API
description:
  Uploads local data files to a named internal or external stage.
---

Uploads local data files to a named internal or external stage.

## REST API

A POST to `/v1/upload_to_stage` uploads the file to the server stage with stream in the POST body, and returns a JSON containing the query status.

| Parameter  | Description | Required |
| ----------- | ----------- | --- |
| `stage_name:<your-stage-name>`  | HTTP header of the stage name | YES |
| `upload=@<your-file-path>`  | Path of the file you want to upload| YES |


## Examples

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="internal" label="Upload to Named Internal Stage">

This example shows how to upload a file to a named internal stage.

1. Create a named internal stage from MySQL client:
```sql
CREATE STAGE my_internal_stage;
```
2. Download and upload the sample file:

Download [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)

```shell title='Put books.parquet to stage'
curl  -H "stage_name:my_internal_stage" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

```shell title='Response'
{"id":"a3b21915-b3a3-477f-8e31-b676074539ea","stage_name":"my_internal_stage","state":"SUCCESS","files":["books.parquet"]}
```

Check the staged file:
```sql
LIST @my_internal_stage;
+---------------+
| file_name     |
+---------------+
| books.parquet |
+---------------+
```

The file `books.parquet` has been uploaded to your named internal stage.

</TabItem>
<TabItem value="external" label="Upload to Named External Stage">

This example shows how to upload a file to a named external stage.

1. Create a named external stage from MySQL client:
```sql
CREATE STAGE my_external_stage url = 's3://testbucket/admin/data/' credentials=(aws_key_id='minioadmin' aws_secret_key='minioadmin');
```
2. Download and upload the sample file:

Download [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)

```shell title='Put books.parquet to stage'
curl  -H "stage_name:my_external_stage" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root:
```

```shell title='Response'
{"id":"a3b21915-b3a3-477f-8e31-b676074539ea","stage_name":"my_external_stage","state":"SUCCESS","files":["books.parquet"]}
```

Check the staged file:
```sql
LIST @my_external_stage;
+---------------+
| file_name     |
+---------------+
| books.parquet |
+---------------+
```

The file `books.parquet` has been uploaded to your named external stage.

</TabItem>
</Tabs>
