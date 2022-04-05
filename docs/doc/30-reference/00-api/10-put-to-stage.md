---
title: PUT to Stage
sidebar_label: PUT to Stage
description:
  Uploads data files from a local file system directory/folder on a client machine to Databend named internal/external stages.
---

## Overview

Uploads data files from a local file system directory/folder on a client machine to Databend named internal stages.


## REST API

* A POST to `/v1/upload_to_stage` uploads the file to the server stage with stream in the POST body, and returns a JSON containing the query status.

| Parameters  | Description | Required |
| ----------- | ----------- | --- |
| `stage_name:<your-stage-name>`  | The client header of the internal stage name | YES |
| `update=@<your-file-path>`  | The file path which will be upload to the stage| YES |


## Quick Example


import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="internal" label="Put to Named Internal Stage">

This example show how we update a file to a named internal stage.

1. Create a named internal stage from MySQL client:
```sql title='mysql>'
create stage my_internal_stage;
```
2. Download sample data and PUT to stage

Download [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)

```shell title='Put books.parquet to stage'
curl  -H "stage_name:my_internal_stage" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

```shell title='Response'
{"id":"a3b21915-b3a3-477f-8e31-b676074539ea","stage_name":"my_internal_stage","state":"SUCCESS","files":["books.parquet"]}
```

Then check the stage files with:
```sql title='mysql>'
list @my_internal_stage;
```

```sql title='Result'
+---------------+
| file_name     |
+---------------+
| books.parquet |
+---------------+
```

The file `books.parquet` has PUT to your named internal stage.

</TabItem>
<TabItem value="external" label="Put to Named External Stage">

This example show how we update a file to a named external stage.

1. Create a named internal stage from MySQL client:
```sql title='mysql>'
create stage my_external_stage url = 's3://testbucket/admin/data/' credentials=(aws_key_id='minioadmin' aws_secret_key='minioadmin');
```
2. Download sample data and PUT to stage

Download [books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)

```shell title='Put books.parquet to stage'
curl  -H "stage_name:my_external_stage" -F "upload=@books.parquet" -XPUT "http://localhost:8000/v1/upload_to_stage"
```

```shell title='Response'
{"id":"a3b21915-b3a3-477f-8e31-b676074539ea","stage_name":"my_external_stage","state":"SUCCESS","files":["books.parquet"]}
```

Then check the stage files with:
```sql title='mysql>'
list @my_external_stage;
```

```sql title='Result'
+---------------+
| file_name     |
+---------------+
| books.parquet |
+---------------+
```

The file `books.parquet` has PUT to your named external stage.

</TabItem>

</Tabs>

