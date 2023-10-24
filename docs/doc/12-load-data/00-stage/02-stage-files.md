---
title: Staging Files
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Databend recommends two file upload methods for stages: [PRESIGN](../../14-sql-commands/00-ddl/80-presign.md) and PUT/GET commands. These methods enable direct data transfer between the client and your storage, eliminating intermediaries and resulting in cost savings by reducing traffic between Databend and your storage.

![Alt text](../../../public/img/load/staging-file.png)

The PRESIGN method generates a time-limited URL with a signature, which clients can use to securely initiate file uploads. This URL grants temporary access to the designated stage, allowing clients to directly transfer data without relying on Databend servers for the entire process, enhancing both security and efficiency.

If you're using [BendSQL](../../13-sql-clients/01-bendsql.md) to manage files in a stage, you can use the PUT command for uploading files and the GET command for downloading files.

- The GET command currently can only download all files in a stage, not individual ones.
- These commands are exclusive to BendSQL and the GET command will not function when Databend uses the file system as the storage backend.

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

### Uploading with PUT Command

The following examples demonstrate how to use BendSQL to upload a sample file ([books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)) to the user stage, an internal stage, and an external stage with the PUT command.

<Tabs groupId="PUT">

<TabItem value="user" label="Upload to User Stage">

```sql
root@localhost:8000/default> PUT fs:///Users/eric/Documents/books.parquet @~

PUT fs:///Users/eric/Documents/books.parquet @~

┌───────────────────────────────────────────────┐
│                 file                │  status │
│                String               │  String │
├─────────────────────────────────────┼─────────┤
│ /Users/eric/Documents/books.parquet │ SUCCESS │
└───────────────────────────────────────────────┘
```

Check the staged file:

```sql
root@localhost:8000/default> LIST @~;

LIST @ ~

┌────────────────────────────────────────────────────────────────────────┐
│      name     │  size  │ ··· │     last_modified    │      creator     │
│     String    │ UInt64 │     │        String        │ Nullable(String) │
├───────────────┼────────┼─────┼──────────────────────┼──────────────────┤
│ books.parquet │    998 │ ... │ 2023-09-04 03:27:... │ NULL             │
└────────────────────────────────────────────────────────────────────────┘
1 row result in 0.287 sec. Processed 1 rows, 1 B (3.49 rows/s, 523 B/s)
```

</TabItem>

<TabItem value="internal" label="Upload to Internal Stage">

```sql
root@localhost:8000/default> CREATE STAGE my_internal_stage;

CREATE STAGE my_internal_stage

0 row written in 0.049 sec. Processed 0 rows, 0 B (0 rows/s, 0 B/s)

root@localhost:8000/default> PUT fs:///Users/eric/Documents/books.parquet @my_internal_stage

PUT fs:///Users/eric/Documents/books.parquet @my_internal_stage

┌───────────────────────────────────────────────┐
│                 file                │  status │
│                String               │  String │
├─────────────────────────────────────┼─────────┤
│ /Users/eric/Documents/books.parquet │ SUCCESS │
└───────────────────────────────────────────────┘
```

Check the staged file:

```sql
root@localhost:8000/default> LIST @my_internal_stage;

LIST @my_internal_stage

┌────────────────────────────────────────────────────────────────────────┐
│      name     │  size  │ ··· │     last_modified    │      creator     │
│     String    │ UInt64 │     │        String        │ Nullable(String) │
├───────────────┼────────┼─────┼──────────────────────┼──────────────────┤
│ books.parquet │    998 │ ... │ 2023-09-04 03:32:... │ NULL             │
└────────────────────────────────────────────────────────────────────────┘
1 row result in 0.273 sec. Processed 1 rows, 1 B (3.67 rows/s, 550 B/s)
```

</TabItem>
<TabItem value="external" label="Upload to External Stage">

```sql
root@localhost:8000/default> CREATE STAGE my_external_stage url = 's3://databend' CONNECTION =(ENDPOINT_URL= 'http://127.0.0.1:9000' aws_key_id='ROOTUSER' aws_secret_key='CHANGEME123');

CREATE STAGE my_external_stage url = 's3://databend' CONNECTION =(
  ENDPOINT_URL = 'http://127.0.0.1:9000' aws_key_id = 'ROOTUSER' aws_secret_key = 'CHANGEME123'
)

0 row written in 0.024 sec. Processed 0 rows, 0 B (0 rows/s, 0 B/s)

root@localhost:8000/default> PUT fs:///Users/eric/Documents/books.parquet @my_external_stage

PUT fs:///Users/eric/Documents/books.parquet @my_external_stage

┌───────────────────────────────────────────────┐
│                 file                │  status │
│                String               │  String │
├─────────────────────────────────────┼─────────┤
│ /Users/eric/Documents/books.parquet │ SUCCESS │
└───────────────────────────────────────────────┘
```

Check the staged file:

```sql
root@localhost:8000/default> LIST @my_external_stage;

LIST @my_external_stage

┌──────────────────────────────────────────────────────────────────────┐
│         name         │ ··· │     last_modified    │      creator     │
│        String        │     │        String        │ Nullable(String) │
├──────────────────────┼─────┼──────────────────────┼──────────────────┤
│ books.parquet        │ ... │ 2023-09-04 03:37:... │ NULL             │
└──────────────────────────────────────────────────────────────────────┘
3 rows result in 0.272 sec. Processed 3 rows, 3 B (11.05 rows/s, 1.55 KiB/s)
```

</TabItem>
</Tabs>

### Downloading with GET Command

The following examples demonstrate how to use BendSQL to download a sample file ([books.parquet](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.parquet)) from the user stage, an internal stage, and an external stage with the GET command.



<Tabs groupId="GET">

<TabItem value="user" label="Download from User Stage">

```sql
root@localhost:8000/default> LIST @~;

LIST @ ~

┌────────────────────────────────────────────────────────────────────────┐
│      name     │  size  │ ··· │     last_modified    │      creator     │
│     String    │ UInt64 │     │        String        │ Nullable(String) │
├───────────────┼────────┼─────┼──────────────────────┼──────────────────┤
│ books.parquet │    998 │ ... │ 2023-09-04 03:27:... │ NULL             │
└────────────────────────────────────────────────────────────────────────┘
1 row result in 0.287 sec. Processed 1 rows, 1 B (3.49 rows/s, 523 B/s)

root@localhost:8000/default> GET @~/ fs:///Users/eric/Downloads/fromStage/;

GET @~/ fs:///Users/eric/Downloads/fromStage/

┌─────────────────────────────────────────────────────────┐
│                      file                     │  status │
│                     String                    │  String │
├───────────────────────────────────────────────┼─────────┤
│ /Users/eric/Downloads/fromStage/books.parquet │ SUCCESS │
└─────────────────────────────────────────────────────────┘
```
</TabItem>

<TabItem value="internal" label="Download from Internal Stage">

```sql
root@localhost:8000/default> LIST @my_internal_stage;

LIST @my_internal_stage

┌────────────────────────────────────────────────────────────────────────┐
│      name     │  size  │ ··· │     last_modified    │      creator     │
│     String    │ UInt64 │     │        String        │ Nullable(String) │
├───────────────┼────────┼─────┼──────────────────────┼──────────────────┤
│ books.parquet │    998 │ ... │ 2023-09-04 03:32:... │ NULL             │
└────────────────────────────────────────────────────────────────────────┘
1 row result in 0.273 sec. Processed 1 rows, 1 B (3.67 rows/s, 550 B/s)

root@localhost:8000/default> GET @my_internal_stage/ fs:///Users/eric/Downloads/fromStage/;


GET @my_internal_stage/ fs:///Users/eric/Downloads/fromStage/

┌─────────────────────────────────────────────────────────┐
│                      file                     │  status │
│                     String                    │  String │
├───────────────────────────────────────────────┼─────────┤
│ /Users/eric/Downloads/fromStage/books.parquet │ SUCCESS │
└─────────────────────────────────────────────────────────┘
```
</TabItem>
<TabItem value="external" label="Download from External Stage">

```sql
root@localhost:8000/default> LIST @my_external_stage;

LIST @my_external_stage

┌──────────────────────────────────────────────────────────────────────┐
│         name         │ ··· │     last_modified    │      creator     │
│        String        │     │        String        │ Nullable(String) │
├──────────────────────┼─────┼──────────────────────┼──────────────────┤
│ books.parquet        │ ... │ 2023-09-04 03:37:... │ NULL             │
└──────────────────────────────────────────────────────────────────────┘
3 rows result in 0.272 sec. Processed 3 rows, 3 B (11.05 rows/s, 1.55 KiB/s)

root@localhost:8000/default> GET @my_external_stage/ fs:///Users/eric/Downloads/fromStage/;


GET @my_external_stage/ fs:///Users/eric/Downloads/fromStage/

┌─────────────────────────────────────────────────────────┐
│                      file                     │  status │
│                     String                    │  String │
├───────────────────────────────────────────────┼─────────┤
│ /Users/eric/Downloads/fromStage/books.parquet │ SUCCESS │
└─────────────────────────────────────────────────────────┘
```
</TabItem>
</Tabs>