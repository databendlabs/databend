---
title: "WebHDFS Storage Backend for Databend"
date: 2023-03-13
slug: 2023-03-13-webhdfs-storage-for-backend
tags: [databend, hadoop, hdfs, webhdfs]
description: "Get to how to configure webhdfs storage backend!"
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is an open-source elastic and workload-aware modern cloud data warehouse that allows you to do blazing-fast data analytics on a variety of storage services.

In this blog post, we will show you how to configure WebHDFS as a storage backend for Databend.

[WebHDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) is a REST API that provides HTTP access to HDFS, a popular distributed file system in the big data ecosystem. By using WebHDFS, you can avoid the dependency on Java environment and specific jar packages that are required by native HDFS client.

## Step 1: Prepare HDFS Environment

If you already have a deployed HDFS environment, you can skip this step. The only requirement is that WebHDFS must be enabled and accessible. Note that some public cloud providers may not support WebHDFS for their managed HDFS services.

If you don't have an HDFS environment, you can easily set up a local environment for testing purposes only.

```bash
git clone https://github.com/PsiACE/databend-workshop.git
cd databend-workshop/webhdfs
docker-compose up
```

After that, you should be able to access `http://127.0.0.1:9870` .

![](../static/img/blog/webhdfs-1.png)

## Step 2: Deploy Databend

Follow the instructions on databend.rs to configure WebHDFS for Databend. You need to set the endpoint and root (the path in HDFS) for WebHDFS. For users who need authentication, you can also configure delegation token (not covered in this blog).

```toml
[storage]
type = "webhdfs"
[storage.webhdfs]
endpoint_url = "http://127.0.0.1:9870"
# set your root
root = "/analyses/databend/storage"
# if your webhdfs needs authentication, uncomment and set with your value
# delegation = "<delegation-token>"
```

Then deploy Databend according to your preferred method.

```bash
./scripts/start.sh
```

## Step 3: Test Functionality

Upload `books.csv` file from your directory to the specified path in HDFS.

```bash
curl -L -X PUT -T ../data/books.csv 'http://127.0.0.1:9870/webhdfs/v1/data-files/books.csv?op=CREATE&overwrite=true'
```

And then, connect to Databend and create the database and table for books data.

```bash
$> mysql -uroot -h0.0.0.0 -P3307

mysql> DROP DATABASE IF EXISTS book_db;
Query OK, 0 rows affected (0.02 sec)

mysql> CREATE DATABASE book_db;
Query OK, 0 rows affected (0.02 sec)

mysql> use book_db;
Database changed

mysql> CREATE TABLE IF NOT EXISTS books (     title VARCHAR,     author VARCHAR,     date VARCHAR );
Query OK, 0 rows affected (0.02 sec)
```

Create a stage and use webhdfs support to `COPY INTO` the `books` table.

```bash
mysql> CREATE STAGE IF NOT EXISTS whdfs URL='webhdfs://127.0.0.1:9870/data-files/' CONNECTION=(HTTPS='false');
Query OK, 0 rows affected (0.01 sec)

mysql> DESC STAGE whdfs;
+-------+------------+----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+--------------------+---------+
| name  | stage_type | stage_params                                                                                                                           | copy_options                                                                                                       | file_format_options                                                                                                                                                                           | number_of_files | creator            | comment |
+-------+------------+----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+--------------------+---------+
| whdfs | External   | StageParams { storage: Webhdfs(StorageWebhdfsConfig { endpoint_url: "http://127.0.0.1:9870", root: "/data-files/", delegation: "" }) } | CopyOptions { on_error: AbortNum(1), size_limit: 0, split_size: 0, purge: false, single: false, max_file_size: 0 } | FileFormatOptions { format: Parquet, skip_header: 0, field_delimiter: ",", record_delimiter: "\n", nan_display: "NaN", escape: "", compression: None, row_tag: "row", quote: "", name: None } |            NULL | 'root'@'127.0.0.1' |         |
+-------+------------+----------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------+--------------------+---------+
1 row in set (0.01 sec)
Read 1 rows, 590.00 B in 0.002 sec., 414.67 rows/sec., 238.92 KiB/sec.

mysql> COPY INTO books FROM @whdfs FILES=('books.csv') file_format=(type=CSV field_delimiter=','  record_delimiter='\n' skip_header=0);
Query OK, 2 rows affected (1.83 sec)
```

After copying the data into the stage, you can run some SQL queries to check it. For example:

```sql
mysql> SELECT * FROM books;
+------------------------------+---------------------+------+
| title                        | author              | date |
+------------------------------+---------------------+------+
| Transaction Processing       | Jim Gray            | 1992 |
| Readings in Database Systems | Michael Stonebraker | 2004 |
+------------------------------+---------------------+------+
2 rows in set (0.02 sec)
Read 2 rows, 157.00 B in 0.015 sec., 137.21 rows/sec., 10.52 KiB/sec.
```

Finally, check the file browser at `127.0.0.1:9870` and you should see the corresponding storage under `/analyses/databend/storage/`.

![](../static/img/blog/webhdfs-2.png)

That's it! You have successfully configured WebHDFS as a storage backend for Databend.
