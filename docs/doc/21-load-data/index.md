---
title: OVERVIEW
slug: ./
---

Databend enables you to load data from the following locations:

- Stage:  Databend loads data from staged files. A Databend stage can be internal or external. For more information, see [What are Databend Stages](../30-reference/30-sql/00-ddl/40-stage/index.md#what-are-databend-stages).

- Bucket or container: Databend loads data from files stored in an object storage bucket or container. For the object storage solutions supported by Databend, see [Supported Object Storage Solutions](../10-deploy/00-understanding-deployment-modes.md#supported-object-storage-solutions).

- Local file system: Databend loads data from local files.

- Remote server: Databend loads data from remote files (including IPFS) accessible by URLs starting with "HTTPS://...".

- MySQL: Databend reproduces your data in MySQL using dump files written by [mysqldump](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html).

The data files can be in various formats and compressed if desired. The following table shows the supported file formats and recommended loading methods for each type of file location.

| Data File Location  | File Formats                         | Compression Formats                           | Load Data with     |
|---------------------|--------------------------------------|-----------------------------------------------|--------------------|
| Stage               | CSV, TSV, JSON, NDJSON, Parquet, XML | GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE | COPY INTO command  |
| Bucket or container | CSV, TSV, JSON, NDJSON, Parquet, XML | GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE | COPY INTO command  |
| Local file system   | CSV, TSV, JSON, NDJSON, Parquet, XML | GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE | Streaming Load API |
| Remote server       | CSV, TSV, JSON, NDJSON, Parquet, XML | GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE | COPY INTO command  |
| MySQL               | Dump files (.sql)                    | n/a                                           | mysqldump          |

Databend recommends the COPY INTO command for loading data from files in a stage, bucket, or remote server. You can tell Databend how to load your data by including the options of the COPY INTO command. The COPY INTO command has many options that allow you to specify how your data will be loaded. Make sure you have fully understood them before you issue the command. For detailed explanations about the COPY INTO command and its options, see [COPY INTO table](../30-reference/30-sql/10-dml/dml-copy-into-table.md).

Here are some tutorials to help you get started with data loading:

- Tutorial: Load from an internal stage
- Tutorial: Load from an Amazon S3 bucket
- Tutorial: Load from a local file with Streaming Load API
- Tutorial: Load from a remote file
- Tutorial: Load from MySQL with mysqldump