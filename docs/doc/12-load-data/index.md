---
title: Load Data into Databend
slug: ./
---
These topics describe how to load data into Databend.

- [Preparations](#preparations)
    - [Supported File Locations](#supported-file-locations)
    - [Supported File Formats](#supported-file-formats)
- [Data Loading Methods](#loading-methods)
    - [COPY INTO Command](#copy-into-command)
    - [Streaming Load API](#streaming-load-api)
- [Data Loading Considerations](#data-loading-considerations)
- [Hands-On Tutorials](#hands-on-tutorials)

---

## Preparations

Databend enables you to load data from files in a variety of formats stored in different locations. Before loading your data into Databend, make sure your files meet these requirements:

### Supported File Locations

Databend can load data from files that are stored in local file system, [Supported Object Storage Solutions](../10-deploy/00-understanding-deployment-modes.md#supported-object-storage-solutions), and remote servers.

### Supported File Formats

The data files can be in various formats and compressed if desired. Databend supports loading data from files in these formats:

- CSV
- TSV
- NDJSON
- Parquet
- XML

The supported compression formats include:

- GZIP
- BZ2
- BROTLI
- ZSTD
- DEFLATE
- RAW_DEFLATE
- XZ

## Loading Methods

Databend recommends using the COPY INTO command to load data from files in a stage, bucket, or remote server, and using the Streaming Load API to load data from local files.

### COPY INTO Command

The COPY INTO command can load data from files in a stage, bucket, or remote server. You can tell Databend how to load your data by including the options of the COPY INTO command. The COPY INTO command has many options that allow you to specify how your data will be loaded. 

For detailed explanations about the COPY INTO command and its options, see [COPY INTO](../14-sql-commands/10-dml/dml-copy-into-table.md).

### Streaming Load API

The Streaming Load API can read data from your local data files and load it into Databend. For more information about the Streaming Load API, see [Streaming Load API](../11-integrations/00-api/03-streaming-load.md).

## Data Loading Considerations

## Hands-On Tutorials

Here are some tutorials to help you get started with data loading:

- [Tutorial: Load from an internal stage](00-stage.md): In this tutorial, you will create an internal stage, stage a sample file, and then load data from the file into Databend with the COPY INTO command.
- [Tutorial: Load from an Amazon S3 bucket](01-s3.md): In this tutorial, you will upload a sample file to your Amazon S3 bucket, and then load data from the file into Databend with the COPY INTO command.
- [Tutorial: Load from a local file](./02-local.md): In this tutorial, you will load data from a local sample file into Databend with the Streaming Load API.
- [Tutorial: Load from a remote file](04-http.md): In this tutorial, you will load data from a remote sample file into Databend with the COPY INTO command.