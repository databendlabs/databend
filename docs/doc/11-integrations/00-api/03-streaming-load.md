---
title: Streaming Load API
sidebar_label: Streaming Load API
description:
  Streaming Load API
---

The Streaming Load API is used to read data from your local files and load it into Databend.

![image](/img/load/load-data-from-local-fs.png)

## API Request Format

To create a request with the Streaming Load API, follow the format below:

```bash
curl -H "<parameter>:<value>"  [-H "<parameter>:<value>"...] -F "upload=@<file_location>" [-F "upload=@<file_location>"] -XPUT http://<user_name>:[password]@<http_handler_host>:<http_handler_port>/v1/streaming_load
```
## Explaining Argument `-H`

The request usually includes many occurrences of the argument `-H` and each is followed by one of the following parameters to tell Databend how to handle the file you're loading data from. Please note that `insert_sql` is required, and other parameters are optional.

| Parameter               | Values                                                                                                                                                                                                                                                                                    | Supported Formats         | Examples                                                                                                                              |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| insert_sql              | [INSERT_statement] + format [file_format]                                                                                                                                                                                                                                                 | All                       | -H "insert_sql: insert into ontime format CSV"                                                                                        |
| format_skip_header      | Tells Databend how many lines at the beginning of the file to skip for header.<br /> 0 (default): No lines to skip;<br /> 1: Skip the first line;<br /> N: Skip the first N lines.                                                                                                        | CSV / TSV / NDJSON | -H "format_skip_header: 1"                                                                                                            |
| format_compression      | Tells Databend the compression format of the file.<br /> NONE (default): Do NOT decompress the file;<br /> AUTO: Automatically decompress the file by suffix;<br />  You can also use one of these values to explicitly specify the compression format: GZIP \                            | BZ2 \| BROTLI \                                                                                                                              | ZSTD \|  DEFALTE \| RAW_DEFLATE. | CSV / TSV / NDJSON | -H "format_compression:auto"                   |
| format_field_delimiter  | Tells Databend the characters used in the file to separate fields.<br /> Default for CSV files: `,`.<br /> Default for TSV files: `\t`.<br /> Hive output files using [SOH control character (\x01)]( https://en.wikipedia.org/wiki/C0_and_C1_control_codes#SOH) as the field delimiter.  | CSV / TSV                 | -H "format_field_delimiter:,". |
| format_record_delimiter | Tells Databend the new line characters used in the file to separate records.<br />  Default: `\n`.                                                                                                                                                                                        | CSV / TSV                 | -H "format_recorder_delimiter:\n"                                                                                                     |
| format_quote           | Tells Databend the quote characters for strings in CSV file.<br /> Default: ""(Double quotes).                                                                                                                                                                                            | CSV                       |                                                                                                                                       |

## Alternatives to Streaming Load API

The [COPY INTO](../../14-sql-commands/10-dml/dml-copy-into-table.md) command enables you to load data from files using insecure protocols, such as HTTP. This simplifies the data loading in some specific scenarios, for example, Databend is installed on-premises with MinIO. In such cases, you can load data from local files with the COPY INTO command. 

Example:

```sql
COPY INTO ontime200 FROM 'fs://<file_path>/ontime_200.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1);
```
To do so, you must add the setting `allow_insecure` to the configuration file `databend-query.toml` as indicated below and set it to `true`:

```toml
...
[storage]
# fs | s3 | azblob | obs
type = "fs"
allow_insecure = true
...
```

:::caution
For security reasons, Databend does NOT recommend insecure protocols for data loading. Use them for tests only. DO NOT set `allow_insecure` to `true` in any production environment. 
:::