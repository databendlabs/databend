---
title: Input File Formats
---

Databend accepts a variety of file formats as a source where you can load or query data from using:
- [COPY INTO command](../14-sql-commands/10-dml/dml-copy-into-table.md)
- [Streaming Load API](../11-integrations/00-api/03-streaming-load.md)

When you select a file to load or query data from, you need to tell Databend what the file looks like in the following format:

```sql
FILE_FORMAT = ( TYPE = { CSV | TSV | NDJSON | PARQUET | XML} [ formatTypeOptions ] )
```

`Type`: Specifies the file format. Must be one of the following formats that Databend supports:
- CSV
- TSV
- NDJSON
- PARQUET
- XML

`formatTypeOptions`: Describes other format details about the file. The options may vary depending on the file format. See the sections below for the available options of each supported file format.

```
formatTypeOptions ::=
  RECORD_DELIMITER = '<character>'
  FIELD_DELIMITER = '<character>'
  SKIP_HEADER = <integer>
  QUOTE = '<character>'
  ESCAPE = '<character>'
  NAN_DISPLAY = '<string>'
  ROW_TAG = '<string>'
  COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | XZ | NONE
```

## CSV Options

## TSV Options

## NDJSON Options

## PARQUET Options

## XML Options