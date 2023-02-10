---
title: Input & Output File Formats
---

Databend accepts a variety of file formats both as a source and as a target for data loading or unloading. For example, you can load data into Databend from a file with the [COPY INTO table command](../14-sql-commands/10-dml/dml-copy-into-table.md) or the [Streaming Load API](../11-integrations/00-api/03-streaming-load.md). You can also unload data from Databend into a file with the [COPY INTO location command](../14-sql-commands/10-dml/dml-copy-into-location.md) command. To do so, you need to tell Databend what the file looks like using the following syntax:

```sql
FILE_FORMAT = ( TYPE = { CSV | TSV | NDJSON | PARQUET | XML } [ formatTypeOptions ] )
```

`Type`: Specifies the file format. Must be one of the ones listed above that Databend supports.

:::note
Databend currently supports XML as a source ONLY. Unloading data into an XML file is not supported yet.
:::

If `FILE_FORMAT` is not specified, use `FILE_FORMAT = (TYPE = PARQUET)` by default.

`formatTypeOptions`: Includes one or more options to describe other format details about the file. The options vary depending on the file format. See the sections below to find out the available options for each supported file format.

```sql
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

Databend accepts CVS files that are compliant with [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180) and is subject to the following conditions:

- A string must be quoted if it contains the character of a [QUOTE](#quote), [ESCAPE](#escape), [RECORD_DELIMITER](#record_delimiter), or [FIELD_DELIMITER](#field_delimiter).
- No character will be escaped in a quoted string except [QUOTE](#quote).
- No space should be left between a [FIELD_DELIMITER](#field_delimiter) and a [QUOTE](#quote).
- A string will be quoted in CSV if it comes from a serialized Array or Struct field.
- If you develop a program and generate the CSV files from it, Databend recommends using the CSV library from the programing language.
- Databend does not recognize the files unloaded from MySQL as the CSV format unless the following conditions are satisfied:
  - `ESCAPED BY` is empty.
  - `ENCLOSED BY` is not empty.
:::note
Files will be recognized as the TSV format if the conditions above are not satisfied. For more information about the clauses `ESCAPED BY` and `ENCLOSED BY`, refer to https://dev.mysql.com/doc/refman/8.0/en/load-data.html.
:::

### RECORD_DELIMITER

Separates records in an input file.

**Available Values**: 
- `\r\n`
- An arbitrary character, such as `a` and `|`.
- A character with the escape char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`

**Default**: `\n`

### FIELD_DELIMITER

Separates fields in a record.

**Available Values**: 
- `\r\n`
- An arbitrary character, such as `a` and `|`.
- A character with the escape char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`

**Default**: `,` (comma)

### QUOTE

Quotes strings in a CSV file. For data loading, the quote is not necessary unless a string contains the character of a [QUOTE](#quote), [ESCAPE](#escape), [RECORD_DELIMITER](#record_delimiter), or [FIELD_DELIMITER](#field_delimiter).

:::note
**Used for data loading ONLY**: This option is not available when you unload data from Databend.
:::

**Available Values**: `\'` or `\"`.

**Default**: `\"`

### ESCAPE

Escapes a quote in a quoted string.

**Available Values**: `\'` or `\"` or `\\`.

**Default**: `\"`

### SKIP_HEADER

Specifies how many lines to be skipped from the beginning of the file.

:::note
**Used for data loading ONLY**: This option is not available when you unload data from Databend.
:::

**Default**: `0`

### NAN_DISPLAY

**Available Values**: Must be literal `'nan'` or `'null'` (case-insensitive)

**Default**: `'NaN'`

### COMPRESSION

Specifies the compression algorithm.

**Default**: `NONE`

**Available Values**:

| Values        | Notes                                                           |
| ------------- | --------------------------------------------------------------- |
| `AUTO`        | Auto detect compression via file extensions                     |
| `GZIP`        |                                                                 |
| `BZ2`         |                                                                 |
| `BROTLI`      | Must be specified if loading/unloading Brotli-compressed files. |
| `ZSTD`        | Zstandard v0.8 (and higher) is supported.                       |
| `DEFLATE`     | Deflate-compressed files (with zlib header, RFC1950).           |
| `RAW_DEFLATE` | Deflate-compressed files (without any header, RFC1951).         |
| `XZ`          |                                                                 |
| `NONE`        | Indicates that the files have not been compressed.              |

## TSV Options

Databend is subject to the following conditions when dealing with a TSV file:

- These characters in a TSV file will be escaped: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\\`, `\'`, [RECORD_DELIMITER](#record_delimiter-1), [FIELD_DELIMITER](#field_delimiter-1).
- Neither quoting nor enclosing is currently supported.
- A string will be quoted in CSV if it comes from a serialized Array or Struct field.
- Null is serialized as `\N`.

### RECORD_DELIMITER

Separates records in an input file.

**Available Values**: 
- `\r\n`
- An arbitrary character, such as `a` and `|`.
- A character with the escape char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`

**Default**: `\n`

### FIELD_DELIMITER

Separates fields in a record.

**Available Values**: 
- `\r\n`
- An arbitrary character, such as `a` and `|`.
- A character with the escape char: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\xHH`

**Default**: `\t` (TAB)

### COMPRESSION

Same as [the COMPRESSION option for CSV](#compression).

## NDJSON Options

### COMPRESSION

Same as [the COMPRESSION option for CSV](#compression).

## PARQUET Options

No available options.

## XML Options

### COMPRESSION

Same as [the COMPRESSION option for CSV](#compression).

### ROW_TAG

Used to select XML elements to be decoded as a record.

**Default**: `'row'`