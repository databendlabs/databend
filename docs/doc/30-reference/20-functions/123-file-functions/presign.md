---
title: PRESIGN
---

Generates the pre-signed URL for a staged file by the stage name and file path you provide. The pre-signed URL enables you to access the file through a web browswer or an API request.

:::tip
This function currently works with files stored at Amazon S3 only.
:::

## Syntax

```sql
PRESIGN @<stage_name>/.../<file_name>
```

## Examples

This example generates the pre-signed URL for the file `books.csv` on the stage `my-stage`:

```sql
PRESIGN @my_stage/books.csv
+--------+---------+---------------------------------------------------------------------------------+
| method | headers | url                                                                             |
+--------+---------+---------------------------------------------------------------------------------+
| GET    | {}      | https://example.s3.amazonaws.com/books.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&... |
+--------+---------+---------------------------------------------------------------------------------+
```