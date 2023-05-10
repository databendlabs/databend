---
title: VACUUM TABLE
---
(This is an enterprise feature.)

Vacuum a table orphan files in Databend.

**Syntax**
```sql
vacuum table  [retain n hours] [dry run]
```

- If `retain n hours` option is specified, only orphan files that were created n hours ago will be removed. If not specified, `retention_period` option(which is 12 hours by default) will be used.

- If `dry run` option is specified, candidate orphan files will not be removed, instead, a result set of at most 1000 candidate files will be returned.

**Example**
```
mysql> vacuum table t retain 0 hours dry run;
+-----------------------------------------------------+
| Files                                               |
+-----------------------------------------------------+
| 1/8/_sg/932addea38c64393b82cb4b8fb7a2177_v3.bincode |
| 1/8/_b/b68cbe5fe015474d85a92d5f7d1b5d99_v2.parquet  |
+-----------------------------------------------------+
```