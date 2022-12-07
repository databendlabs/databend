---
title: SHOW SETTINGS
---

Shows all settings of the current session.

## Syntax

```sql
SHOW SETTINGS;
```

## Examples

```sql
SHOW SETTINGS;

+---------------------------------+-------------+-------------+---------+-------------------------------------------------------------------------------------------------------------------+--------+
| name                            | value       | default     | level   | description                                                                                                       | type   |
+---------------------------------+-------------+-------------+---------+-------------------------------------------------------------------------------------------------------------------+--------+
| collation                       | binary      | binary      | SESSION | Char collation, support "binary" "utf8" default value: binary                                                     | String |
| enable_async_insert             | 0           | 0           | SESSION | Whether the client open async insert mode, default value: 0.                                                      | UInt64 |
| enable_cbo                      | 1           | 1           | SESSION | If enable cost based optimization, default value: 1.                                                              | UInt64 |
| enable_distributed_eval_index   | 1           | 1           | SESSION | If enable distributed eval index, default value: 1                                                                | UInt64 |
| enable_new_processor_framework  | 1           | 1           | SESSION | Enable new processor framework if value != 0, default value: 1.                                                   | UInt64 |
| enable_planner_v2               | 1           | 1           | SESSION | Enable planner v2 by setting this variable to 1, default value: 1.                                                | UInt64 |
| flight_client_timeout           | 60          | 60          | SESSION | Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds.               | UInt64 |
| format_compression              | None        | None        | SESSION | Format compression, default value: "None".                                                                        | String |
| format_empty_as_default         | 1           | 1           | SESSION | Format empty_as_default, default value: 1.                                                                        | UInt64 |
| format_escape                   |             |             | SESSION | format escape char, default value: "", which means the format`s default setting.                                  | String |
| format_field_delimiter          |             |             | SESSION | Format field delimiter, default value is "": use default of the format.                                           | String |
| format_nan_display              |             |             | SESSION | must be literal `nan` or `null` (case-sensitive), default value is "".                                            | String |
| format_quote                    |             |             | SESSION | The quote char for format. default value is "": use default of the format.                                        | String |
| format_record_delimiter         |             |             | SESSION | Format record_delimiter, default value is "": use default of the format.                                          | String |
| format_skip_header              | 0           | 0           | SESSION | Whether to skip the input header, default value: 0.                                                               | UInt64 |
| group_by_two_level_threshold    | 10000       | 10000       | SESSION | The threshold of keys to open two-level aggregation, default value: 10000.                                        | UInt64 |
| input_read_buffer_size          | 1048576     | 1048576     | SESSION | The size of buffer in bytes for input with format. By default, it is 1MB.                                         | UInt64 |
| load_file_metadata_expire_hours | 168         | 168         | SESSION | How many hours will the COPY file metadata expired in the metasrv, default value: 24*7=7days                      | UInt64 |
| max_block_size                  | 65536       | 65536       | SESSION | Maximum block size for reading, default value: 65536.                                                             | UInt64 |
| max_execute_time                | 0           | 0           | SESSION | The maximum query execution time. it means no limit if the value is zero. default value: 0.                       | UInt64 |
| max_memory_usage                | 26771259392 | 26771259392 | SESSION | The maximum memory usage for processing single query, in bytes. By default the value is determined automatically. | UInt64 |
| max_storage_io_requests         | 1000        | 1000        | SESSION | The maximum number of concurrent IO requests. By default, it is 64.                                               | UInt64 |
| max_threads                     | 24          | 24          | SESSION | The maximum number of threads to execute the request. By default the value is determined automatically.           | UInt64 |
| prefer_broadcast_join           | 0           | 0           | SESSION | If enable broadcast join, default value: 0                                                                        | UInt64 |
| quoted_ident_case_sensitive     | 1           | 1           | SESSION | Case sensitivity of quoted identifiers, default value: 1 (aka case-sensitive).                                    | UInt64 |
| row_tag                         | row         | row         | SESSION | In xml format, this field is represented as a row tag, e.g. <row>...</row>.                                       | String |
| sql_dialect                     | PostgreSQL  | PostgreSQL  | SESSION | SQL dialect, support "PostgreSQL" "MySQL" and "Hive", default value: "PostgreSQL".                                | String |
| storage_read_buffer_size        | 1048576     | 1048576     | SESSION | The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.                                    | UInt64 |
| timezone                        | UTC         | UTC         | SESSION | Timezone, default value: "UTC".                                                                                   | String |
| unquoted_ident_case_sensitive   | 0           | 0           | SESSION | Case sensitivity of unquoted identifiers, default value: 0 (aka case-insensitive).                                | UInt64 |
| wait_for_async_insert           | 1           | 1           | SESSION | Whether the client wait for the reply of async insert, default value: 1.                                          | UInt64 |
| wait_for_async_insert_timeout   | 100         | 100         | SESSION | The timeout in seconds for waiting for processing of async insert, default value: 100.                            | UInt64 |
+---------------------------------+-------------+-------------+---------+-------------------------------------------------------------------------------------------------------------------+--------+

```