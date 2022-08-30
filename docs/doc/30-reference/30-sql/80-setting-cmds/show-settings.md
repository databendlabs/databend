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

|name                          |value  |default|level  |description                                                                                       |type  |
|------------------------------|-------|-------|-------|--------------------------------------------------------------------------------------------------|------|
|compression                   |None   |None   |SESSION|Format compression, default value: None                                                           |String|
|empty_as_default              |1      |1      |SESSION|Format empty_as_default, default value: 1                                                         |UInt64|
|enable_async_insert           |0      |0      |SESSION|Whether the client open async insert mode, default value: 0                                       |UInt64|
|enable_new_processor_framework|1      |1      |SESSION|Enable new processor framework if value != 0, default value: 1                                    |UInt64|
|enable_planner_v2             |0      |0      |SESSION|Enable planner v2 by setting this variable to 1, default value: 0                                 |UInt64|
|field_delimiter               |,      |,      |SESSION|Format field delimiter, default value: ,                                                          |String|
|flight_client_timeout         |60     |60     |SESSION|Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds|UInt64|
|group_by_two_level_threshold  |10000  |10000  |SESSION|The threshold of keys to open two-level aggregation, default value: 10000                         |UInt64|
|max_block_size                |10000  |10000  |SESSION|Maximum block size for reading                                                                    |UInt64|
|max_execute_time              |0      |0      |SESSION|The maximum query execution time. it means no limit if the value is zero. default value: 0        |UInt64|
|max_threads                   |4      |16     |SESSION|The maximum number of threads to execute the request. By default, it is determined automatically. |UInt64|
|record_delimiter              |¶      |¶      |SESSION|Format record_delimiter, default value: ¶                                                         |String|
|skip_header                   |0      |0      |SESSION|Whether to skip the input header, default value: 0                                                |UInt64|
|storage_read_buffer_size      |1048576|1048576|SESSION|The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.                    |UInt64|
|timezone                      |UTC    |UTC    |SESSION|Timezone, default value: UTC,                                                                     |String|
|wait_for_async_insert         |1      |1      |SESSION|Whether the client wait for the reply of async insert, default value: 1                           |UInt64|
|wait_for_async_insert_timeout |100    |100    |SESSION|The timeout in seconds for waiting for processing of async insert, default value: 100             |UInt64|
```