// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_base::base::tokio;
use common_exception::Result;
use databend_query::sessions::TableContext;
use databend_query::storages::system::SettingsTable;
use databend_query::storages::TableStreamReadWrap;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_settings_table() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    ctx.get_settings().set_max_threads(2)?;

    let table = SettingsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let expected = vec![
        "+--------------------------------+------------+------------+---------+----------------------------------------------------------------------------------------------------+--------+",
        "| name                           | value      | default    | level   | description                                                                                        | type   |",
        "+--------------------------------+------------+------------+---------+----------------------------------------------------------------------------------------------------+--------+",
        "| compression                    | None       | None       | SESSION | Format compression, default value: None                                                            | String |",
        "| empty_as_default               | 1          | 1          | SESSION | Format empty_as_default, default value: 1                                                          | UInt64 |",
        "| enable_async_insert            | 0          | 0          | SESSION | Whether the client open async insert mode, default value: 0                                        | UInt64 |",
        "| enable_new_processor_framework | 1          | 1          | SESSION | Enable new processor framework if value != 0, default value: 1                                     | UInt64 |",
        "| enable_planner_v2              | 1          | 1          | SESSION | Enable planner v2 by setting this variable to 1, default value: 1                                  | UInt64 |",
        "| field_delimiter                | ,          | ,          | SESSION | Format field delimiter, default value: ,                                                           | String |",
        "| flight_client_timeout          | 60         | 60         | SESSION | Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds | UInt64 |",
        "| group_by_two_level_threshold   | 10000      | 10000      | SESSION | The threshold of keys to open two-level aggregation, default value: 10000                          | UInt64 |",
        "| max_block_size                 | 10000      | 10000      | SESSION | Maximum block size for reading                                                                     | UInt64 |",
        "| max_execute_time               | 0          | 0          | SESSION | The maximum query execution time. it means no limit if the value is zero. default value: 0         | UInt64 |",
        "| max_threads                    | 2          | 16         | SESSION | The maximum number of threads to execute the request. By default, it is determined automatically.  | UInt64 |",
        "| quoted_ident_case_sensitive    | 1          | 1          | SESSION | Case sensitivity of quoted identifiers, default value: 1 (aka case-sensitive)                      | UInt64 |",
        "| record_delimiter               | \"\\n\"       | \"\\n\"       | SESSION | Format record_delimiter, default value: \"\\n\"                                                       | String |",
        "| skip_header                    | 0          | 0          | SESSION | Whether to skip the input header, default value: 0                                                 | UInt64 |",
        "| sql_dialect                    | PostgreSQL | PostgreSQL | SESSION | SQL dialect, support \"PostgreSQL\" and \"MySQL\", default value: \"PostgreSQL\"                         | String |",
        "| storage_read_buffer_size       | 1048576    | 1048576    | SESSION | The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.                     | UInt64 |",
        "| timezone                       | UTC        | UTC        | SESSION | Timezone, default value: UTC,                                                                      | String |",
        "| unquoted_ident_case_sensitive  | 0          | 0          | SESSION | Case sensitivity of unquoted identifiers, default value: 0 (aka case-insensitive)                  | UInt64 |",
        "| wait_for_async_insert          | 1          | 1          | SESSION | Whether the client wait for the reply of async insert, default value: 1                            | UInt64 |",
        "| wait_for_async_insert_timeout  | 100        | 100        | SESSION | The timeout in seconds for waiting for processing of async insert, default value: 100              | UInt64 |",
        "+--------------------------------+------------+------------+---------+----------------------------------------------------------------------------------------------------+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
