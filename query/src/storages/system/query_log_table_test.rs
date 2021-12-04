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

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::storages::system::QueryLog;
use crate::storages::system::QueryLogTable;
use crate::storages::Table;
use crate::storages::ToReadDataSourcePlan;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query_log_table() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    ctx.get_settings().set_max_threads(2)?;
    ctx.get_sessions_manager()
        .get_query_log_memory_store()
        .append_query_log(QueryLog {
            query_log_type: 0,
            tenant_id: String::from("tenant_id"),
            cluster_id: String::from("cluster_id"),
            sql_user: String::from("sql_user"),
            sql_user_privileges: String::from("sql_user_privileges"),
            sql_user_quota: String::from("sql_user_quota"),
            client_address: String::from("client_address"),
            query_id: String::from("query_id"),
            query_text: String::from("query_text"),
            /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
            /// in seconds, it's physical type is UInt32
            query_start_time: 0,
            /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
            /// in seconds, it's physical type is UInt32
            query_end_time: 0,
            written_rows: 0,
            written_bytes: 0,
            read_rows: 0,
            read_bytes: 0,
            result_rows: 0,
            result_result: 0,
            memory_usage: 0,
            cpu_usage: 0,
            exception_code: 0,
            exception: String::from("exception"),
            client_info: String::from("client_info"),
            current_database: String::from("current_database"),
            databases: String::from("databases"),
            columns: String::from("columns"),
            projections: String::from("projections"),
            server_version: String::from("server_version"),
        });

    ctx.get_sessions_manager()
        .get_query_log_memory_store()
        .append_query_log(QueryLog {
            query_log_type: 0,
            tenant_id: String::from("tenant_id"),
            cluster_id: String::from("cluster_id"),
            sql_user: String::from("sql_user"),
            sql_user_privileges: String::from("sql_user_privileges"),
            sql_user_quota: String::from("sql_user_quota"),
            client_address: String::from("client_address"),
            query_id: String::from("query_id"),
            query_text: String::from("query_text"),
            /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
            /// in seconds, it's physical type is UInt32
            query_start_time: 0,
            /// A 32-bit datetime representing the elapsed time since UNIX epoch (1970-01-01)
            /// in seconds, it's physical type is UInt32
            query_end_time: 0,
            written_rows: 10,
            written_bytes: 10,
            read_rows: 0,
            read_bytes: 0,
            result_rows: 0,
            result_result: 0,
            memory_usage: 0,
            cpu_usage: 0,
            exception_code: 0,
            exception: String::from("exception"),
            client_info: String::from("client_info"),
            current_database: String::from("current_database"),
            databases: String::from("databases"),
            columns: String::from("columns"),
            projections: String::from("projections"),
            server_version: String::from("server_version"),
        });

    let table: Arc<dyn Table> = Arc::new(QueryLogTable::create(10));
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 27);

    let expected = vec![
        "+------+-----------+------------+----------+---------------------+----------------+----------------+----------+------------+------------------+----------------+--------------+---------------+-----------+------------+-------------+---------------+--------------+-----------+----------------+-----------+-------------+------------------+-----------+---------+-------------+----------------+",
        "| type | tenant_id | cluster_id | sql_user | sql_user_privileges | sql_user_quota | client_address | query_id | query_text | query_start_time | query_end_time | written_rows | written_bytes | read_rows | read_bytes | result_rows | result_result | memory_usage | cpu_usage | exception_code | exception | client_info | current_database | databases | columns | projections | server_version |",
        "+------+-----------+------------+----------+---------------------+----------------+----------------+----------+------------+------------------+----------------+--------------+---------------+-----------+------------+-------------+---------------+--------------+-----------+----------------+-----------+-------------+------------------+-----------+---------+-------------+----------------+",
        "| 0    | tenant_id | cluster_id | sql_user | sql_user_privileges | sql_user_quota | client_address | query_id | query_text | 0                | 0              | 0            | 0             | 0         | 0          | 0           | 0             | 0            | 0         | 0              | exception | client_info | current_database | databases | columns | projections | server_version |",
        "| 0    | tenant_id | cluster_id | sql_user | sql_user_privileges | sql_user_quota | client_address | query_id | query_text | 0                | 0              | 10           | 10            | 0         | 0          | 0           | 0             | 0            | 0         | 0              | exception | client_info | current_database | databases | columns | projections | server_version |",
        "+------+-----------+------------+----------+---------------------+----------------+----------------+----------+------------+------------------+----------------+--------------+---------------+-----------+------------+-------------+---------------+--------------+-----------+----------------+-----------+-------------+------------------+-----------+---------+-------------+----------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    Ok(())
}
