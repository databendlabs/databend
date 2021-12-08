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

use common_base::tokio;
use common_exception::Result;

use crate::storages::system::QueryLog;
use crate::storages::system::QueryLogMemoryStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_metrics_table() -> Result<()> {
    {
        let mut new_query_logs = Vec::new();
        for i in 1..6 {
            new_query_logs.push(QueryLog {
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
                written_rows: i,
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
        }

        let query_log_memory_store = QueryLogMemoryStore::create(10);
        query_log_memory_store.append_query_logs(new_query_logs);
        assert_eq!(query_log_memory_store.size(), 5);
        let logs = query_log_memory_store.list_query_logs();
        assert_eq!(logs.len(), 5);
        let mut written_rows: u64 = 1;
        for log in logs {
            assert_eq!(log.written_rows, written_rows);
            written_rows += 1;
        }
    }

    {
        let mut new_query_logs = Vec::new();
        for i in 1..21 {
            new_query_logs.push(QueryLog {
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
                written_rows: i,
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
        }

        let query_log_memory_store = QueryLogMemoryStore::create(10);
        query_log_memory_store.append_query_logs(new_query_logs);
        assert_eq!(query_log_memory_store.size(), 10);
        let logs = query_log_memory_store.list_query_logs();
        assert_eq!(logs.len(), 10);
        let mut written_rows: u64 = 11;
        for log in logs {
            assert_eq!(log.written_rows, written_rows);
            written_rows += 1;
        }
    }

    Ok(())
}
