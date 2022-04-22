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

use clap::Args;
use serde::Deserialize;
use serde::Serialize;

/// Query config group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    #[clap(long, default_value_t)]
    pub tenant_id: String,

    /// ID for construct the cluster.
    #[clap(long, default_value_t)]
    pub cluster_id: String,

    #[clap(long, default_value_t)]
    pub num_cpus: u64,

    #[clap(long, default_value_t)]
    pub mysql_handler_host: String,

    #[clap(long, default_value_t)]
    pub mysql_handler_port: u16,

    #[clap(long, default_value_t)]
    pub max_active_sessions: u64,

    #[clap(long, default_value_t)]
    pub clickhouse_handler_host: String,

    #[clap(long, default_value_t)]
    pub clickhouse_handler_port: u16,

    #[clap(long, default_value_t)]
    pub http_handler_host: String,

    #[clap(long, default_value_t)]
    pub http_handler_port: u16,

    #[clap(long, default_value_t)]
    pub http_handler_result_timeout_millis: u64,

    #[clap(long, default_value_t)]
    pub flight_api_address: String,

    #[clap(long, default_value_t)]
    pub admin_api_address: String,

    #[clap(long, default_value_t)]
    pub metric_api_address: String,

    #[clap(long, default_value_t)]
    pub http_handler_tls_server_cert: String,

    #[clap(long, default_value_t)]
    pub http_handler_tls_server_key: String,

    #[clap(long, default_value_t)]
    pub http_handler_tls_server_root_ca_cert: String,

    #[clap(long, default_value_t)]
    pub api_tls_server_cert: String,

    #[clap(long, default_value_t)]
    pub api_tls_server_key: String,

    #[clap(long, default_value_t)]
    pub api_tls_server_root_ca_cert: String,

    /// rpc server cert
    #[clap(long, default_value_t)]
    pub rpc_tls_server_cert: String,

    /// key for rpc server cert
    #[clap(long, default_value_t)]
    pub rpc_tls_server_key: String,

    /// Certificate for client to identify query rpc server
    #[clap(long, default_value_t)]
    pub rpc_tls_query_server_root_ca_cert: String,

    #[clap(long, default_value_t)]
    pub rpc_tls_query_service_domain_name: String,

    /// Table engine csv enabled
    #[clap(long)]
    pub table_engine_csv_enabled: bool,

    /// Table engine parquet enabled
    #[clap(long)]
    pub table_engine_parquet_enabled: bool,

    /// Table engine memory enabled
    #[clap(long)]
    pub table_engine_memory_enabled: bool,

    /// Database engine github enabled
    #[clap(long)]
    pub database_engine_github_enabled: bool,

    #[clap(long, default_value_t)]
    pub wait_timeout_mills: u64,

    #[clap(long, default_value_t)]
    pub max_query_log_size: usize,

    /// Table Cached enabled
    #[clap(long)]
    pub table_cache_enabled: bool,

    /// Max number of cached table snapshot
    #[clap(long, default_value_t)]
    pub table_cache_snapshot_count: u64,

    /// Max number of cached table segment
    #[clap(long, default_value_t)]
    pub table_cache_segment_count: u64,

    /// Max number of cached table block meta
    #[clap(long, default_value_t)]
    pub table_cache_block_meta_count: u64,

    /// Table memory cache size (mb)
    #[clap(long, default_value_t)]
    pub table_memory_cache_mb_size: u64,

    /// Table disk cache folder root
    #[clap(long, default_value_t)]
    pub table_disk_cache_root: String,

    /// Table disk cache size (mb)
    #[clap(long, default_value_t)]
    pub table_disk_cache_mb_size: u64,

    /// If in management mode, only can do some meta level operations(database/table/user/stage etc.) with metasrv.
    #[clap(long)]
    pub management_mode: bool,

    #[clap(long, default_value_t)]
    pub jwt_key_file: String,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            tenant_id: "admin".to_string(),
            cluster_id: "".to_string(),
            num_cpus: num_cpus::get() as u64,
            mysql_handler_host: "127.0.0.1".to_string(),
            mysql_handler_port: 3307,
            max_active_sessions: 256,
            clickhouse_handler_host: "127.0.0.1".to_string(),
            clickhouse_handler_port: 9000,
            http_handler_host: "127.0.0.1".to_string(),
            http_handler_port: 8000,
            http_handler_result_timeout_millis: 10000,
            flight_api_address: "127.0.0.1:9090".to_string(),
            admin_api_address: "127.0.0.1:8080".to_string(),
            metric_api_address: "127.0.0.1:7070".to_string(),
            api_tls_server_cert: "".to_string(),
            api_tls_server_key: "".to_string(),
            api_tls_server_root_ca_cert: "".to_string(),
            http_handler_tls_server_cert: "".to_string(),
            http_handler_tls_server_key: "".to_string(),
            http_handler_tls_server_root_ca_cert: "".to_string(),
            rpc_tls_server_cert: "".to_string(),
            rpc_tls_server_key: "".to_string(),
            rpc_tls_query_server_root_ca_cert: "".to_string(),
            rpc_tls_query_service_domain_name: "localhost".to_string(),
            table_engine_csv_enabled: false,
            table_engine_parquet_enabled: false,
            table_engine_memory_enabled: true,
            database_engine_github_enabled: true,
            wait_timeout_mills: 5000,
            max_query_log_size: 10000,
            table_cache_enabled: false,
            table_cache_snapshot_count: 256,
            table_cache_segment_count: 10240,
            table_cache_block_meta_count: 102400,
            table_memory_cache_mb_size: 256,
            table_disk_cache_root: "_cache".to_string(),
            table_disk_cache_mb_size: 1024,
            management_mode: false,
            jwt_key_file: "".to_string(),
        }
    }
}
