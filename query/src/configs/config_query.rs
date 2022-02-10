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

use crate::configs::Config;

// Query env.
pub const QUERY_TENANT_ID: &str = "QUERY_TENANT_ID";
pub const QUERY_CLUSTER_ID: &str = "QUERY_CLUSTER_ID";
pub const QUERY_NUM_CPUS: &str = "QUERY_NUM_CPUS";
pub const QUERY_MYSQL_HANDLER_HOST: &str = "QUERY_MYSQL_HANDLER_HOST";
pub const QUERY_MYSQL_HANDLER_PORT: &str = "QUERY_MYSQL_HANDLER_PORT";
pub const QUERY_MAX_ACTIVE_SESSIONS: &str = "QUERY_MAX_ACTIVE_SESSIONS";
pub const QUERY_CLICKHOUSE_HANDLER_HOST: &str = "QUERY_CLICKHOUSE_HANDLER_HOST";
pub const QUERY_CLICKHOUSE_HANDLER_PORT: &str = "QUERY_CLICKHOUSE_HANDLER_PORT";
pub const QUERY_HTTP_HANDLER_HOST: &str = "QUERY_HTTP_HANDLER_HOST";
pub const QUERY_HTTP_HANDLER_PORT: &str = "QUERY_HTTP_HANDLER_PORT";
pub const QUERY_HTTP_HANDLER_RESULT_TIMEOUT_MILLIS: &str =
    "QUERY_HTTP_HANDLER_RESULT_TIMEOUT_MILLIS";
pub const QUERY_FLIGHT_API_ADDRESS: &str = "QUERY_FLIGHT_API_ADDRESS";
pub const QUERY_HTTP_API_ADDRESS: &str = "QUERY_HTTP_API_ADDRESS";
pub const QUERY_METRICS_API_ADDRESS: &str = "QUERY_METRIC_API_ADDRESS";
pub const QUERY_WAIT_TIMEOUT_MILLS: &str = "QUERY_WAIT_TIMEOUT_MILLS";
pub const QUERY_MAX_QUERY_LOG_SIZE: &str = "QUERY_MAX_QUERY_LOG_SIZE";
pub const QUERY_TABLE_CACHE_ENABLED: &str = "QUERY_TABLE_CACHE_ENABLED";
pub const QUERY_TABLE_CACHE_SNAPSHOT_COUNT: &str = "QUERY_TABLE_CACHE_SNAPSHOT_COUNT";
pub const QUERY_TABLE_CACHE_SEGMENT_COUNT: &str = "QUERY_TABLE_CACHE_SEGMENT_COUNT";
pub const QUERY_TABLE_CACHE_BLOCK_META_COUNT: &str = "QUERY_TABLE_CACHE_BLOCK_META_COUNT";
pub const QUERY_TABLE_MEMORY_CACHE_MB_SIZE: &str = "QUERY_TABLE_MEMORY_CACHE_MB_SIZE";
pub const QUERY_TABLE_DISK_CACHE_ROOT: &str = "QUERY_TABLE_DISK_CACHE_ROOT";
pub const QUERY_TABLE_DISK_CACHE_MB_SIZE: &str = "QUERY_TABLE_DISK_CACHE_MB_SIZE";

const QUERY_HTTP_HANDLER_TLS_SERVER_CERT: &str = "QUERY_HTTP_HANDLER_TLS_SERVER_CERT";
const QUERY_HTTP_HANDLER_TLS_SERVER_KEY: &str = "QUERY_HTTP_HANDLER_TLS_SERVER_KEY";
const QUERY_HTTP_HANDLER_TLS_SERVER_ROOT_CA_CERT: &str =
    "QUERY_HTTP_HANDLER_TLS_SERVER_ROOT_CA_CERT";

const QUERY_API_TLS_SERVER_CERT: &str = "QUERY_API_TLS_SERVER_CERT";
const QUERY_API_TLS_SERVER_KEY: &str = "QUERY_API_TLS_SERVER_KEY";
const QUERY_API_TLS_SERVER_ROOT_CA_CERT: &str = "QUERY_API_TLS_SERVER_ROOT_CA_CERT";

const QUERY_RPC_TLS_SERVER_CERT: &str = "QUERY_RPC_TLS_SERVER_CERT";
const QUERY_RPC_TLS_SERVER_KEY: &str = "QUERY_RPC_TLS_SERVER_KEY";
const QUERY_RPC_TLS_SERVER_ROOT_CA_CERT: &str = "QUERY_RPC_TLS_SERVER_ROOT_CA_CERT";
const QUERY_RPC_TLS_SERVICE_DOMAIN_NAME: &str = "QUERY_RPC_TLS_SERVICE_DOMAIN_NAME";

const QUERY_TABLE_ENGINE_CSV_ENABLED: &str = "QUERY_TABLE_ENGINE_CSV_ENABLED";
const QUERY_TABLE_ENGINE_PARQUET_ENABLED: &str = "QUERY_TABLE_ENGINE_PARQUET_ENABLED";
const QUERY_TABLE_ENGINE_MEMORY_ENABLED: &str = "QUERY_TABLE_ENGINE_MEMORY_ENABLED";
const QUERY_DATABASE_ENGINE_GITHUB_ENABLED: &str = "QUERY_DATABASE_ENGINE_GITHUB_ENABLED";

const QUERY_MANAGEMENT_MODE: &str = "QUERY_MANAGEMENT_MODE";
const QUERY_JWT_KEY_FILE: &str = "QUERY_JWT_KEY_FILE";

/// Query config group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    #[clap(long, env = QUERY_TENANT_ID, default_value = "admin")]
    pub tenant_id: String,

    /// ID for construct the cluster.
    #[clap(long, env = QUERY_CLUSTER_ID, default_value = "")]
    pub cluster_id: String,

    #[clap(long, env = QUERY_NUM_CPUS, default_value = "0")]
    pub num_cpus: u64,

    #[clap(long, env = QUERY_MYSQL_HANDLER_HOST, default_value = "127.0.0.1")]
    pub mysql_handler_host: String,

    #[clap(long, env = QUERY_MYSQL_HANDLER_PORT, default_value = "3307")]
    pub mysql_handler_port: u16,

    #[clap(long, env = QUERY_MAX_ACTIVE_SESSIONS, default_value = "256")]
    pub max_active_sessions: u64,

    #[clap(long, env = QUERY_CLICKHOUSE_HANDLER_HOST, default_value = "127.0.0.1")]
    pub clickhouse_handler_host: String,

    #[clap(long, env = QUERY_CLICKHOUSE_HANDLER_PORT, default_value = "9000")]
    pub clickhouse_handler_port: u16,

    #[clap(long, env = QUERY_HTTP_HANDLER_HOST, default_value = "127.0.0.1")]
    pub http_handler_host: String,

    #[clap(long, env = QUERY_HTTP_HANDLER_PORT, default_value = "8000")]
    pub http_handler_port: u16,

    #[clap(long, env = QUERY_HTTP_HANDLER_RESULT_TIMEOUT_MILLIS, default_value = "10000")]
    pub http_handler_result_timeout_millis: u64,

    #[clap(long, env = QUERY_FLIGHT_API_ADDRESS, default_value = "127.0.0.1:9090")]
    pub flight_api_address: String,

    #[clap(long, env = QUERY_HTTP_API_ADDRESS, default_value = "127.0.0.1:8080")]
    pub http_api_address: String,

    #[clap(long,env = QUERY_METRICS_API_ADDRESS, default_value = "127.0.0.1:7070")]
    pub metric_api_address: String,

    #[clap(long, env = QUERY_HTTP_HANDLER_TLS_SERVER_CERT, default_value = "")]
    pub http_handler_tls_server_cert: String,

    #[clap(long, env = QUERY_HTTP_HANDLER_TLS_SERVER_KEY, default_value = "")]
    pub http_handler_tls_server_key: String,

    #[clap(long, env = QUERY_HTTP_HANDLER_TLS_SERVER_ROOT_CA_CERT, default_value = "")]
    pub http_handler_tls_server_root_ca_cert: String,

    #[clap(long, env = QUERY_API_TLS_SERVER_CERT, default_value = "")]
    pub api_tls_server_cert: String,

    #[clap(long, env = QUERY_API_TLS_SERVER_KEY, default_value = "")]
    pub api_tls_server_key: String,

    #[clap(long, env = QUERY_API_TLS_SERVER_ROOT_CA_CERT, default_value = "")]
    pub api_tls_server_root_ca_cert: String,

    /// rpc server cert
    #[clap(long, env = "QUERY_RPC_TLS_SERVER_CERT", default_value = "")]
    pub rpc_tls_server_cert: String,

    /// key for rpc server cert
    #[clap(long, env = "QUERY_RPC_TLS_SERVER_KEY", default_value = "")]
    pub rpc_tls_server_key: String,

    /// Certificate for client to identify query rpc server
    #[clap(long, env = "QUERY_RPC_TLS_SERVER_ROOT_CA_CERT", default_value = "")]
    pub rpc_tls_query_server_root_ca_cert: String,

    #[clap(
        long,
        env = "QUERY_RPC_TLS_SERVICE_DOMAIN_NAME",
        default_value = "localhost"
    )]
    pub rpc_tls_query_service_domain_name: String,

    /// Table engine csv enabled
    #[clap(long, env = QUERY_TABLE_ENGINE_CSV_ENABLED)]
    pub table_engine_csv_enabled: bool,

    /// Table engine parquet enabled
    #[clap(long, env = QUERY_TABLE_ENGINE_PARQUET_ENABLED)]
    pub table_engine_parquet_enabled: bool,

    /// Table engine memory enabled
    #[clap(
        long,
        env = QUERY_TABLE_ENGINE_MEMORY_ENABLED,
        parse(try_from_str),
        default_value = "true",
    )]
    pub table_engine_memory_enabled: bool,

    /// Database engine github enabled
    #[clap(
        long,
        env = QUERY_DATABASE_ENGINE_GITHUB_ENABLED,
        parse(try_from_str),
        default_value = "true",
    )]
    pub database_engine_github_enabled: bool,

    #[clap(long, env = QUERY_WAIT_TIMEOUT_MILLS, default_value = "5000")]
    pub wait_timeout_mills: u64,

    #[clap(long, env = QUERY_MAX_QUERY_LOG_SIZE, default_value = "10000")]
    pub max_query_log_size: usize,

    /// Table Cached enabled
    #[clap(long, env = QUERY_TABLE_CACHE_ENABLED)]
    pub table_cache_enabled: bool,

    /// Max number of cached table snapshot  
    #[clap(long, env = QUERY_TABLE_CACHE_SNAPSHOT_COUNT, default_value = "256")]
    pub table_cache_snapshot_count: u64,

    /// Max number of cached table segment
    #[clap(long, env = QUERY_TABLE_CACHE_SEGMENT_COUNT, default_value = "10240")]
    pub table_cache_segment_count: u64,

    /// Max number of cached table block meta
    #[clap(long, env = QUERY_TABLE_CACHE_BLOCK_META_COUNT, default_value = "102400")]
    pub table_cache_block_meta_count: u64,

    /// Table memory cache size (mb)
    #[clap(long, env = QUERY_TABLE_MEMORY_CACHE_MB_SIZE, default_value = "256")]
    pub table_memory_cache_mb_size: u64,

    /// Table disk cache folder root
    #[clap(long, env = QUERY_TABLE_DISK_CACHE_ROOT, default_value = "_cache")]
    pub table_disk_cache_root: String,

    /// Table disk cache size (mb)
    #[clap(long, env = QUERY_TABLE_DISK_CACHE_MB_SIZE, default_value = "1024")]
    pub table_disk_cache_mb_size: u64,

    /// If in management mode, only can do some meta level operations(database/table/user/stage etc.) with metasrv.
    #[clap(long, env = QUERY_MANAGEMENT_MODE)]
    pub management_mode: bool,

    #[clap(long, env = QUERY_JWT_KEY_FILE, default_value = "")]
    pub jwt_key_file: String,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            tenant_id: "".to_string(),
            cluster_id: "".to_string(),
            num_cpus: 8,
            mysql_handler_host: "127.0.0.1".to_string(),
            mysql_handler_port: 3307,
            max_active_sessions: 256,
            clickhouse_handler_host: "127.0.0.1".to_string(),
            clickhouse_handler_port: 9000,
            http_handler_host: "127.0.0.1".to_string(),
            http_handler_port: 8000,
            http_handler_result_timeout_millis: 10000,
            flight_api_address: "127.0.0.1:9090".to_string(),
            http_api_address: "127.0.0.1:8080".to_string(),
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

impl QueryConfig {
    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(mut_config, query, tenant_id, String, QUERY_TENANT_ID);
        env_helper!(mut_config, query, cluster_id, String, QUERY_CLUSTER_ID);
        env_helper!(mut_config, query, num_cpus, u64, QUERY_NUM_CPUS);
        env_helper!(
            mut_config,
            query,
            mysql_handler_host,
            String,
            QUERY_MYSQL_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            query,
            mysql_handler_port,
            u16,
            QUERY_MYSQL_HANDLER_PORT
        );
        env_helper!(
            mut_config,
            query,
            max_active_sessions,
            u64,
            QUERY_MAX_ACTIVE_SESSIONS
        );
        env_helper!(
            mut_config,
            query,
            clickhouse_handler_host,
            String,
            QUERY_CLICKHOUSE_HANDLER_HOST
        );
        env_helper!(
            mut_config,
            query,
            clickhouse_handler_port,
            u16,
            QUERY_CLICKHOUSE_HANDLER_PORT
        );
        env_helper!(
            mut_config,
            query,
            flight_api_address,
            String,
            QUERY_FLIGHT_API_ADDRESS
        );
        env_helper!(
            mut_config,
            query,
            http_api_address,
            String,
            QUERY_HTTP_API_ADDRESS
        );
        env_helper!(
            mut_config,
            query,
            metric_api_address,
            String,
            QUERY_METRICS_API_ADDRESS
        );

        // for api http service
        env_helper!(
            mut_config,
            query,
            api_tls_server_cert,
            String,
            QUERY_API_TLS_SERVER_CERT
        );

        env_helper!(
            mut_config,
            query,
            api_tls_server_key,
            String,
            QUERY_API_TLS_SERVER_KEY
        );

        // for http handler service
        env_helper!(
            mut_config,
            query,
            http_handler_tls_server_cert,
            String,
            QUERY_HTTP_HANDLER_TLS_SERVER_CERT
        );

        env_helper!(
            mut_config,
            query,
            http_handler_tls_server_key,
            String,
            QUERY_HTTP_HANDLER_TLS_SERVER_KEY
        );

        env_helper!(
            mut_config,
            query,
            http_handler_tls_server_root_ca_cert,
            String,
            QUERY_HTTP_HANDLER_TLS_SERVER_ROOT_CA_CERT
        );

        env_helper!(
            mut_config,
            query,
            http_handler_result_timeout_millis,
            u64,
            QUERY_HTTP_HANDLER_RESULT_TIMEOUT_MILLIS
        );

        // for query rpc server
        env_helper!(
            mut_config,
            query,
            rpc_tls_server_cert,
            String,
            QUERY_RPC_TLS_SERVER_CERT
        );

        env_helper!(
            mut_config,
            query,
            rpc_tls_server_key,
            String,
            QUERY_RPC_TLS_SERVER_KEY
        );

        // for query rpc client
        env_helper!(
            mut_config,
            query,
            rpc_tls_query_server_root_ca_cert,
            String,
            QUERY_RPC_TLS_SERVER_ROOT_CA_CERT
        );
        env_helper!(
            mut_config,
            query,
            rpc_tls_query_service_domain_name,
            String,
            QUERY_RPC_TLS_SERVICE_DOMAIN_NAME
        );
        env_helper!(
            mut_config,
            query,
            wait_timeout_mills,
            u64,
            QUERY_WAIT_TIMEOUT_MILLS
        );
        env_helper!(
            mut_config,
            query,
            max_query_log_size,
            usize,
            QUERY_MAX_QUERY_LOG_SIZE
        );
        env_helper!(
            mut_config,
            query,
            table_engine_csv_enabled,
            bool,
            QUERY_TABLE_ENGINE_CSV_ENABLED
        );
        env_helper!(
            mut_config,
            query,
            table_engine_parquet_enabled,
            bool,
            QUERY_TABLE_ENGINE_PARQUET_ENABLED
        );
        env_helper!(
            mut_config,
            query,
            table_engine_memory_enabled,
            bool,
            QUERY_TABLE_ENGINE_MEMORY_ENABLED
        );
        env_helper!(
            mut_config,
            query,
            database_engine_github_enabled,
            bool,
            QUERY_DATABASE_ENGINE_GITHUB_ENABLED
        );
        env_helper!(
            mut_config,
            query,
            table_cache_enabled,
            bool,
            QUERY_TABLE_CACHE_ENABLED
        );
        env_helper!(
            mut_config,
            query,
            table_cache_snapshot_count,
            u64,
            QUERY_TABLE_CACHE_SNAPSHOT_COUNT
        );
        env_helper!(
            mut_config,
            query,
            table_cache_segment_count,
            u64,
            QUERY_TABLE_CACHE_SEGMENT_COUNT
        );
        env_helper!(
            mut_config,
            query,
            table_cache_block_meta_count,
            u64,
            QUERY_TABLE_CACHE_BLOCK_META_COUNT
        );
        env_helper!(
            mut_config,
            query,
            table_memory_cache_mb_size,
            u64,
            QUERY_TABLE_MEMORY_CACHE_MB_SIZE
        );
        env_helper!(
            mut_config,
            query,
            table_disk_cache_root,
            String,
            QUERY_TABLE_DISK_CACHE_ROOT
        );
        env_helper!(
            mut_config,
            query,
            table_disk_cache_mb_size,
            u64,
            QUERY_TABLE_DISK_CACHE_MB_SIZE
        );
        env_helper!(
            mut_config,
            query,
            management_mode,
            bool,
            QUERY_MANAGEMENT_MODE
        );
        env_helper!(mut_config, query, management_mode, bool, QUERY_JWT_KEY_FILE);
    }
}
