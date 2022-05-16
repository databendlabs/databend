// Copyright 2022 Datafuse Labs.
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

use common_configs::HiveCatalogConfig;
use common_configs::LogConfig;
use common_configs::MetaConfig;
use common_exception::Result;
use common_grpc::RpcClientTlsConfig;
use common_io::prelude::StorageConfig;

use super::outer_v0::Config as OuterV0Config;

/// Inner config for query.
///
/// All function should implemented based on this Config.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Config {
    pub config_file: String,

    // Query engine config.
    pub query: QueryConfig,

    pub log: LogConfig,

    // Meta Service config.
    pub meta: MetaConfig,

    // Storage backend config.
    pub storage: StorageConfig,

    // external catalog config.
    // - Later, catalog information SHOULD be kept in KV Service
    // - currently only supports HIVE (via hive meta store)
    pub catalog: HiveCatalogConfig,
}

impl Config {
    /// As requires by [RFC: Config Backward Compatibility](https://github.com/datafuselabs/databend/pull/5324), we will load user's config via wrapper [`ConfigV0`] and than convert from [`ConfigV0`] to [`Config`].
    ///
    /// In the future, we could have `ConfigV1` and `ConfigV2`.
    pub fn load() -> Result<Self> {
        let cfg = OuterV0Config::load()?.try_into()?;

        Ok(cfg)
    }

    pub fn tls_query_cli_enabled(&self) -> bool {
        !self.query.rpc_tls_query_server_root_ca_cert.is_empty()
            && !self.query.rpc_tls_query_service_domain_name.is_empty()
    }

    pub fn tls_meta_cli_enabled(&self) -> bool {
        !self.meta.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.meta.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.query.rpc_tls_server_key.is_empty() && !self.query.rpc_tls_server_cert.is_empty()
    }

    /// Transform config into the outer style.
    ///
    /// This function should only be used for end-users.
    ///
    /// For examples:
    ///
    /// - system config table
    /// - HTTP Handler
    /// - tests
    pub fn into_outer(self) -> OuterV0Config {
        OuterV0Config::from(self)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    pub tenant_id: String,

    /// ID for construct the cluster.
    pub cluster_id: String,

    pub num_cpus: u64,

    pub mysql_handler_host: String,

    pub mysql_handler_port: u16,

    pub max_active_sessions: u64,

    pub clickhouse_handler_host: String,

    pub clickhouse_handler_port: u16,

    pub http_handler_host: String,

    pub http_handler_port: u16,

    pub http_handler_result_timeout_millis: u64,

    pub flight_api_address: String,

    pub admin_api_address: String,

    pub metric_api_address: String,

    pub http_handler_tls_server_cert: String,

    pub http_handler_tls_server_key: String,

    pub http_handler_tls_server_root_ca_cert: String,

    pub api_tls_server_cert: String,

    pub api_tls_server_key: String,

    pub api_tls_server_root_ca_cert: String,

    /// rpc server cert
    pub rpc_tls_server_cert: String,

    /// key for rpc server cert
    pub rpc_tls_server_key: String,

    /// Certificate for client to identify query rpc server
    pub rpc_tls_query_server_root_ca_cert: String,

    pub rpc_tls_query_service_domain_name: String,

    /// Table engine memory enabled
    pub table_engine_memory_enabled: bool,

    /// Database engine github enabled
    pub database_engine_github_enabled: bool,

    pub wait_timeout_mills: u64,

    pub max_query_log_size: usize,

    /// Table Cached enabled
    pub table_cache_enabled: bool,

    /// Max number of cached table snapshot
    pub table_cache_snapshot_count: u64,

    /// Max number of cached table segment
    pub table_cache_segment_count: u64,

    /// Max number of cached table block meta
    pub table_cache_block_meta_count: u64,

    /// Table memory cache size (mb)
    pub table_memory_cache_mb_size: u64,

    /// Table disk cache folder root
    pub table_disk_cache_root: String,

    /// Table disk cache size (mb)
    pub table_disk_cache_mb_size: u64,

    /// If in management mode, only can do some meta level operations(database/table/user/stage etc.) with metasrv.
    pub management_mode: bool,

    pub jwt_key_file: String,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            tenant_id: "admin".to_string(),
            cluster_id: "".to_string(),
            num_cpus: 0,
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
    pub fn to_rpc_client_tls_config(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.rpc_tls_query_server_root_ca_cert.clone(),
            domain_name: self.rpc_tls_query_service_domain_name.clone(),
        }
    }
}
