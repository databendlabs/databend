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

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::time::Duration;

use common_base::base::mask_string;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::RpcClientConf;
use common_grpc::RpcClientTlsConfig;
use common_storage::StorageConfig;
use common_tracing::Config as LogConfig;

use super::outer_v0::Config as OuterV0Config;

/// Inner config for query.
///
/// All function should implement based on this Config.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Config {
    pub cmd: String,
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
    /// As requires by [RFC: Config Backward Compatibility](https://github.com/datafuselabs/databend/pull/5324), we will load user's config via wrapper [`ConfigV0`] and then convert from [`ConfigV0`] to [`Config`].
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    pub tenant_id: String,
    /// ID for construct the cluster.
    pub cluster_id: String,
    pub num_cpus: u64,
    pub mysql_handler_host: String,
    pub mysql_handler_port: u16,
    pub max_active_sessions: u64,
    pub clickhouse_http_handler_host: String,
    pub clickhouse_http_handler_port: u16,
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
    pub async_insert_max_data_size: u64,
    pub async_insert_busy_timeout: u64,
    pub async_insert_stale_timeout: u64,
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
            clickhouse_http_handler_host: "127.0.0.1".to_string(),
            clickhouse_http_handler_port: 8124,
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
            async_insert_max_data_size: 10000,
            async_insert_busy_timeout: 200,
            async_insert_stale_timeout: 0,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThriftProtocol {
    Binary,
    // Compact,
}

impl FromStr for ThriftProtocol {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<ThriftProtocol> {
        let s = s.to_lowercase();
        match s.as_str() {
            "binary" => Ok(ThriftProtocol::Binary),
            _ => Err(ErrorCode::StorageOther("invalid thrift protocol spec")),
        }
    }
}

impl Display for ThriftProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Binary => write!(f, "binary"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HiveCatalogConfig {
    pub meta_store_address: String,
    pub protocol: ThriftProtocol,
}

impl Default for HiveCatalogConfig {
    fn default() -> Self {
        Self {
            meta_store_address: "127.0.0.1:9083".to_string(),
            protocol: ThriftProtocol::Binary,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    pub embedded_dir: String,
    /// MetaStore backend address
    pub address: String,
    pub endpoints: Vec<String>,
    /// MetaStore backend user name
    pub username: String,
    /// MetaStore backend user password
    pub password: String,
    /// Timeout for each client request, in seconds
    pub client_timeout_in_second: u64,
    /// AutoSyncInterval is the interval to update endpoints with its latest members.
    /// 0 disables auto-sync. By default auto-sync is disabled.
    pub auto_sync_interval: u64,
    /// Certificate for client to identify meta rpc serve
    pub rpc_tls_meta_server_root_ca_cert: String,
    pub rpc_tls_meta_service_domain_name: String,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            embedded_dir: "./.databend/meta_embedded".to_string(),
            address: "".to_string(),
            endpoints: vec![],
            username: "root".to_string(),
            password: "".to_string(),
            client_timeout_in_second: 10,
            auto_sync_interval: 10,
            rpc_tls_meta_server_root_ca_cert: "".to_string(),
            rpc_tls_meta_service_domain_name: "localhost".to_string(),
        }
    }
}

impl MetaConfig {
    pub fn is_tls_enabled(&self) -> bool {
        !self.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn to_rpc_client_tls_config(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.rpc_tls_meta_server_root_ca_cert.to_string(),
            domain_name: self.rpc_tls_meta_service_domain_name.to_string(),
        }
    }

    pub fn to_meta_grpc_client_conf(&self) -> RpcClientConf {
        RpcClientConf {
            address: self.address.clone(),
            endpoints: self.endpoints.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            tls_conf: if self.is_tls_enabled() {
                Some(self.to_rpc_client_tls_config())
            } else {
                None
            },

            timeout: Some(Duration::from_secs(self.client_timeout_in_second)),
            auto_sync_interval: if self.auto_sync_interval > 0 {
                Some(Duration::from_secs(self.auto_sync_interval))
            } else {
                None
            },
        }
    }
}

impl Debug for MetaConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("MetaConfig")
            .field("address", &self.address)
            .field("endpoints", &self.endpoints)
            .field("username", &self.username)
            .field("password", &mask_string(&self.password, 3))
            .field("embedded_dir", &self.embedded_dir)
            .field("client_timeout_in_second", &self.client_timeout_in_second)
            .field("auto_sync_interval", &self.auto_sync_interval)
            .field(
                "rpc_tls_meta_server_root_ca_cert",
                &self.rpc_tls_meta_server_root_ca_cert,
            )
            .field(
                "rpc_tls_meta_service_domain_name",
                &self.rpc_tls_meta_service_domain_name,
            )
            .finish()
    }
}
