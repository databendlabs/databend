// Copyright 2021 Datafuse Labs
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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::OrderedFloat;
use databend_common_base::base::mask_string;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_storage::StorageConfig;
use databend_common_tracing::Config as LogConfig;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::RpcClientConf;
use databend_meta_client::RpcClientTlsConfig;
use serde::Deserialize;
use serde::Serialize;

pub use super::config::CacheConfig;
pub use super::config::CacheStorageTypeConfig;
use super::config::Config;
pub use super::config::DiskCacheConfig;
pub use super::config::DiskCacheKeyReloadPolicy;
pub use super::config::MetaConfig;
use super::config::QueryConfig as OuterQueryConfig;
pub use super::config::TaskConfig;
use super::config::TelemetryConfig;
use crate::BuiltInConfig;

/// Inner config for query.
///
/// All function should implement based on this Config.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct InnerConfig {
    // Query engine config.
    pub query: QueryConfig,

    pub log: LogConfig,

    pub task: TaskConfig,

    // Meta Service config.
    pub meta: MetaConfig,

    // Storage backend config.
    pub storage: StorageConfig,

    // external catalog config.
    // - Later, catalog information SHOULD be kept in KV Service
    // - currently only supports HIVE (via hive meta store)
    pub catalogs: HashMap<String, CatalogConfig>,

    // Cache Config
    pub cache: CacheConfig,

    // Spill Config
    pub spill: SpillConfig,

    // Telemetry Config
    pub telemetry: TelemetryConfig,
}

impl InnerConfig {
    /// As requires by [RFC: Config Backward Compatibility](https://github.com/datafuselabs/databend/pull/5324), we will load user's config via wrapper [`ConfigV0`] and then convert from [`ConfigV0`] to [`InnerConfig`].
    ///
    /// In the future, we could have `ConfigV1` and `ConfigV2`.
    pub async fn init(cfg: Config, check_meta: bool) -> Result<Self> {
        let mut cfg: Self = cfg.try_into()?;

        // Handle the node_id and node_secret for query node.
        cfg.query.node_id = GlobalUniq::unique();
        cfg.query.node_secret = GlobalUniq::unique();

        // Handle auto detect for storage params.
        cfg.storage.params = cfg.storage.params.auto_detect().await?;

        // Set default allow_credential_chain to true for config storage params.
        if let StorageParams::S3(s3) = &mut cfg.storage.params
            && s3.allow_credential_chain.is_none()
        {
            s3.allow_credential_chain = Some(true);
        }
        if let Some(StorageParams::S3(s3)) = &mut cfg.spill.storage_params
            && s3.allow_credential_chain.is_none()
        {
            s3.allow_credential_chain = Some(true);
        }

        if check_meta {
            cfg.meta.check_valid()?;
        }
        Ok(cfg)
    }

    /// # NOTE
    ///
    /// This function is served for tests only.
    pub fn load_for_test() -> Result<Self> {
        Config::load_with_config_file("")?.try_into()
    }

    pub fn tls_query_cli_enabled(&self) -> bool {
        !self
            .query
            .common
            .rpc_tls_query_server_root_ca_cert
            .is_empty()
            && !self
                .query
                .common
                .rpc_tls_query_service_domain_name
                .is_empty()
    }

    pub fn tls_meta_cli_enabled(&self) -> bool {
        !self.meta.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.meta.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn flight_sql_tls_server_enabled(&self) -> bool {
        !self.query.common.flight_sql_tls_server_key.is_empty()
            && !self.query.common.flight_sql_tls_server_cert.is_empty()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.query.common.rpc_tls_server_key.is_empty()
            && !self.query.common.rpc_tls_server_cert.is_empty()
    }

    /// Transform inner::Config into the Config.
    ///
    /// This function should only be used for end-users.
    ///
    /// For examples:
    ///
    /// - system config table
    /// - HTTP Handler
    /// - tests
    pub fn into_config(self) -> Config {
        Config::from(self)
    }
}

impl Debug for InnerConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("InnerConfig")
            .field("query", &self.query.sanitize())
            .field("log", &self.log)
            .field("meta", &self.meta)
            .field("storage", &self.storage)
            .field("catalogs", &self.catalogs)
            .field("cache", &self.cache)
            .field("spill", &self.spill)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    pub tenant_id: Tenant,

    pub common: OuterQueryConfig,

    // ID for the query node.
    // This only initialized when InnerConfig::load().
    pub node_id: String,
    // ID for the query secret key. Every flight request will check it
    // This only initialized when InnerConfig::load().
    pub node_secret: String,
    pub tenant_quota: Option<TenantQuota>,
    pub upgrade_to_pb: bool,
    pub builtin: BuiltInConfig,

    pub settings: HashMap<String, UserSettingValue>,
    pub check_connection_before_schedule: bool,
}

impl Default for QueryConfig {
    fn default() -> Self {
        let common = OuterQueryConfig::default();
        Self {
            tenant_id: Tenant::new_or_err(&common.tenant_id, "default()").unwrap(),
            common,
            node_id: "".to_string(),
            node_secret: "".to_string(),
            builtin: BuiltInConfig::default(),
            tenant_quota: None,
            upgrade_to_pb: false,
            settings: HashMap::new(),
            check_connection_before_schedule: true,
        }
    }
}

impl QueryConfig {
    pub fn to_rpc_client_tls_config(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.common.rpc_tls_query_server_root_ca_cert.clone(),
            domain_name: self.common.rpc_tls_query_service_domain_name.clone(),
        }
    }

    /// Returns TLS config for use with `ConnectionFactory`.
    pub fn to_grpc_tls_config(&self) -> databend_common_grpc::RpcClientTlsConfig {
        databend_common_grpc::RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.common.rpc_tls_query_server_root_ca_cert.clone(),
            domain_name: self.common.rpc_tls_query_service_domain_name.clone(),
        }
    }

    pub fn sanitize(&self) -> Self {
        let mut sanitized = self.clone();
        sanitized.node_secret = mask_string(&self.node_secret, 3);
        sanitized.common.databend_enterprise_license = self
            .common
            .databend_enterprise_license
            .clone()
            .map(|s| mask_string(&s, 3));
        sanitized
    }
}

impl MetaConfig {
    pub fn is_embedded_meta(&self) -> Result<bool> {
        Ok(!self.embedded_dir.is_empty())
    }

    pub fn check_valid(&self) -> Result<()> {
        let has_embedded_dir = !self.embedded_dir.is_empty();
        let has_remote = !self.endpoints.is_empty();
        if has_embedded_dir && has_remote {
            return Err(ErrorCode::InvalidConfig(
                "Can not set embedded_dir and endpoints at the same time, embedded_dir is only for testing, please remove this config".to_string(),
            ));
        }

        if !has_embedded_dir && !has_remote {
            return Err(ErrorCode::InvalidConfig(
                "Please set your meta endpoints config: endpoints = [<your-meta-service-endpoints>]".to_string(),
            ));
        }

        Ok(())
    }

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

    pub fn grpc_max_message_size(&self) -> usize {
        self.grpc_max_message_size
            .unwrap_or(DEFAULT_GRPC_MESSAGE_SIZE)
    }

    pub fn to_meta_grpc_client_conf(&self) -> RpcClientConf {
        let embedded_dir = if self.embedded_dir.is_empty() {
            None
        } else {
            Some(self.embedded_dir.to_string())
        };
        RpcClientConf {
            embedded_dir,
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
            unhealthy_endpoint_evict_time: Duration::from_secs(self.unhealth_endpoint_evict_time),
            grpc_max_message_size: self.grpc_max_message_size(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogConfig {
    #[serde(rename = "type")]
    pub ty: String,

    #[serde(flatten)]
    pub hive: CatalogHiveConfig,
}

impl CatalogConfig {
    pub fn validate(&self) -> Result<()> {
        match self.ty.as_str() {
            "hive" => self.hive.validate(),
            ty => Err(ErrorCode::CatalogNotSupported(format!(
                "got unsupported catalog type in config: {}",
                ty
            ))),
        }
    }
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            ty: "hive".to_string(),
            hive: CatalogHiveConfig::default(),
        }
    }
}

// TODO: add compat protocol support
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThriftProtocol {
    Binary,
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Binary => write!(f, "binary"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CatalogHiveConfig {
    #[serde(rename = "address")]
    pub metastore_address: String,

    /// Deprecated fields, used for catching error, will be removed later.
    pub meta_store_address: Option<String>,

    pub protocol: String,
}

impl CatalogHiveConfig {
    pub fn validate(&self) -> Result<()> {
        if self.meta_store_address.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`meta_store_address` is deprecated, please use `address` instead",
            ));
        }

        // Keep the same behavior as before: validate thrift protocol spec.
        self.protocol.parse::<ThriftProtocol>()?;

        Ok(())
    }
}

impl Default for CatalogHiveConfig {
    fn default() -> Self {
        Self {
            metastore_address: "127.0.0.1:9083".to_string(),
            meta_store_address: None,
            protocol: ThriftProtocol::Binary.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpillConfig {
    pub(crate) local_writeable_root: Option<String>,
    pub(crate) path: String,

    /// Ratio of the reserve of the disk space.
    pub reserved_disk_ratio: OrderedFloat<f64>,

    /// Allow bytes use of disk space.
    pub global_bytes_limit: u64,

    pub storage_params: Option<StorageParams>,

    /// Maximum percentage of the global local spill quota that a single
    /// sort operator may use for one query.
    ///
    /// Value range: 0-100. Effective only when local spill is enabled
    /// (i.e. there is a valid local spill path and non-zero global
    /// bytes limit).
    pub sort_spilling_disk_quota_ratio: u64,

    /// Maximum percentage of the global local spill quota that window
    /// partitioners may use for one query.
    pub window_partition_spilling_disk_quota_ratio: u64,

    /// Maximum percentage of the global local spill quota that HTTP
    /// result-set spilling may use for one query.
    pub result_set_spilling_disk_quota_ratio: u64,
}

impl SpillConfig {
    /// Path of spill to local disk.
    pub fn local_path(&self) -> Option<PathBuf> {
        if self.global_bytes_limit == 0 {
            return None;
        }

        if !self.path.is_empty() {
            return Some(self.path.clone().into());
        }

        if let Some(root) = &self.local_writeable_root {
            return Some(PathBuf::from(root).join("temp/_query_spill"));
        }

        None
    }

    /// Helper to compute a per-query local spill quota (in bytes) from a
    /// percentage of the global local spill limit.
    ///
    /// - If local spill is disabled (no local path or zero global
    ///   limit), returns 0.
    /// - `ratio` is clamped into [0, 100].
    pub fn quota_bytes_from_ratio(&self, ratio: u64) -> usize {
        // Only effective when local spill is enabled.
        if self.local_path().is_none() {
            return 0;
        }

        let ratio = std::cmp::min(ratio, 100);
        if ratio == 0 {
            return 0;
        }

        let bytes = self.global_bytes_limit.saturating_mul(ratio) / 100;

        // TempDirManager works with `usize` limits.
        std::cmp::min(bytes, usize::MAX as u64) as usize
    }

    /// Per-query quota for sort operators.
    pub fn sort_spill_bytes_limit(&self) -> usize {
        self.quota_bytes_from_ratio(self.sort_spilling_disk_quota_ratio)
    }

    /// Per-query quota for window partitioners.
    pub fn window_partition_spill_bytes_limit(&self) -> usize {
        self.quota_bytes_from_ratio(self.window_partition_spilling_disk_quota_ratio)
    }

    /// Per-query quota for HTTP result-set spilling.
    pub fn result_set_spill_bytes_limit(&self) -> usize {
        self.quota_bytes_from_ratio(self.result_set_spilling_disk_quota_ratio)
    }

    pub fn new_for_test(path: String, reserved_disk_ratio: f64, global_bytes_limit: u64) -> Self {
        Self {
            local_writeable_root: None,
            path,
            reserved_disk_ratio: OrderedFloat(reserved_disk_ratio),
            global_bytes_limit,
            storage_params: None,
            // Use the same defaults as the external config.
            sort_spilling_disk_quota_ratio: 60,
            window_partition_spilling_disk_quota_ratio: 60,
            // TODO: keep 0 to avoid deleting local result-set spill dir before HTTP pagination finishes.
            result_set_spilling_disk_quota_ratio: 0,
        }
    }
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            local_writeable_root: None,
            path: "".to_string(),
            reserved_disk_ratio: OrderedFloat(0.1),
            global_bytes_limit: u64::MAX,
            storage_params: None,
            sort_spilling_disk_quota_ratio: 60,
            window_partition_spilling_disk_quota_ratio: 60,
            // TODO: keep 0 to avoid deleting local result-set spill dir before HTTP pagination finishes.
            result_set_spilling_disk_quota_ratio: 0,
        }
    }
}
