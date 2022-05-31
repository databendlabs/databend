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
use std::env;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use clap::Args;
use clap::Parser;
use common_base::base::mask_string;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::StorageAzblobConfig as InnerStorageAzblobConfig;
use common_io::prelude::StorageConfig as InnerStorageConfig;
use common_io::prelude::StorageFsConfig as InnerStorageFsConfig;
use common_io::prelude::StorageHdfsConfig as InnerStorageHdfsConfig;
use common_io::prelude::StorageParams;
use common_io::prelude::StorageS3Config as InnerStorageS3Config;
use common_tracing::Config as InnerLogConfig;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;

use super::inner::Config as InnerConfig;
use super::inner::HiveCatalogConfig as InnerHiveCatalogConfig;
use super::inner::MetaConfig as InnerMetaConfig;
use super::inner::QueryConfig as InnerQueryConfig;

/// Outer config for `query`.
///
/// We will use this config to handle
///
/// - Args parse
/// - Env loading
/// - Config files serialize and deserialize
///
/// It's forbidden to do any breaking changes on this struct.
/// Only adding new fields is allowed.
/// This same rules should be applied to all fields of this struct.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Parser)]
#[clap(about, version, author)]
#[serde(default)]
pub struct Config {
    #[clap(long, short = 'c', default_value_t)]
    pub config_file: String,

    // Query engine config.
    #[clap(flatten)]
    pub query: QueryConfig,

    #[clap(flatten)]
    pub log: LogConfig,

    // Meta Service config.
    #[clap(flatten)]
    pub meta: MetaConfig,

    // Storage backend config.
    #[clap(flatten)]
    pub storage: StorageConfig,

    // external catalog config.
    // - Later, catalog information SHOULD be kept in KV Service
    // - currently only supports HIVE (via hive meta store)
    #[clap(flatten)]
    pub catalog: HiveCatalogConfig,
}

impl Default for Config {
    fn default() -> Self {
        InnerConfig::default().into_outer()
    }
}

impl Config {
    /// Load will load config from file, env and args.
    ///
    /// - Load from file as default.
    /// - Load from env, will override config from file.
    /// - Load from args as finally override
    pub fn load() -> Result<Self> {
        let arg_conf = Self::parse();

        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // Load from config file first.
        {
            let config_file = if !arg_conf.config_file.is_empty() {
                arg_conf.config_file.clone()
            } else if let Ok(path) = env::var("CONFIG_FILE") {
                path
            } else {
                "".to_string()
            };

            builder = builder.collect(from_file(Toml, &config_file));
        }

        // Then, load from env.
        builder = builder.collect(from_env());

        // Finally, load from args.
        builder = builder.collect(from_self(arg_conf));

        Ok(builder.build()?)
    }
}

impl From<InnerConfig> for Config {
    fn from(inner: InnerConfig) -> Self {
        Self {
            config_file: inner.config_file,
            query: inner.query.into(),
            log: inner.log.into(),
            meta: inner.meta.into(),
            storage: inner.storage.into(),
            catalog: inner.catalog.into(),
        }
    }
}

impl TryInto<InnerConfig> for Config {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerConfig> {
        Ok(InnerConfig {
            config_file: self.config_file,
            query: self.query.try_into()?,
            log: self.log.try_into()?,
            meta: self.meta.try_into()?,
            storage: self.storage.try_into()?,
            catalog: self.catalog.try_into()?,
        })
    }
}

/// Storage config group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StorageConfig {
    /// Current storage type: fs|s3
    #[clap(long, default_value = "fs")]
    #[serde(rename = "type", alias = "storage_type")]
    pub storage_type: String,

    #[clap(long, default_value_t)]
    #[serde(rename = "num_cpus", alias = "storage_num_cpus")]
    pub storage_num_cpus: u64,

    // Fs storage backend config.
    #[clap(flatten)]
    pub fs: FsStorageConfig,

    // S3 storage backend config.
    #[clap(flatten)]
    pub s3: S3StorageConfig,

    // azure storage blob config.
    #[clap(flatten)]
    pub azblob: AzblobStorageConfig,

    // hdfs storage backend config
    #[clap(flatten)]
    pub hdfs: HdfsConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        InnerStorageConfig::default().into()
    }
}

impl From<InnerStorageConfig> for StorageConfig {
    fn from(inner: InnerStorageConfig) -> Self {
        let mut cfg = Self {
            storage_num_cpus: inner.num_cpus,
            storage_type: "".to_string(),
            fs: Default::default(),
            s3: Default::default(),
            azblob: Default::default(),
            hdfs: Default::default(),
        };

        match inner.params {
            StorageParams::Azblob(v) => {
                cfg.storage_type = "azblob".to_string();
                cfg.azblob = v.into();
            }
            StorageParams::Fs(v) => {
                cfg.storage_type = "fs".to_string();
                cfg.fs = v.into();
            }
            #[cfg(feature = "storage-hdfs")]
            StorageParams::Hdfs(v) => {
                cfg.storage_type = "hdfs".to_string();
                cfg.hdfs = v.into();
            }
            StorageParams::Memory => {
                cfg.storage_type = "memory".to_string();
            }
            StorageParams::S3(v) => {
                cfg.storage_type = "s3".to_string();
                cfg.s3 = v.into()
            }
        }

        cfg
    }
}

impl TryInto<InnerStorageConfig> for StorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageConfig> {
        Ok(InnerStorageConfig {
            num_cpus: self.storage_num_cpus,
            params: {
                match self.storage_type.as_str() {
                    "azblob" => StorageParams::Azblob(self.azblob.try_into()?),
                    "fs" => StorageParams::Fs(self.fs.try_into()?),
                    #[cfg(feature = "storage-hdfs")]
                    "hdfs" => StorageParams::Hdfs(self.hdfs.try_into()?),
                    "memory" => StorageParams::Memory,
                    "s3" => StorageParams::S3(self.s3.try_into()?),
                    _ => return Err(ErrorCode::StorageOther("not supported storage type")),
                }
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FsStorageConfig {
    /// fs storage backend data path
    #[clap(long = "storage-fs-data-path", default_value = "_data")]
    pub data_path: String,
}

impl Default for FsStorageConfig {
    fn default() -> Self {
        InnerStorageFsConfig::default().into()
    }
}

impl From<InnerStorageFsConfig> for FsStorageConfig {
    fn from(inner: InnerStorageFsConfig) -> Self {
        Self {
            data_path: inner.root,
        }
    }
}

impl TryInto<InnerStorageFsConfig> for FsStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageFsConfig> {
        Ok(InnerStorageFsConfig {
            root: self.data_path,
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Eq, Args)]
#[serde(default)]
pub struct S3StorageConfig {
    /// Region for S3 storage
    #[clap(long = "storage-s3-region", default_value_t)]
    pub region: String,

    /// Endpoint URL for S3 storage
    #[clap(
        long = "storage-s3-endpoint-url",
        default_value = "https://s3.amazonaws.com"
    )]
    pub endpoint_url: String,

    /// Access key for S3 storage
    #[clap(long = "storage-s3-access-key-id", default_value_t)]
    pub access_key_id: String,

    /// Secret key for S3 storage
    #[clap(long = "storage-s3-secret-access-key", default_value_t)]
    pub secret_access_key: String,

    /// S3 Bucket to use for storage
    #[clap(long = "storage-s3-bucket", default_value_t)]
    pub bucket: String,

    /// <root>
    #[clap(long = "storage-s3-root", default_value_t)]
    pub root: String,

    // TODO(xuanwo): We should support both AWS SSE and CSE in the future.
    #[clap(long = "storage-s3-master-key", default_value_t)]
    pub master_key: String,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        InnerStorageS3Config::default().into()
    }
}

impl fmt::Debug for S3StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("S3StorageConfig")
            .field("endpoint_url", &self.endpoint_url)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("access_key_id", &mask_string(&self.access_key_id, 3))
            .field(
                "secret_access_key",
                &mask_string(&self.secret_access_key, 3),
            )
            .field("master_key", &mask_string(&self.master_key, 3))
            .finish()
    }
}

impl From<InnerStorageS3Config> for S3StorageConfig {
    fn from(inner: InnerStorageS3Config) -> Self {
        Self {
            region: inner.region,
            endpoint_url: inner.endpoint_url,
            access_key_id: inner.access_key_id,
            secret_access_key: inner.secret_access_key,
            bucket: inner.bucket,
            root: inner.root,
            master_key: inner.master_key,
        }
    }
}

impl TryInto<InnerStorageS3Config> for S3StorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageS3Config> {
        Ok(InnerStorageS3Config {
            endpoint_url: self.endpoint_url,
            region: self.region,
            bucket: self.bucket,
            access_key_id: self.access_key_id,
            secret_access_key: self.secret_access_key,
            master_key: self.master_key,
            root: self.root,
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct AzblobStorageConfig {
    /// Account for Azblob
    #[clap(long = "storage-azblob-account-name", default_value_t)]
    pub account_name: String,

    /// Master key for Azblob
    #[clap(long = "storage-azblob-account-key", default_value_t)]
    pub account_key: String,

    /// Container for Azblob
    #[clap(long = "storage-azblob-container", default_value_t)]
    pub container: String,

    /// Endpoint URL for Azblob
    ///
    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use endpoint_url directly.
    #[clap(long = "storage-azblob-endpoint-url", default_value_t)]
    #[serde(rename = "endpoint_url")]
    pub azblob_endpoint_url: String,

    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use root directly.
    #[clap(long = "storage-azblob-root", default_value_t)]
    #[serde(rename = "root")]
    pub azblob_root: String,
}

impl Default for AzblobStorageConfig {
    fn default() -> Self {
        InnerStorageAzblobConfig::default().into()
    }
}

impl fmt::Debug for AzblobStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AzureStorageBlobConfig")
            .field("endpoint_url", &self.azblob_endpoint_url)
            .field("container", &self.container)
            .field("root", &self.azblob_root)
            .field("account_name", &mask_string(&self.account_name, 3))
            .field("account_key", &mask_string(&self.account_key, 3))
            .finish()
    }
}

impl From<InnerStorageAzblobConfig> for AzblobStorageConfig {
    fn from(inner: InnerStorageAzblobConfig) -> Self {
        Self {
            account_name: inner.account_name,
            account_key: inner.account_key,
            container: inner.container,
            azblob_endpoint_url: inner.endpoint_url,
            azblob_root: inner.root,
        }
    }
}

impl TryInto<InnerStorageAzblobConfig> for AzblobStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageAzblobConfig> {
        Ok(InnerStorageAzblobConfig {
            endpoint_url: self.azblob_endpoint_url,
            container: self.container,
            account_name: self.account_name,
            account_key: self.account_key,
            root: self.azblob_root,
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Args, Debug)]
#[serde(default)]
pub struct HdfsConfig {
    #[clap(long = "storage-hdfs-name-node", default_value_t)]
    pub name_node: String,
    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use root directly.
    #[clap(long = "storage-hdfs-root", default_value_t)]
    #[serde(rename = "root")]
    pub hdfs_root: String,
}

impl Default for HdfsConfig {
    fn default() -> Self {
        InnerStorageHdfsConfig::default().into()
    }
}

impl From<InnerStorageHdfsConfig> for HdfsConfig {
    fn from(inner: InnerStorageHdfsConfig) -> Self {
        Self {
            name_node: inner.name_node,
            hdfs_root: inner.root,
        }
    }
}

impl TryInto<InnerStorageHdfsConfig> for HdfsConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageHdfsConfig> {
        Ok(InnerStorageHdfsConfig {
            name_node: self.name_node,
            root: self.hdfs_root,
        })
    }
}

/// Query config group.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    #[clap(long, default_value = "admin")]
    pub tenant_id: String,

    /// ID for construct the cluster.
    #[clap(long, default_value_t)]
    pub cluster_id: String,

    #[clap(long, default_value_t)]
    pub num_cpus: u64,

    #[clap(long, default_value = "127.0.0.1")]
    pub mysql_handler_host: String,

    #[clap(long, default_value = "3307")]
    pub mysql_handler_port: u16,

    #[clap(long, default_value = "256")]
    pub max_active_sessions: u64,

    #[clap(long, default_value = "127.0.0.1")]
    pub clickhouse_handler_host: String,

    #[clap(long, default_value = "9000")]
    pub clickhouse_handler_port: u16,

    #[clap(long, default_value = "127.0.0.1")]
    pub http_handler_host: String,

    #[clap(long, default_value = "8000")]
    pub http_handler_port: u16,

    #[clap(long, default_value = "10000")]
    pub http_handler_result_timeout_millis: u64,

    #[clap(long, default_value = "127.0.0.1:9090")]
    pub flight_api_address: String,

    #[clap(long, default_value = "127.0.0.1:8080")]
    pub admin_api_address: String,

    #[clap(long, default_value = "127.0.0.1:7070")]
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

    #[clap(long, default_value = "localhost")]
    pub rpc_tls_query_service_domain_name: String,

    /// Table engine memory enabled
    #[clap(long, parse(try_from_str), default_value = "true")]
    pub table_engine_memory_enabled: bool,

    /// Database engine github enabled
    #[clap(long, parse(try_from_str), default_value = "true")]
    pub database_engine_github_enabled: bool,

    #[clap(long, default_value = "5000")]
    pub wait_timeout_mills: u64,

    #[clap(long, default_value = "10000")]
    pub max_query_log_size: usize,

    /// Table Cached enabled
    #[clap(long)]
    pub table_cache_enabled: bool,

    /// Max number of cached table snapshot
    #[clap(long, default_value = "256")]
    pub table_cache_snapshot_count: u64,

    /// Max number of cached table segment
    #[clap(long, default_value = "10240")]
    pub table_cache_segment_count: u64,

    /// Max number of cached table block meta
    #[clap(long, default_value = "102400")]
    pub table_cache_block_meta_count: u64,

    /// Table memory cache size (mb)
    #[clap(long, default_value = "256")]
    pub table_memory_cache_mb_size: u64,

    /// Table disk cache folder root
    #[clap(long, default_value = "_cache")]
    pub table_disk_cache_root: String,

    /// Table disk cache size (mb)
    #[clap(long, default_value = "1024")]
    pub table_disk_cache_mb_size: u64,

    /// If in management mode, only can do some meta level operations(database/table/user/stage etc.) with metasrv.
    #[clap(long)]
    pub management_mode: bool,

    #[clap(long, default_value_t)]
    pub jwt_key_file: String,

    /// TODO(fkuner)
    #[clap(long)]
    pub enable_async_insert: bool,

    /// TODO(fkuner)
    #[clap(long, default_value="10000")]
    pub async_max_data_size: u64,

    /// TODO(fkuner)
    #[clap(long, default_value="200")]
    pub async_insert_busy_timeout: u64,

    /// TODO(fkuner)
    #[clap(long, default_value="0")]
    pub async_insert_stale_timeout: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        InnerQueryConfig::default().into()
    }
}

impl TryInto<InnerQueryConfig> for QueryConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerQueryConfig> {
        Ok(InnerQueryConfig {
            tenant_id: self.tenant_id,
            cluster_id: self.cluster_id,
            num_cpus: self.num_cpus,
            mysql_handler_host: self.mysql_handler_host,
            mysql_handler_port: self.mysql_handler_port,
            max_active_sessions: self.max_active_sessions,
            clickhouse_handler_host: self.clickhouse_handler_host,
            clickhouse_handler_port: self.clickhouse_handler_port,
            http_handler_host: self.http_handler_host,
            http_handler_port: self.http_handler_port,
            http_handler_result_timeout_millis: self.http_handler_result_timeout_millis,
            flight_api_address: self.flight_api_address,
            admin_api_address: self.admin_api_address,
            metric_api_address: self.metric_api_address,
            http_handler_tls_server_cert: self.http_handler_tls_server_cert,
            http_handler_tls_server_key: self.http_handler_tls_server_key,
            http_handler_tls_server_root_ca_cert: self.http_handler_tls_server_root_ca_cert,
            api_tls_server_cert: self.api_tls_server_cert,
            api_tls_server_key: self.api_tls_server_key,
            api_tls_server_root_ca_cert: self.api_tls_server_root_ca_cert,
            rpc_tls_server_cert: self.rpc_tls_server_cert,
            rpc_tls_server_key: self.rpc_tls_server_key,
            rpc_tls_query_server_root_ca_cert: self.rpc_tls_query_server_root_ca_cert,
            rpc_tls_query_service_domain_name: self.rpc_tls_query_service_domain_name,
            table_engine_memory_enabled: self.table_engine_memory_enabled,
            database_engine_github_enabled: self.database_engine_github_enabled,
            wait_timeout_mills: self.wait_timeout_mills,
            max_query_log_size: self.max_query_log_size,
            table_cache_enabled: self.table_cache_enabled,
            table_cache_snapshot_count: self.table_cache_snapshot_count,
            table_cache_segment_count: self.table_cache_segment_count,
            table_cache_block_meta_count: self.table_cache_block_meta_count,
            table_memory_cache_mb_size: self.table_memory_cache_mb_size,
            table_disk_cache_root: self.table_disk_cache_root,
            table_disk_cache_mb_size: self.table_disk_cache_mb_size,
            management_mode: self.management_mode,
            jwt_key_file: self.jwt_key_file,
            enable_async_insert: self.enable_async_insert,
            async_max_data_size: self.async_max_data_size,
            async_insert_busy_timeout: self.async_insert_busy_timeout,
            async_insert_stale_timeout: self.async_insert_stale_timeout,
        })
    }
}

impl From<InnerQueryConfig> for QueryConfig {
    fn from(inner: InnerQueryConfig) -> Self {
        Self {
            tenant_id: inner.tenant_id,
            cluster_id: inner.cluster_id,
            num_cpus: inner.num_cpus,
            mysql_handler_host: inner.mysql_handler_host,
            mysql_handler_port: inner.mysql_handler_port,
            max_active_sessions: inner.max_active_sessions,
            clickhouse_handler_host: inner.clickhouse_handler_host,
            clickhouse_handler_port: inner.clickhouse_handler_port,
            http_handler_host: inner.http_handler_host,
            http_handler_port: inner.http_handler_port,
            http_handler_result_timeout_millis: inner.http_handler_result_timeout_millis,
            flight_api_address: inner.flight_api_address,
            admin_api_address: inner.admin_api_address,
            metric_api_address: inner.metric_api_address,
            http_handler_tls_server_cert: inner.http_handler_tls_server_cert,
            http_handler_tls_server_key: inner.http_handler_tls_server_key,
            http_handler_tls_server_root_ca_cert: inner.http_handler_tls_server_root_ca_cert,
            api_tls_server_cert: inner.api_tls_server_cert,
            api_tls_server_key: inner.api_tls_server_key,
            api_tls_server_root_ca_cert: inner.api_tls_server_root_ca_cert,
            rpc_tls_server_cert: inner.rpc_tls_server_cert,
            rpc_tls_server_key: inner.rpc_tls_server_key,
            rpc_tls_query_server_root_ca_cert: inner.rpc_tls_query_server_root_ca_cert,
            rpc_tls_query_service_domain_name: inner.rpc_tls_query_service_domain_name,
            table_engine_memory_enabled: inner.table_engine_memory_enabled,
            database_engine_github_enabled: inner.database_engine_github_enabled,
            wait_timeout_mills: inner.wait_timeout_mills,
            max_query_log_size: inner.max_query_log_size,
            table_cache_enabled: inner.table_cache_enabled,
            table_cache_snapshot_count: inner.table_cache_snapshot_count,
            table_cache_segment_count: inner.table_cache_segment_count,
            table_cache_block_meta_count: inner.table_cache_block_meta_count,
            table_memory_cache_mb_size: inner.table_memory_cache_mb_size,
            table_disk_cache_root: inner.table_disk_cache_root,
            table_disk_cache_mb_size: inner.table_disk_cache_mb_size,
            management_mode: inner.management_mode,
            jwt_key_file: inner.jwt_key_file,
            enable_async_insert: inner.enable_async_insert,
            async_max_data_size: inner.async_max_data_size,
            async_insert_busy_timeout: inner.async_insert_busy_timeout,
            async_insert_stale_timeout: inner.async_insert_stale_timeout,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct LogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-level", default_value = "INFO")]
    #[serde(alias = "log_level")]
    pub level: String,

    /// Log file dir
    #[clap(long = "log-dir", default_value = "./.databend/logs")]
    #[serde(alias = "log_dir")]
    pub dir: String,

    /// Log file dir
    #[clap(long = "log-query-enabled")]
    #[serde(alias = "log_query_enabled")]
    pub query_enabled: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        InnerLogConfig::default().into()
    }
}

impl TryInto<InnerLogConfig> for LogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerLogConfig> {
        Ok(InnerLogConfig {
            level: self.level,
            dir: self.dir,
            query_enabled: self.query_enabled,
        })
    }
}

impl From<InnerLogConfig> for LogConfig {
    fn from(inner: InnerLogConfig) -> Self {
        Self {
            level: inner.level,
            dir: inner.dir,
            query_enabled: inner.query_enabled,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct HiveCatalogConfig {
    #[clap(long = "hive-meta-store-address", default_value = "127.0.0.1:9083")]
    pub meta_store_address: String,
    #[clap(long = "hive-thrift-protocol", default_value = "binary")]
    pub protocol: String,
}

impl Default for HiveCatalogConfig {
    fn default() -> Self {
        InnerHiveCatalogConfig::default().into()
    }
}

impl TryInto<InnerHiveCatalogConfig> for HiveCatalogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerHiveCatalogConfig> {
        Ok(InnerHiveCatalogConfig {
            meta_store_address: self.meta_store_address,
            protocol: self.protocol.parse()?,
        })
    }
}

impl From<InnerHiveCatalogConfig> for HiveCatalogConfig {
    fn from(inner: InnerHiveCatalogConfig) -> Self {
        Self {
            meta_store_address: inner.meta_store_address,
            protocol: inner.protocol.to_string(),
        }
    }
}

/// Meta config group.
/// TODO(xuanwo): All meta_xxx should be rename to xxx.
#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    #[clap(
        long = "meta-embedded-dir",
        default_value = "./.databend/meta_embedded"
    )]
    #[serde(alias = "meta_embedded_dir")]
    pub embedded_dir: String,

    /// MetaStore backend address
    #[clap(long = "meta-address", default_value_t)]
    #[serde(alias = "meta_address")]
    pub address: String,

    #[clap(long = "meta-endpoints", help = "MetaStore peers endpoints")]
    pub endpoints: Vec<String>,

    /// MetaStore backend user name
    #[clap(long = "meta-username", default_value = "root")]
    #[serde(alias = "meta_username")]
    pub username: String,

    /// MetaStore backend user password
    #[clap(long = "meta-password", default_value_t)]
    #[serde(alias = "meta_password")]
    pub password: String,

    /// Timeout for each client request, in seconds
    #[clap(long = "meta-client-timeout-in-second", default_value = "10")]
    #[serde(alias = "meta_client_timeout_in_second")]
    pub client_timeout_in_second: u64,

    /// Certificate for client to identify meta rpc serve
    #[clap(long = "meta-rpc-tls-meta-server-root-ca-cert", default_value_t)]
    pub rpc_tls_meta_server_root_ca_cert: String,

    #[clap(long = "meta-rpc-tls-meta-service-domain-name", default_value_t)]
    pub rpc_tls_meta_service_domain_name: String,
}

impl Default for MetaConfig {
    fn default() -> Self {
        InnerMetaConfig::default().into()
    }
}

impl TryInto<InnerMetaConfig> for MetaConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerMetaConfig> {
        Ok(InnerMetaConfig {
            embedded_dir: self.embedded_dir,
            address: self.address,
            endpoints: self.endpoints,
            username: self.username,
            password: self.password,
            client_timeout_in_second: self.client_timeout_in_second,
            rpc_tls_meta_server_root_ca_cert: self.rpc_tls_meta_server_root_ca_cert,
            rpc_tls_meta_service_domain_name: self.rpc_tls_meta_service_domain_name,
        })
    }
}

impl From<InnerMetaConfig> for MetaConfig {
    fn from(inner: InnerMetaConfig) -> Self {
        Self {
            embedded_dir: inner.embedded_dir,
            address: inner.address,
            endpoints: inner.endpoints,
            username: inner.username,
            password: inner.password,
            client_timeout_in_second: inner.client_timeout_in_second,
            rpc_tls_meta_server_root_ca_cert: inner.rpc_tls_meta_server_root_ca_cert,
            rpc_tls_meta_service_domain_name: inner.rpc_tls_meta_service_domain_name,
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
