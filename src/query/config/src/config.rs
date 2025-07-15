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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use clap::ArgAction;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use databend_common_base::base::mask_string;
use databend_common_base::base::OrderedFloat;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::storage::StorageAzblobConfig as InnerStorageAzblobConfig;
use databend_common_meta_app::storage::StorageCosConfig as InnerStorageCosConfig;
use databend_common_meta_app::storage::StorageFsConfig as InnerStorageFsConfig;
use databend_common_meta_app::storage::StorageGcsConfig as InnerStorageGcsConfig;
use databend_common_meta_app::storage::StorageHdfsConfig as InnerStorageHdfsConfig;
use databend_common_meta_app::storage::StorageMokaConfig as InnerStorageMokaConfig;
use databend_common_meta_app::storage::StorageNetworkParams;
use databend_common_meta_app::storage::StorageObsConfig as InnerStorageObsConfig;
use databend_common_meta_app::storage::StorageOssConfig as InnerStorageOssConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config as InnerStorageS3Config;
use databend_common_meta_app::storage::StorageWebhdfsConfig as InnerStorageWebhdfsConfig;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_storage::StorageConfig as InnerStorageConfig;
use databend_common_tracing::Config as InnerLogConfig;
use databend_common_tracing::FileConfig as InnerFileLogConfig;
use databend_common_tracing::HistoryConfig as InnerHistoryConfig;
use databend_common_tracing::HistoryTableConfig as InnerHistoryTableConfig;
use databend_common_tracing::OTLPConfig as InnerOTLPLogConfig;
use databend_common_tracing::OTLPEndpointConfig as InnerOTLPEndpointConfig;
use databend_common_tracing::OTLPProtocol;
use databend_common_tracing::ProfileLogConfig as InnerProfileLogConfig;
use databend_common_tracing::QueryLogConfig as InnerQueryLogConfig;
use databend_common_tracing::StderrConfig as InnerStderrLogConfig;
use databend_common_tracing::StructLogConfig as InnerStructLogConfig;
use databend_common_tracing::TracingConfig as InnerTracingConfig;
use databend_common_tracing::CONFIG_DEFAULT_LOG_LEVEL;
use databend_common_version::DATABEND_COMMIT_VERSION;
use serde::Deserialize;
use serde::Serialize;
use serde_with::with_prefix;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;

use super::inner;
use super::inner::CatalogConfig as InnerCatalogConfig;
use super::inner::CatalogHiveConfig as InnerCatalogHiveConfig;
use super::inner::InnerConfig;
use super::inner::LocalConfig as InnerLocalConfig;
use super::inner::MetaConfig as InnerMetaConfig;
use super::inner::QueryConfig as InnerQueryConfig;
use crate::builtin::BuiltInConfig;
use crate::builtin::UDFConfig;
use crate::builtin::UserConfig;
use crate::toml::TomlIgnored;

const CATALOG_HIVE: &str = "hive";

/// Config for `query`.
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Parser)]
#[clap(name = "databend-query", about, version = & * * DATABEND_COMMIT_VERSION, author)]
#[serde(default)]
pub struct Config {
    /// Run a command and quit
    #[command(subcommand)]
    #[serde(skip)]
    pub subcommand: Option<Commands>,

    // To be compatible with the old version, we keep the `cmd` arg
    // We should always use `databend-query ver` instead `databend-query --cmd ver` in latest version
    #[clap(long)]
    pub cmd: Option<String>,

    #[clap(long, short = 'c', value_name = "VALUE", default_value_t)]
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

    /// Note: Legacy Config API
    ///
    /// When setting its all fields to empty strings, it will be ignored
    ///
    /// external catalog config.
    /// - Later, catalog information SHOULD be kept in KV Service
    /// - currently only supports HIVE (via hive meta store)
    #[clap(flatten)]
    pub catalog: HiveCatalogConfig,

    // cache configs
    #[clap(flatten)]
    pub cache: CacheConfig,

    // spill Config
    #[clap(flatten)]
    pub spill: SpillConfig,

    /// external catalog config.
    ///
    /// - Later, catalog information SHOULD be kept in KV Service
    /// - currently only supports HIVE (via hive meta store)
    ///
    /// Note:
    ///
    /// when converted from inner config, all catalog configurations will store in `catalogs`
    #[clap(skip)]
    pub catalogs: HashMap<String, CatalogConfig>,
}

#[derive(Subcommand, Default, Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum Commands {
    #[default]
    Ver,
    Local {
        #[clap(long, short = 'q', default_value_t)]
        query: String,
        #[clap(long, default_value_t)]
        output_format: String,
        #[clap(long, short = 'c')]
        config: String,
    },
}

impl Default for Config {
    fn default() -> Self {
        InnerConfig::default().into_config()
    }
}

impl Config {
    /// Load will load config from file, env and args.
    ///
    /// - Load from file as default.
    /// - Load from env, will override config from file.
    /// - Load from args as finally override
    ///
    /// # Notes
    ///
    /// with_args is to control whether we need to load from args or not.
    /// We should set this to false during tests because we don't want
    /// our test binary to parse cargo's args.
    #[no_sanitize(address)]
    pub fn load(with_args: bool) -> Result<Self> {
        let mut arg_conf = Self::default();

        if with_args {
            arg_conf = Self::parse();
        }

        if arg_conf.cmd == Some("ver".to_string()) {
            arg_conf.subcommand = Some(Commands::Ver);
        }

        if arg_conf.subcommand.is_some() {
            return Ok(arg_conf);
        }

        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // Load from config file first.
        {
            let final_config_file = if !arg_conf.config_file.is_empty() {
                // TODO: remove this `allow(clippy::redundant_clone)`
                // as soon as this issue is fixed:
                // https://github.com/rust-lang/rust-clippy/issues/10940
                #[allow(clippy::redundant_clone)]
                arg_conf.config_file.clone()
            } else if let Ok(path) = env::var("CONFIG_FILE") {
                path
            } else {
                "".to_string()
            };

            if !final_config_file.is_empty() {
                let toml = TomlIgnored::new(Box::new(|path| {
                    log::warn!("unknown field in config: {}", &path);
                }));
                builder = builder.collect(from_file(toml, &final_config_file));
            }
        }

        // Then, load from env.
        builder = builder.collect(from_env());

        // Finally, load from args.
        if with_args {
            builder = builder.collect(from_self(arg_conf));
        }

        // Check obsoleted.
        let conf = builder.build()?;
        conf.check_obsoleted()?;

        Ok(conf)
    }

    pub fn load_with_config_file(config_file: &str) -> Result<Self> {
        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // Load from config file first.
        {
            let config_file = if !config_file.is_empty() {
                config_file.to_string()
            } else if let Ok(path) = env::var("CONFIG_FILE") {
                path
            } else {
                "".to_string()
            };

            if !config_file.is_empty() {
                let toml = TomlIgnored::new(Box::new(|path| {
                    log::warn!("unknown field in config: {}", &path);
                }));
                builder = builder.collect(from_file(toml, &config_file));
            }
        }

        // Then, load from env.
        builder = builder.collect(from_env());

        // Check obsoleted.
        let conf = builder.build()?;
        conf.check_obsoleted()?;

        Ok(conf)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StorageNetworkConfig {
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_retry_timeout: u64,
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_retry_io_timeout: u64,
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_pool_max_idle_per_host: usize,
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_connect_timeout: u64,
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_tcp_keepalive: u64,
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_max_concurrent_io_requests: usize,
}

impl From<StorageNetworkParams> for StorageNetworkConfig {
    fn from(value: StorageNetworkParams) -> Self {
        StorageNetworkConfig {
            storage_retry_timeout: value.retry_timeout,
            storage_retry_io_timeout: value.retry_io_timeout,
            storage_pool_max_idle_per_host: value.pool_max_idle_per_host,
            storage_connect_timeout: value.connect_timeout,
            storage_tcp_keepalive: value.tcp_keepalive,
            storage_max_concurrent_io_requests: value.max_concurrent_io_requests,
        }
    }
}

impl TryInto<StorageNetworkParams> for StorageNetworkConfig {
    type Error = ErrorCode;

    fn try_into(self) -> std::result::Result<StorageNetworkParams, Self::Error> {
        Ok(StorageNetworkParams {
            retry_timeout: self.storage_retry_timeout,
            retry_io_timeout: self.storage_retry_io_timeout,
            tcp_keepalive: self.storage_tcp_keepalive,
            connect_timeout: self.storage_connect_timeout,
            pool_max_idle_per_host: self.storage_pool_max_idle_per_host,
            max_concurrent_io_requests: self.storage_max_concurrent_io_requests,
        })
    }
}

/// Storage config group.
///
/// # TODO(xuanwo)
///
/// In the future, we will use the following storage config layout:
///
/// ```toml
/// [storage]
///
/// [storage.data]
/// type = "s3"
///
/// [storage.temporary]
/// type = "s3"
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StorageConfig {
    #[clap(long = "storage-type", value_name = "VALUE", default_value = "fs")]
    #[serde(rename = "type")]
    pub typ: String,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub storage_type: Option<String>,

    #[clap(long, value_name = "VALUE", default_value_t)]
    #[serde(rename = "num_cpus")]
    pub storage_num_cpus: u64,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    #[serde(rename = "storage_num_cpus")]
    pub deprecated_storage_num_cpus: Option<u64>,

    #[clap(long = "storage-allow-insecure")]
    pub allow_insecure: bool,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_retry_timeout: u64,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_retry_io_timeout: u64,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_pool_max_idle_per_host: usize,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_connect_timeout: u64,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_tcp_keepalive: u64,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub storage_max_concurrent_io_requests: usize,

    // Fs storage backend config.
    #[clap(flatten)]
    pub fs: FsStorageConfig,

    // GCS backend config
    #[clap(flatten)]
    pub gcs: GcsStorageConfig,

    // S3 storage backend config.
    #[clap(flatten)]
    pub s3: S3StorageConfig,

    // azure storage blob config.
    #[clap(flatten)]
    pub azblob: AzblobStorageConfig,

    // hdfs storage backend config
    #[clap(flatten)]
    pub hdfs: HdfsConfig,

    // OBS storage backend config
    #[clap(flatten)]
    pub obs: ObsStorageConfig,

    // OSS storage backend config
    #[clap(flatten)]
    pub oss: OssStorageConfig,

    // WebHDFS storage backend config
    #[clap(flatten)]
    pub webhdfs: WebhdfsStorageConfig,

    // COS storage backend config
    #[clap(flatten)]
    pub cos: CosStorageConfig,
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
            typ: "".to_string(),
            allow_insecure: inner.allow_insecure,
            // use default for each config instead of using `..Default::default`
            // using `..Default::default` is calling `Self::default`
            // and `Self::default` relies on `InnerStorage::into()`
            // this will lead to a stack overflow
            fs: Default::default(),
            gcs: Default::default(),
            s3: Default::default(),
            oss: Default::default(),
            azblob: Default::default(),
            hdfs: Default::default(),
            obs: Default::default(),
            webhdfs: Default::default(),
            cos: Default::default(),

            storage_retry_timeout: 0,
            storage_retry_io_timeout: 0,
            storage_pool_max_idle_per_host: 0,
            storage_connect_timeout: 0,
            storage_tcp_keepalive: 0,
            storage_max_concurrent_io_requests: 0,

            // Deprecated fields
            storage_type: None,
            deprecated_storage_num_cpus: None,
        };

        match inner.params {
            StorageParams::Azblob(v) => {
                cfg.typ = "azblob".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.azblob = v.into();
            }
            StorageParams::Fs(v) => {
                cfg.typ = "fs".to_string();
                cfg.fs = v.into();
            }
            #[cfg(feature = "storage-hdfs")]
            StorageParams::Hdfs(v) => {
                cfg.typ = "hdfs".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.hdfs = v.into();
            }
            StorageParams::Memory => {
                cfg.typ = "memory".to_string();
            }
            StorageParams::S3(v) => {
                cfg.typ = "s3".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.s3 = v.into()
            }
            StorageParams::Gcs(v) => {
                cfg.typ = "gcs".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.gcs = v.into()
            }
            StorageParams::Obs(v) => {
                cfg.typ = "obs".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.obs = v.into()
            }
            StorageParams::Oss(v) => {
                cfg.typ = "oss".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.oss = v.into()
            }
            StorageParams::Webhdfs(v) => {
                cfg.typ = "webhdfs".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.webhdfs = v.into()
            }
            StorageParams::Cos(v) => {
                cfg.typ = "cos".to_string();

                if let Some(v) = v.network_config.as_ref() {
                    cfg.update_storage_network_param(v);
                }

                cfg.cos = v.into()
            }
            v => unreachable!("{v:?} should not be used as storage backend"),
        }

        cfg
    }
}

impl StorageConfig {
    fn update_storage_network_param(&mut self, v: &StorageNetworkParams) {
        self.storage_retry_timeout = v.retry_timeout;
        self.storage_tcp_keepalive = v.tcp_keepalive;
        self.storage_connect_timeout = v.connect_timeout;
        self.storage_retry_io_timeout = v.retry_io_timeout;
        self.storage_pool_max_idle_per_host = v.pool_max_idle_per_host;
        self.storage_max_concurrent_io_requests = v.max_concurrent_io_requests;
    }

    fn create_storage_network_params(&self) -> StorageNetworkParams {
        StorageNetworkParams {
            retry_timeout: self.storage_retry_timeout,
            retry_io_timeout: self.storage_retry_io_timeout,
            tcp_keepalive: self.storage_tcp_keepalive,
            connect_timeout: self.storage_connect_timeout,
            pool_max_idle_per_host: self.storage_pool_max_idle_per_host,
            max_concurrent_io_requests: self.storage_max_concurrent_io_requests,
        }
    }
}

impl TryInto<InnerStorageConfig> for StorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageConfig> {
        if self.storage_type.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`storage_type` is deprecated, please use `type` instead",
            ));
        }
        if self.deprecated_storage_num_cpus.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`storage_num_cpus` is deprecated, please use `num_cpus` instead",
            ));
        }

        let storage_network_params = self.create_storage_network_params();
        Ok(InnerStorageConfig {
            num_cpus: self.storage_num_cpus,
            allow_insecure: self.allow_insecure,
            params: {
                match self.typ.as_str() {
                    "azblob" => {
                        let mut azblob_config: InnerStorageAzblobConfig = self.azblob.try_into()?;
                        azblob_config.network_config = Some(storage_network_params);
                        StorageParams::Azblob(azblob_config)
                    }
                    "fs" => StorageParams::Fs(self.fs.try_into()?),
                    "gcs" => {
                        let mut gcs_config: InnerStorageGcsConfig = self.gcs.try_into()?;
                        gcs_config.network_config = Some(storage_network_params);
                        StorageParams::Gcs(gcs_config)
                    }
                    #[cfg(feature = "storage-hdfs")]
                    "hdfs" => {
                        let mut hdfs_config: InnerStorageHdfsConfig = self.hdfs.try_into()?;
                        hdfs_config.network_config = Some(storage_network_params);
                        StorageParams::Hdfs(hdfs_config)
                    }
                    "memory" => StorageParams::Memory,
                    "s3" => {
                        let mut s3config: InnerStorageS3Config = self.s3.try_into()?;
                        s3config.network_config = Some(storage_network_params);
                        StorageParams::S3(s3config)
                    }
                    "obs" => {
                        let mut obs_config: InnerStorageObsConfig = self.obs.try_into()?;
                        obs_config.network_config = Some(storage_network_params);
                        StorageParams::Obs(obs_config)
                    }
                    "oss" => {
                        let mut oss_config: InnerStorageOssConfig = self.oss.try_into()?;
                        oss_config.network_config = Some(storage_network_params);
                        StorageParams::Oss(oss_config)
                    }
                    "webhdfs" => {
                        let mut webhdfs_config: InnerStorageWebhdfsConfig =
                            self.webhdfs.try_into()?;
                        webhdfs_config.network_config = Some(storage_network_params);
                        StorageParams::Webhdfs(webhdfs_config)
                    }
                    "cos" => {
                        let mut cos_config: InnerStorageCosConfig = self.cos.try_into()?;
                        cos_config.network_config = Some(storage_network_params);
                        StorageParams::Cos(cos_config)
                    }
                    _ => return Err(ErrorCode::StorageOther("not supported storage type")),
                }
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogConfig {
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(flatten)]
    pub hive: CatalogsHiveConfig,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            ty: "hive".to_string(),
            hive: CatalogsHiveConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CatalogsHiveConfig {
    pub address: String,

    /// Deprecated fields, used for catching error, will be removed later.
    pub meta_store_address: Option<String>,

    pub protocol: String,
}

/// this is the legacy version of external catalog configuration
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct HiveCatalogConfig {
    #[clap(
        long = "hive-meta-store-address",
        value_name = "VALUE",
        default_value_t
    )]
    pub address: String,
    /// Deprecated fields, used for catching error, will be removed later.
    pub meta_store_address: Option<String>,

    #[clap(long = "hive-thrift-protocol", value_name = "VALUE", default_value_t)]
    pub protocol: String,
}

impl TryInto<InnerCatalogConfig> for CatalogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> std::result::Result<InnerCatalogConfig, Self::Error> {
        match self.ty.as_str() {
            "hive" => Ok(InnerCatalogConfig::Hive(self.hive.try_into()?)),
            ty => Err(ErrorCode::CatalogNotSupported(format!(
                "got unsupported catalog type in config: {}",
                ty
            ))),
        }
    }
}

impl From<InnerCatalogConfig> for CatalogConfig {
    fn from(inner: InnerCatalogConfig) -> Self {
        match inner {
            InnerCatalogConfig::Hive(v) => Self {
                ty: "hive".to_string(),
                hive: v.into(),
            },
        }
    }
}

impl TryInto<InnerCatalogHiveConfig> for CatalogsHiveConfig {
    type Error = ErrorCode;
    fn try_into(self) -> std::result::Result<InnerCatalogHiveConfig, Self::Error> {
        if self.meta_store_address.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`meta_store_address` is deprecated, please use `address` instead",
            ));
        }

        Ok(InnerCatalogHiveConfig {
            metastore_address: self.address,
            protocol: self.protocol.parse()?,
        })
    }
}

impl From<InnerCatalogHiveConfig> for CatalogsHiveConfig {
    fn from(inner: InnerCatalogHiveConfig) -> Self {
        Self {
            address: inner.metastore_address,
            protocol: inner.protocol.to_string(),

            // Deprecated fields
            meta_store_address: None,
        }
    }
}

impl Default for CatalogsHiveConfig {
    fn default() -> Self {
        InnerCatalogHiveConfig::default().into()
    }
}

impl TryInto<InnerCatalogHiveConfig> for HiveCatalogConfig {
    type Error = ErrorCode;
    fn try_into(self) -> std::result::Result<InnerCatalogHiveConfig, Self::Error> {
        if self.meta_store_address.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`meta_store_address` is deprecated, please use `address` instead",
            ));
        }

        Ok(InnerCatalogHiveConfig {
            metastore_address: self.address,
            protocol: self.protocol.parse()?,
        })
    }
}

impl From<InnerCatalogHiveConfig> for HiveCatalogConfig {
    fn from(inner: InnerCatalogHiveConfig) -> Self {
        Self {
            address: inner.metastore_address,
            protocol: inner.protocol.to_string(),

            // Deprecated fields
            meta_store_address: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FsStorageConfig {
    /// fs storage backend data path
    #[clap(
        long = "storage-fs-data-path",
        value_name = "VALUE",
        default_value = "_data"
    )]
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct GcsStorageConfig {
    #[clap(
        long = "storage-gcs-endpoint-url",
        value_name = "VALUE",
        default_value = "https://storage.googleapis.com"
    )]
    #[serde(rename = "endpoint_url")]
    pub gcs_endpoint_url: String,

    #[clap(long = "storage-gcs-bucket", value_name = "VALUE", default_value_t)]
    #[serde(rename = "bucket")]
    pub gcs_bucket: String,

    #[clap(long = "storage-gcs-root", value_name = "VALUE", default_value_t)]
    #[serde(rename = "root")]
    pub gcs_root: String,

    #[clap(long = "storage-gcs-credential", value_name = "VALUE", default_value_t)]
    pub credential: String,
}

impl Default for GcsStorageConfig {
    fn default() -> Self {
        InnerStorageGcsConfig::default().into()
    }
}

impl Debug for GcsStorageConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("GcsStorageConfig")
            .field("endpoint_url", &self.gcs_endpoint_url)
            .field("root", &self.gcs_root)
            .field("bucket", &self.gcs_bucket)
            .field("credential", &mask_string(&self.credential, 3))
            .finish()
    }
}

impl From<InnerStorageGcsConfig> for GcsStorageConfig {
    fn from(inner: InnerStorageGcsConfig) -> Self {
        Self {
            gcs_endpoint_url: inner.endpoint_url,
            gcs_bucket: inner.bucket,
            gcs_root: inner.root,
            credential: inner.credential,
        }
    }
}

impl TryInto<InnerStorageGcsConfig> for GcsStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> std::result::Result<InnerStorageGcsConfig, Self::Error> {
        Ok(InnerStorageGcsConfig {
            endpoint_url: self.gcs_endpoint_url,
            bucket: self.gcs_bucket,
            root: self.gcs_root,
            credential: self.credential,
            network_config: None,
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Eq, Args)]
#[serde(default)]
pub struct S3StorageConfig {
    /// Region for S3 storage
    #[clap(long = "storage-s3-region", value_name = "VALUE", default_value_t)]
    pub region: String,

    /// Endpoint URL for S3 storage
    #[clap(
        long = "storage-s3-endpoint-url",
        value_name = "VALUE",
        default_value = "https://s3.amazonaws.com"
    )]
    pub endpoint_url: String,

    /// Access key for S3 storage
    #[clap(
        long = "storage-s3-access-key-id",
        value_name = "VALUE",
        default_value_t
    )]
    pub access_key_id: String,

    /// Secret key for S3 storage
    #[clap(
        long = "storage-s3-secret-access-key",
        value_name = "VALUE",
        default_value_t
    )]
    pub secret_access_key: String,

    /// Security token for S3 storage
    ///
    /// Check out [documents](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) for details
    #[clap(
        long = "storage-s3-security-token",
        value_name = "VALUE",
        default_value_t
    )]
    pub security_token: String,

    /// S3 Bucket to use for storage
    #[clap(long = "storage-s3-bucket", value_name = "VALUE", default_value_t)]
    pub bucket: String,

    /// <root>
    #[clap(long = "storage-s3-root", value_name = "VALUE", default_value_t)]
    pub root: String,

    // TODO(xuanwo): We should support both AWS SSE and CSE in the future.
    #[clap(long = "storage-s3-master-key", value_name = "VALUE", default_value_t)]
    pub master_key: String,

    #[clap(long = "storage-s3-enable-virtual-host-style", value_name = "VALUE")]
    pub enable_virtual_host_style: bool,

    #[clap(long = "storage-s3-role-arn", value_name = "VALUE", default_value_t)]
    #[serde(rename = "role_arn")]
    pub s3_role_arn: String,

    #[clap(long = "storage-s3-external-id", value_name = "VALUE", default_value_t)]
    #[serde(rename = "external_id")]
    pub s3_external_id: String,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        InnerStorageS3Config::default().into()
    }
}

impl Debug for S3StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("S3StorageConfig")
            .field("endpoint_url", &self.endpoint_url)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("enable_virtual_host_style", &self.enable_virtual_host_style)
            .field("role_arn", &self.s3_role_arn)
            .field("external_id", &self.s3_external_id)
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
            security_token: inner.security_token,
            bucket: inner.bucket,
            root: inner.root,
            master_key: inner.master_key,
            enable_virtual_host_style: inner.enable_virtual_host_style,
            s3_role_arn: inner.role_arn,
            s3_external_id: inner.external_id,
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
            security_token: self.security_token,
            master_key: self.master_key,
            root: self.root,
            disable_credential_loader: false,
            enable_virtual_host_style: self.enable_virtual_host_style,
            role_arn: self.s3_role_arn,
            external_id: self.s3_external_id,
            network_config: None,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct AzblobStorageConfig {
    /// Account for Azblob
    #[clap(
        long = "storage-azblob-account-name",
        value_name = "VALUE",
        default_value_t
    )]
    pub account_name: String,

    /// Master key for Azblob
    #[clap(
        long = "storage-azblob-account-key",
        value_name = "VALUE",
        default_value_t
    )]
    pub account_key: String,

    /// Container for Azblob
    #[clap(
        long = "storage-azblob-container",
        value_name = "VALUE",
        default_value_t
    )]
    pub container: String,

    /// Endpoint URL for Azblob
    ///
    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use endpoint_url directly.
    #[clap(
        long = "storage-azblob-endpoint-url",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "endpoint_url")]
    pub azblob_endpoint_url: String,

    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use root directly.
    #[clap(long = "storage-azblob-root", value_name = "VALUE", default_value_t)]
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
            network_config: None,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args, Debug)]
#[serde(default)]
pub struct HdfsConfig {
    #[clap(long = "storage-hdfs-name-node", value_name = "VALUE", default_value_t)]
    pub name_node: String,
    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use root directly.
    #[clap(long = "storage-hdfs-root", value_name = "VALUE", default_value_t)]
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
            network_config: None,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct ObsStorageConfig {
    /// Access key for OBS storage
    #[clap(
        long = "storage-obs-access-key-id",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "access_key_id")]
    pub obs_access_key_id: String,

    /// Secret key for OBS storage
    #[clap(
        long = "storage-obs-secret-access-key",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "secret_access_key")]
    pub obs_secret_access_key: String,

    /// Bucket for OBS
    #[clap(long = "storage-obs-bucket", value_name = "VALUE", default_value_t)]
    #[serde(rename = "bucket")]
    pub obs_bucket: String,

    /// Endpoint URL for OBS
    ///
    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use endpoint_url directly.
    #[clap(
        long = "storage-obs-endpoint-url",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "endpoint_url")]
    pub obs_endpoint_url: String,

    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use root directly.
    #[clap(long = "storage-obs-root", value_name = "VALUE", default_value_t)]
    #[serde(rename = "root")]
    pub obs_root: String,
}

impl Default for ObsStorageConfig {
    fn default() -> Self {
        InnerStorageObsConfig::default().into()
    }
}

impl Debug for ObsStorageConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ObsStorageConfig")
            .field("endpoint_url", &self.obs_endpoint_url)
            .field("bucket", &self.obs_bucket)
            .field("root", &self.obs_root)
            .field("access_key_id", &mask_string(&self.obs_access_key_id, 3))
            .field(
                "secret_access_key",
                &mask_string(&self.obs_secret_access_key, 3),
            )
            .finish()
    }
}

impl From<InnerStorageObsConfig> for ObsStorageConfig {
    fn from(inner: InnerStorageObsConfig) -> Self {
        Self {
            obs_access_key_id: inner.access_key_id,
            obs_secret_access_key: inner.secret_access_key,
            obs_bucket: inner.bucket,
            obs_endpoint_url: inner.endpoint_url,
            obs_root: inner.root,
        }
    }
}

impl TryInto<InnerStorageObsConfig> for ObsStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageObsConfig> {
        Ok(InnerStorageObsConfig {
            endpoint_url: self.obs_endpoint_url,
            bucket: self.obs_bucket,
            access_key_id: self.obs_access_key_id,
            secret_access_key: self.obs_secret_access_key,
            root: self.obs_root,
            network_config: None,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct OssStorageConfig {
    /// Access key id for OSS storage
    #[clap(
        long = "storage-oss-access-key-id",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "access_key_id")]
    pub oss_access_key_id: String,

    /// Access Key Secret for OSS storage
    #[clap(
        long = "storage-oss-access-key-secret",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "access_key_secret")]
    pub oss_access_key_secret: String,

    /// Bucket for OSS
    #[clap(long = "storage-oss-bucket", value_name = "VALUE", default_value_t)]
    #[serde(rename = "bucket")]
    pub oss_bucket: String,

    #[clap(
        long = "storage-oss-endpoint-url",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "endpoint_url")]
    pub oss_endpoint_url: String,

    #[clap(
        long = "storage-oss-presign-endpoint-url",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "presign_endpoint_url")]
    pub oss_presign_endpoint_url: String,

    #[clap(long = "storage-oss-root", value_name = "VALUE", default_value_t)]
    #[serde(rename = "root")]
    pub oss_root: String,

    #[clap(
        long = "storage-oss-server-side-encryption",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "server_side_encryption")]
    pub oss_server_side_encryption: String,

    #[clap(
        long = "storage-oss-server-side-encryption-key-id",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "server_side_encryption_key_id")]
    pub oss_server_side_encryption_key_id: String,
}

impl Default for OssStorageConfig {
    fn default() -> Self {
        InnerStorageOssConfig::default().into()
    }
}

impl Debug for OssStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("OssStorageConfig")
            .field("root", &self.oss_root)
            .field("bucket", &self.oss_bucket)
            .field("endpoint_url", &self.oss_endpoint_url)
            .field("access_key_id", &mask_string(&self.oss_access_key_id, 3))
            .field(
                "access_key_secret",
                &mask_string(&self.oss_access_key_secret, 3),
            )
            .finish()
    }
}

impl From<InnerStorageOssConfig> for OssStorageConfig {
    fn from(inner: InnerStorageOssConfig) -> Self {
        Self {
            oss_access_key_id: inner.access_key_id,
            oss_access_key_secret: inner.access_key_secret,
            oss_bucket: inner.bucket,
            oss_endpoint_url: inner.endpoint_url,
            oss_presign_endpoint_url: inner.presign_endpoint_url,
            oss_root: inner.root,
            oss_server_side_encryption: inner.server_side_encryption,
            oss_server_side_encryption_key_id: inner.server_side_encryption_key_id,
        }
    }
}

impl TryInto<InnerStorageOssConfig> for OssStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> std::result::Result<InnerStorageOssConfig, Self::Error> {
        Ok(InnerStorageOssConfig {
            endpoint_url: self.oss_endpoint_url,
            presign_endpoint_url: self.oss_presign_endpoint_url,
            bucket: self.oss_bucket,
            access_key_id: self.oss_access_key_id,
            access_key_secret: self.oss_access_key_secret,
            root: self.oss_root,
            server_side_encryption: self.oss_server_side_encryption,
            server_side_encryption_key_id: self.oss_server_side_encryption_key_id,
            network_config: None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct MokaStorageConfig {
    pub max_capacity: u64,
    pub time_to_live: i64,
    pub time_to_idle: i64,
}

impl Default for MokaStorageConfig {
    fn default() -> Self {
        InnerStorageMokaConfig::default().into()
    }
}

impl From<InnerStorageMokaConfig> for MokaStorageConfig {
    fn from(v: InnerStorageMokaConfig) -> Self {
        Self {
            max_capacity: v.max_capacity,
            time_to_live: v.time_to_live,
            time_to_idle: v.time_to_idle,
        }
    }
}

impl TryInto<InnerStorageMokaConfig> for MokaStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageMokaConfig> {
        Ok(InnerStorageMokaConfig {
            max_capacity: self.max_capacity,
            time_to_live: self.time_to_live,
            time_to_idle: self.time_to_idle,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct WebhdfsStorageConfig {
    /// delegation token for webhdfs storage
    #[clap(
        long = "storage-webhdfs-delegation",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "delegation")]
    pub webhdfs_delegation: String,
    /// endpoint url for webhdfs storage
    #[clap(
        long = "storage-webhdfs-endpoint",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "endpoint_url")]
    pub webhdfs_endpoint_url: String,
    /// working directory root for webhdfs storage
    #[clap(long = "storage-webhdfs-root", value_name = "VALUE", default_value_t)]
    #[serde(rename = "root")]
    pub webhdfs_root: String,
    /// Disable list batch if hdfs doesn't support yet.
    #[clap(
        long = "storage-webhdfs-disable-list-batch",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "disable_list_batch")]
    pub webhdfs_disable_list_batch: bool,
    #[clap(
        long = "storage-webhdfs-user-name",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "user_name")]
    pub webhdfs_user_name: String,
}

impl Default for WebhdfsStorageConfig {
    fn default() -> Self {
        InnerStorageWebhdfsConfig::default().into()
    }
}

impl Debug for WebhdfsStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("WebhdfsStorageConfig")
            .field("endpoint_url", &self.webhdfs_endpoint_url)
            .field("root", &self.webhdfs_root)
            .field("user_name", &self.webhdfs_user_name)
            .field("delegation", &mask_string(&self.webhdfs_delegation, 3))
            .finish()
    }
}

impl From<InnerStorageWebhdfsConfig> for WebhdfsStorageConfig {
    fn from(v: InnerStorageWebhdfsConfig) -> Self {
        Self {
            webhdfs_delegation: v.delegation,
            webhdfs_endpoint_url: v.endpoint_url,
            webhdfs_root: v.root,
            webhdfs_disable_list_batch: v.disable_list_batch,
            webhdfs_user_name: v.user_name,
        }
    }
}

impl TryFrom<WebhdfsStorageConfig> for InnerStorageWebhdfsConfig {
    type Error = ErrorCode;

    fn try_from(value: WebhdfsStorageConfig) -> std::result::Result<Self, Self::Error> {
        Ok(InnerStorageWebhdfsConfig {
            delegation: value.webhdfs_delegation,
            endpoint_url: value.webhdfs_endpoint_url,
            root: value.webhdfs_root,
            disable_list_batch: value.webhdfs_disable_list_batch,
            user_name: value.webhdfs_user_name,
            network_config: None,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct CosStorageConfig {
    /// Access key for COS storage
    #[clap(long = "storage-cos-secret-id", value_name = "VALUE", default_value_t)]
    #[serde(rename = "secret_id")]
    pub cos_secret_id: String,

    /// Secret key for COS storage
    #[clap(long = "storage-cos-secret-key", value_name = "VALUE", default_value_t)]
    #[serde(rename = "secret_key")]
    pub cos_secret_key: String,

    /// Bucket for COS
    #[clap(long = "storage-cos-bucket", value_name = "VALUE", default_value_t)]
    #[serde(rename = "bucket")]
    pub cos_bucket: String,

    /// Endpoint URL for COS
    #[clap(
        long = "storage-cos-endpoint-url",
        value_name = "VALUE",
        default_value_t
    )]
    #[serde(rename = "endpoint_url")]
    pub cos_endpoint_url: String,

    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use root directly.
    #[clap(long = "storage-cos-root", value_name = "VALUE", default_value_t)]
    #[serde(rename = "root")]
    pub cos_root: String,
}

impl Default for CosStorageConfig {
    fn default() -> Self {
        InnerStorageCosConfig::default().into()
    }
}

impl Debug for CosStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut ds = f.debug_struct("CosStorageConfig");

        ds.field("bucket", &self.cos_bucket);
        ds.field("endpoint_url", &self.cos_endpoint_url);
        ds.field("root", &self.cos_root);
        ds.field("secret_id", &mask_string(&self.cos_secret_id, 3));
        ds.field("secret_key", &mask_string(&self.cos_secret_key, 3));

        ds.finish()
    }
}

impl From<InnerStorageCosConfig> for CosStorageConfig {
    fn from(v: InnerStorageCosConfig) -> Self {
        Self {
            cos_secret_id: v.secret_id,
            cos_secret_key: v.secret_key,
            cos_bucket: v.bucket,
            cos_endpoint_url: v.endpoint_url,
            cos_root: v.root,
        }
    }
}

impl TryFrom<CosStorageConfig> for InnerStorageCosConfig {
    type Error = ErrorCode;

    fn try_from(value: CosStorageConfig) -> std::result::Result<Self, Self::Error> {
        Ok(InnerStorageCosConfig {
            secret_id: value.cos_secret_id,
            secret_key: value.cos_secret_key,
            bucket: value.cos_bucket,
            endpoint_url: value.cos_endpoint_url,
            root: value.cos_root,
            network_config: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingValue {
    UInt64(u64),
    String(String),
}

impl Serialize for SettingValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            SettingValue::UInt64(v) => serializer.serialize_u64(*v),
            SettingValue::String(v) => serializer.serialize_str(v),
        }
    }
}

impl<'de> Deserialize<'de> for SettingValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_any(SettingVisitor)
    }
}

impl From<SettingValue> for UserSettingValue {
    fn from(v: SettingValue) -> Self {
        match v {
            SettingValue::UInt64(v) => UserSettingValue::UInt64(v),
            SettingValue::String(v) => UserSettingValue::String(v),
        }
    }
}

struct SettingVisitor;

impl serde::de::Visitor<'_> for SettingVisitor {
    type Value = SettingValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "integer or string")
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where E: serde::de::Error {
        Ok(SettingValue::UInt64(value))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
    where E: serde::de::Error {
        if value < 0 {
            return Err(E::custom("setting value must be positive"));
        }
        Ok(SettingValue::UInt64(value as u64))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where E: serde::de::Error {
        Ok(SettingValue::String(value.to_string()))
    }
}

/// Query config group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    #[clap(long, value_name = "VALUE", default_value = "admin")]
    pub tenant_id: String,

    /// ID for construct the cluster.
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub cluster_id: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub num_cpus: u64,

    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1")]
    pub mysql_handler_host: String,

    #[clap(long, value_name = "VALUE", default_value = "3307")]
    pub mysql_handler_port: u16,

    #[clap(long, value_name = "VALUE", default_value = "120")]
    pub mysql_handler_tcp_keepalive_timeout_secs: u64,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub mysql_tls_server_cert: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub mysql_tls_server_key: String,

    #[clap(long, value_name = "VALUE", default_value = "256")]
    pub max_active_sessions: u64,

    #[clap(long, value_name = "VALUE", default_value = "8")]
    pub max_running_queries: u64,

    #[clap(long, value_name = "VALUE", default_value = "false")]
    pub global_statement_queue: bool,

    /// The max total memory in bytes that can be used by this process.
    #[clap(long, value_name = "VALUE", default_value = "0")]
    pub max_server_memory_usage: u64,

    #[clap(
        long,
        value_name = "VALUE",
        value_parser = clap::value_parser!(bool),
        default_value = "false"
    )]
    pub max_memory_limit_enabled: bool,

    #[deprecated(note = "clickhouse tcp support is deprecated")]
    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1")]
    pub clickhouse_handler_host: String,

    #[deprecated(note = "clickhouse tcp support is deprecated")]
    #[clap(long, value_name = "VALUE", default_value = "9000")]
    pub clickhouse_handler_port: u16,

    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1")]
    pub clickhouse_http_handler_host: String,

    #[clap(long, value_name = "VALUE", default_value = "8124")]
    pub clickhouse_http_handler_port: u16,

    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1")]
    pub http_handler_host: String,

    #[clap(long, value_name = "VALUE", default_value = "8000")]
    pub http_handler_port: u16,

    #[clap(long, value_name = "VALUE", default_value = "60")]
    pub http_handler_result_timeout_secs: u64,

    #[clap(long, value_name = "VALUE", default_value = "14400")]
    pub http_session_timeout_secs: u64,

    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1")]
    pub flight_sql_handler_host: String,

    #[clap(long, value_name = "VALUE", default_value = "8900")]
    pub flight_sql_handler_port: u16,

    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1:9090")]
    pub flight_api_address: String,

    #[clap(long, value_name = "VALUE", default_value = "")]
    pub discovery_address: String,

    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1:8080")]
    pub admin_api_address: String,

    #[clap(long, value_name = "VALUE", default_value = "127.0.0.1:7070")]
    pub metric_api_address: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub http_handler_tls_server_cert: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub http_handler_tls_server_key: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub http_handler_tls_server_root_ca_cert: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub flight_sql_tls_server_cert: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub flight_sql_tls_server_key: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub api_tls_server_cert: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub api_tls_server_key: String,

    #[clap(long, value_name = "VALUE", default_value_t)]
    pub api_tls_server_root_ca_cert: String,

    /// rpc server cert
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub rpc_tls_server_cert: String,

    /// key for rpc server cert
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub rpc_tls_server_key: String,

    /// Certificate for client to identify query rpc server
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub rpc_tls_query_server_root_ca_cert: String,

    #[clap(long, value_name = "VALUE", default_value = "localhost")]
    pub rpc_tls_query_service_domain_name: String,

    #[clap(long, value_name = "VALUE", default_value = "0")]
    pub rpc_client_timeout_secs: u64,

    /// Table engine memory enabled
    #[clap(
        long,
        value_name = "VALUE",
        value_parser = clap::value_parser!(bool),
        default_value = "true"
    )]
    pub table_engine_memory_enabled: bool,

    #[clap(long, value_name = "VALUE", default_value = "5000")]
    pub shutdown_wait_timeout_ms: u64,

    #[clap(long, value_name = "VALUE", default_value = "10000")]
    pub max_query_log_size: usize,

    #[clap(long, value_name = "VALUE")]
    pub databend_enterprise_license: Option<String>,
    /// If in management mode, only can do some meta level operations(database/table/user/stage etc.) with metasrv.
    #[clap(long)]
    pub management_mode: bool,

    /// Deprecated: jwt_key_file is deprecated, use jwt_key_files to add a list of available jwks url
    #[clap(long, value_name = "VALUE", default_value_t)]
    pub jwt_key_file: String,

    /// Interval in seconds to refresh jwks
    #[clap(long, value_name = "VALUE", default_value = "600")]
    pub jwks_refresh_interval: u64,

    /// Timeout in seconds to refresh jwks
    #[clap(long, value_name = "VALUE", default_value = "10")]
    pub jwks_refresh_timeout: u64,

    #[clap(skip)]
    pub jwt_key_files: Vec<String>,

    #[clap(long, value_name = "VALUE", default_value = "auto")]
    pub default_storage_format: String,

    #[clap(long, value_name = "VALUE", default_value = "auto")]
    pub default_compression: String,

    #[clap(skip)]
    users: Vec<UserConfig>,

    #[clap(skip)]
    udfs: Vec<UDFConfig>,

    #[clap(long, value_name = "VALUE", default_value = "")]
    pub share_endpoint_address: String,

    #[clap(long, value_name = "VALUE", default_value = "")]
    pub share_endpoint_auth_token_file: String,

    #[clap(skip)]
    quota: Option<TenantQuota>,

    #[clap(long, value_name = "VALUE")]
    pub internal_enable_sandbox_tenant: bool,

    #[clap(long, value_name = "VALUE")]
    pub enable_meta_data_upgrade_json_to_pb_from_v307: bool,

    /// Experiment config options, DO NOT USE IT IN PRODUCTION ENV
    #[clap(long, value_name = "VALUE")]
    pub internal_merge_on_read_mutation: bool,

    /// Max retention time in days for data, default is 90 days.
    #[clap(long, value_name = "VALUE", default_value = "90")]
    pub(crate) data_retention_time_in_days_max: u64,

    // ----- the following options/args are all deprecated               ----
    // ----- and turned into Option<T>, to help user migrate the configs ----
    /// OBSOLETED: Table disk cache size (mb).
    #[clap(long, value_name = "VALUE")]
    pub table_disk_cache_mb_size: Option<u64>,

    /// OBSOLETED: Table Meta Cached enabled
    #[clap(long, value_name = "VALUE")]
    pub table_meta_cache_enabled: Option<bool>,

    /// OBSOLETED: Max number of cached table block meta
    #[clap(long, value_name = "VALUE")]
    pub table_cache_block_meta_count: Option<u64>,

    /// OBSOLETED: Table memory cache size (mb)
    #[clap(long, value_name = "VALUE")]
    pub table_memory_cache_mb_size: Option<u64>,

    /// OBSOLETED: Table disk cache folder root
    #[clap(long, value_name = "VALUE")]
    pub table_disk_cache_root: Option<String>,

    /// OBSOLETED: Max number of cached table snapshot
    #[clap(long, value_name = "VALUE")]
    pub table_cache_snapshot_count: Option<u64>,

    /// OBSOLETED: Max number of cached table snapshot statistics
    #[clap(long, value_name = "VALUE")]
    pub table_cache_statistic_count: Option<u64>,

    /// OBSOLETED: Max number of cached table segment
    #[clap(long, value_name = "VALUE")]
    pub table_cache_segment_count: Option<u64>,

    /// OBSOLETED: Max number of cached bloom index meta objects
    #[clap(long, value_name = "VALUE")]
    pub table_cache_bloom_index_meta_count: Option<u64>,

    /// OBSOLETED:
    /// Max number of cached bloom index filters, default value is 1024 * 1024 items.
    /// One bloom index filter per column of data block being indexed will be generated if necessary.
    ///
    /// For example, a table of 1024 columns, with 800 data blocks, a query that triggers a full
    /// table filter on 2 columns, might populate 2 * 800 bloom index filter cache items (at most)
    #[clap(long, value_name = "VALUE")]
    pub table_cache_bloom_index_filter_count: Option<u64>,

    /// OBSOLETED: (cache of raw bloom filter data is no longer supported)
    /// Max bytes of cached bloom filter bytes.
    #[clap(long, value_name = "VALUE")]
    pub(crate) table_cache_bloom_index_data_bytes: Option<u64>,

    /// OBSOLETED: use settings['parquet_fast_read_bytes'] instead
    /// Parquet file with smaller size will be read as a whole file, instead of column by column.
    /// For example:
    /// parquet_fast_read_bytes = 52428800
    /// will let databend read whole file for parquet file less than 50MB and read column by column
    /// if file size is greater than 50MB
    #[clap(long, value_name = "VALUE")]
    pub parquet_fast_read_bytes: Option<u64>,

    /// OBSOLETED: use settings['max_storage_io_requests'] instead
    #[clap(long, value_name = "VALUE")]
    pub max_storage_io_requests: Option<u64>,

    /// Disable some system load(For example system.configs) for cloud security.
    #[clap(long, value_name = "VALUE")]
    pub disable_system_table_load: bool,

    /// chat base url.
    #[clap(
        long,
        value_name = "VALUE",
        default_value = "https://api.openai.com/v1/"
    )]
    pub openai_api_chat_base_url: String,

    /// embedding base url.
    #[clap(
        long,
        value_name = "VALUE",
        default_value = "https://api.openai.com/v1/"
    )]
    pub openai_api_embedding_base_url: String,

    // This will not show in system.configs, put it to mask.rs.
    #[clap(long, value_name = "VALUE", default_value = "")]
    pub openai_api_key: String,

    // For azure openai.
    #[clap(long, value_name = "VALUE", default_value = "")]
    pub openai_api_version: String,

    /// https://platform.openai.com/docs/models/embeddings
    #[clap(long, value_name = "VALUE", default_value = "text-embedding-ada-002")]
    pub openai_api_embedding_model: String,

    /// https://platform.openai.com/docs/guides/chat
    #[clap(long, value_name = "VALUE", default_value = "gpt-3.5-turbo")]
    pub openai_api_completion_model: String,

    #[clap(long, value_name = "VALUE", default_value = "true")]
    pub enable_udf_python_script: bool,

    #[clap(long, value_name = "VALUE", default_value = "true")]
    pub enable_udf_js_script: bool,

    #[clap(long, value_name = "VALUE", default_value = "true")]
    pub enable_udf_wasm_script: bool,

    #[clap(long, value_name = "VALUE", default_value = "false")]
    pub enable_udf_server: bool,

    /// A list of allowed udf server addresses.
    #[clap(long, value_name = "VALUE")]
    pub udf_server_allow_list: Vec<String>,

    #[clap(long, value_name = "VALUE", default_value = "false")]
    pub udf_server_allow_insecure: bool,

    #[clap(long)]
    pub cloud_control_grpc_server_address: Option<String>,

    #[clap(long, value_name = "VALUE", default_value = "0")]
    pub cloud_control_grpc_timeout: u64,

    #[clap(long, value_name = "VALUE", default_value = "50")]
    pub max_cached_queries_profiles: usize,

    /// A list of network that not to be checked by network policy.
    #[clap(long, value_name = "VALUE")]
    pub network_policy_whitelist: Vec<String>,

    #[clap(skip)]
    pub settings: HashMap<String, SettingValue>,

    #[clap(skip)]
    pub resources_management: Option<ResourcesManagementConfig>,

    #[clap(long, value_name = "VALUE", default_value = "false")]
    pub enable_queries_executor: bool,
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
            tenant_id: Tenant::new_or_err(self.tenant_id, "")
                .map_err(|_e| ErrorCode::InvalidConfig("tenant-id can not be empty"))?,
            cluster_id: self.cluster_id,
            node_id: "".to_string(),
            node_secret: "".to_string(),
            num_cpus: self.num_cpus,
            mysql_handler_host: self.mysql_handler_host,
            mysql_handler_port: self.mysql_handler_port,
            mysql_handler_tcp_keepalive_timeout_secs: self.mysql_handler_tcp_keepalive_timeout_secs,
            mysql_tls_server_cert: self.mysql_tls_server_cert,
            mysql_tls_server_key: self.mysql_tls_server_key,
            max_active_sessions: self.max_active_sessions,
            max_running_queries: self.max_running_queries,
            global_statement_queue: self.global_statement_queue,
            max_server_memory_usage: self.max_server_memory_usage,
            max_memory_limit_enabled: self.max_memory_limit_enabled,
            clickhouse_http_handler_host: self.clickhouse_http_handler_host,
            clickhouse_http_handler_port: self.clickhouse_http_handler_port,
            http_handler_host: self.http_handler_host,
            http_handler_port: self.http_handler_port,
            http_handler_result_timeout_secs: self.http_handler_result_timeout_secs,
            http_session_timeout_secs: self.http_session_timeout_secs,
            flight_api_address: self.flight_api_address,
            discovery_address: self.discovery_address,
            flight_sql_handler_host: self.flight_sql_handler_host,
            flight_sql_handler_port: self.flight_sql_handler_port,
            admin_api_address: self.admin_api_address,
            metric_api_address: self.metric_api_address,
            http_handler_tls_server_cert: self.http_handler_tls_server_cert,
            http_handler_tls_server_key: self.http_handler_tls_server_key,
            http_handler_tls_server_root_ca_cert: self.http_handler_tls_server_root_ca_cert,
            api_tls_server_cert: self.api_tls_server_cert,
            api_tls_server_key: self.api_tls_server_key,
            api_tls_server_root_ca_cert: self.api_tls_server_root_ca_cert,
            flight_sql_tls_server_cert: self.flight_sql_tls_server_cert,
            flight_sql_tls_server_key: self.flight_sql_tls_server_key,
            rpc_tls_server_cert: self.rpc_tls_server_cert,
            rpc_tls_server_key: self.rpc_tls_server_key,
            rpc_tls_query_server_root_ca_cert: self.rpc_tls_query_server_root_ca_cert,
            rpc_tls_query_service_domain_name: self.rpc_tls_query_service_domain_name,
            rpc_client_timeout_secs: self.rpc_client_timeout_secs,
            table_engine_memory_enabled: self.table_engine_memory_enabled,
            shutdown_wait_timeout_ms: self.shutdown_wait_timeout_ms,
            max_query_log_size: self.max_query_log_size,
            databend_enterprise_license: self.databend_enterprise_license,
            management_mode: self.management_mode,
            parquet_fast_read_bytes: self.parquet_fast_read_bytes,
            max_storage_io_requests: self.max_storage_io_requests,
            jwt_key_file: self.jwt_key_file,
            jwt_key_files: self.jwt_key_files,
            jwks_refresh_interval: self.jwks_refresh_interval,
            jwks_refresh_timeout: self.jwks_refresh_timeout,
            default_storage_format: self.default_storage_format,
            default_compression: self.default_compression,
            builtin: BuiltInConfig {
                users: self.users,
                udfs: self.udfs,
            },
            share_endpoint_address: self.share_endpoint_address,
            share_endpoint_auth_token_file: self.share_endpoint_auth_token_file,
            tenant_quota: self.quota,
            upgrade_to_pb: self.enable_meta_data_upgrade_json_to_pb_from_v307,
            internal_enable_sandbox_tenant: self.internal_enable_sandbox_tenant,
            internal_merge_on_read_mutation: self.internal_merge_on_read_mutation,
            data_retention_time_in_days_max: self.data_retention_time_in_days_max,
            disable_system_table_load: self.disable_system_table_load,
            openai_api_chat_base_url: self.openai_api_chat_base_url,
            openai_api_embedding_base_url: self.openai_api_embedding_base_url,
            openai_api_key: self.openai_api_key,
            openai_api_completion_model: self.openai_api_completion_model,
            openai_api_embedding_model: self.openai_api_embedding_model,
            openai_api_version: self.openai_api_version,
            enable_udf_server: self.enable_udf_server,
            enable_udf_python_script: self.enable_udf_python_script,
            enable_udf_js_script: self.enable_udf_js_script,
            enable_udf_wasm_script: self.enable_udf_wasm_script,
            udf_server_allow_list: self.udf_server_allow_list,
            udf_server_allow_insecure: self.udf_server_allow_insecure,
            cloud_control_grpc_server_address: self.cloud_control_grpc_server_address,
            cloud_control_grpc_timeout: self.cloud_control_grpc_timeout,
            max_cached_queries_profiles: self.max_cached_queries_profiles,
            network_policy_whitelist: self.network_policy_whitelist,
            settings: self
                .settings
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            resources_management: self.resources_management,
            enable_queries_executor: self.enable_queries_executor,
        })
    }
}

#[allow(deprecated)]
impl From<InnerQueryConfig> for QueryConfig {
    fn from(inner: InnerQueryConfig) -> Self {
        Self {
            tenant_id: inner.tenant_id.tenant_name().to_string(),
            cluster_id: inner.cluster_id,
            num_cpus: inner.num_cpus,
            mysql_handler_host: inner.mysql_handler_host,
            mysql_handler_port: inner.mysql_handler_port,
            mysql_handler_tcp_keepalive_timeout_secs: inner
                .mysql_handler_tcp_keepalive_timeout_secs,
            mysql_tls_server_cert: inner.mysql_tls_server_cert,
            mysql_tls_server_key: inner.mysql_tls_server_key,
            max_active_sessions: inner.max_active_sessions,
            max_running_queries: inner.max_running_queries,
            global_statement_queue: inner.global_statement_queue,
            max_server_memory_usage: inner.max_server_memory_usage,
            max_memory_limit_enabled: inner.max_memory_limit_enabled,

            // clickhouse tcp is deprecated
            clickhouse_handler_host: "127.0.0.1".to_string(),
            clickhouse_handler_port: 9000,

            clickhouse_http_handler_host: inner.clickhouse_http_handler_host,
            clickhouse_http_handler_port: inner.clickhouse_http_handler_port,
            http_handler_host: inner.http_handler_host,
            http_handler_port: inner.http_handler_port,
            http_handler_result_timeout_secs: inner.http_handler_result_timeout_secs,
            http_session_timeout_secs: inner.http_session_timeout_secs,
            flight_api_address: inner.flight_api_address,
            flight_sql_handler_host: inner.flight_sql_handler_host,
            flight_sql_handler_port: inner.flight_sql_handler_port,
            discovery_address: inner.discovery_address,
            admin_api_address: inner.admin_api_address,
            metric_api_address: inner.metric_api_address,
            http_handler_tls_server_cert: inner.http_handler_tls_server_cert,
            http_handler_tls_server_key: inner.http_handler_tls_server_key,
            http_handler_tls_server_root_ca_cert: inner.http_handler_tls_server_root_ca_cert,
            api_tls_server_cert: inner.api_tls_server_cert,
            api_tls_server_key: inner.api_tls_server_key,
            api_tls_server_root_ca_cert: inner.api_tls_server_root_ca_cert,
            flight_sql_tls_server_cert: inner.flight_sql_tls_server_cert,
            flight_sql_tls_server_key: inner.flight_sql_tls_server_key,
            rpc_tls_server_cert: inner.rpc_tls_server_cert,
            rpc_tls_server_key: inner.rpc_tls_server_key,
            rpc_tls_query_server_root_ca_cert: inner.rpc_tls_query_server_root_ca_cert,
            rpc_tls_query_service_domain_name: inner.rpc_tls_query_service_domain_name,
            rpc_client_timeout_secs: inner.rpc_client_timeout_secs,
            table_engine_memory_enabled: inner.table_engine_memory_enabled,
            shutdown_wait_timeout_ms: inner.shutdown_wait_timeout_ms,
            max_query_log_size: inner.max_query_log_size,
            databend_enterprise_license: inner.databend_enterprise_license,
            management_mode: inner.management_mode,
            parquet_fast_read_bytes: inner.parquet_fast_read_bytes,
            max_storage_io_requests: inner.max_storage_io_requests,
            jwt_key_file: inner.jwt_key_file,
            jwt_key_files: inner.jwt_key_files,
            jwks_refresh_interval: inner.jwks_refresh_interval,
            jwks_refresh_timeout: inner.jwks_refresh_timeout,
            default_storage_format: inner.default_storage_format,
            default_compression: inner.default_compression,
            users: inner.builtin.users,
            udfs: inner.builtin.udfs,
            share_endpoint_address: inner.share_endpoint_address,
            share_endpoint_auth_token_file: inner.share_endpoint_auth_token_file,
            quota: inner.tenant_quota,
            enable_meta_data_upgrade_json_to_pb_from_v307: inner.upgrade_to_pb,
            internal_enable_sandbox_tenant: inner.internal_enable_sandbox_tenant,
            internal_merge_on_read_mutation: false,
            data_retention_time_in_days_max: 90,

            // obsoleted config entries
            table_disk_cache_mb_size: None,
            table_meta_cache_enabled: None,
            table_cache_block_meta_count: None,
            table_memory_cache_mb_size: None,
            table_disk_cache_root: None,
            table_cache_snapshot_count: None,
            table_cache_statistic_count: None,
            table_cache_segment_count: None,
            table_cache_bloom_index_meta_count: None,
            table_cache_bloom_index_filter_count: None,
            table_cache_bloom_index_data_bytes: None,
            //
            disable_system_table_load: inner.disable_system_table_load,
            openai_api_chat_base_url: inner.openai_api_chat_base_url,
            openai_api_embedding_base_url: inner.openai_api_embedding_base_url,
            openai_api_key: inner.openai_api_key,
            openai_api_version: inner.openai_api_version,
            openai_api_completion_model: inner.openai_api_completion_model,
            openai_api_embedding_model: inner.openai_api_embedding_model,
            enable_udf_python_script: inner.enable_udf_python_script,
            enable_udf_js_script: inner.enable_udf_js_script,
            enable_udf_wasm_script: inner.enable_udf_wasm_script,

            enable_udf_server: inner.enable_udf_server,
            udf_server_allow_list: inner.udf_server_allow_list,
            udf_server_allow_insecure: inner.udf_server_allow_insecure,
            cloud_control_grpc_server_address: inner.cloud_control_grpc_server_address,
            cloud_control_grpc_timeout: inner.cloud_control_grpc_timeout,
            max_cached_queries_profiles: inner.max_cached_queries_profiles,
            network_policy_whitelist: inner.network_policy_whitelist,
            settings: HashMap::new(),
            resources_management: None,
            enable_queries_executor: inner.enable_queries_executor,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct LogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-level", value_name = "VALUE", default_value = CONFIG_DEFAULT_LOG_LEVEL)]
    pub level: String,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub log_level: Option<String>,

    /// Log file dir
    #[clap(
        long = "log-dir",
        value_name = "VALUE",
        default_value = "./.databend/logs"
    )]
    pub dir: String,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub log_dir: Option<String>,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub query_enabled: Option<bool>,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub log_query_enabled: Option<bool>,

    #[clap(flatten)]
    pub file: FileLogConfig,

    #[clap(flatten)]
    pub stderr: StderrLogConfig,

    #[clap(flatten)]
    pub otlp: OTLPLogConfig,

    #[clap(flatten)]
    pub query: QueryLogConfig,

    #[clap(flatten)]
    pub profile: ProfileLogConfig,

    #[clap(flatten)]
    pub structlog: StructLogConfig,

    #[clap(flatten)]
    pub tracing: TracingConfig,

    #[clap(flatten)]
    pub history: HistoryLogConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        InnerLogConfig::default().into()
    }
}

impl TryInto<InnerLogConfig> for LogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerLogConfig> {
        if self.log_dir.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`log_dir` is deprecated, use `dir` instead".to_string(),
            ));
        }
        if self.log_level.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`log_level` is deprecated, use `level` instead".to_string(),
            ));
        }
        if self.query_enabled.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`query_enabled` is deprecated, use `query.on` instead".to_string(),
            ));
        }
        if self.log_query_enabled.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`log_query_enabled` is deprecated, use `query.on` instead".to_string(),
            ));
        }

        let mut file: InnerFileLogConfig = self.file.try_into()?;
        if self.level != CONFIG_DEFAULT_LOG_LEVEL {
            file.level = self.level.to_string();
        }
        if self.dir != "./.databend/logs" {
            file.dir = self.dir.to_string();
        }

        let otlp: InnerOTLPLogConfig = self.otlp.try_into()?;
        if otlp.on && otlp.endpoint.endpoint.is_empty() {
            return Err(ErrorCode::InvalidConfig(
                "`endpoint` must be set when `otlp.on` is true".to_string(),
            ));
        }

        let mut query: InnerQueryLogConfig = self.query.try_into()?;
        if query.on && query.dir.is_empty() && query.otlp.is_none() {
            if file.dir.is_empty() {
                return Err(ErrorCode::InvalidConfig(
                    "`dir` or `file.dir` must be set when `query.dir` is empty".to_string(),
                ));
            } else {
                query.dir = format!("{}/query-details", &file.dir);
            }
        }

        let mut profile: InnerProfileLogConfig = self.profile.try_into()?;
        if profile.on && profile.dir.is_empty() && profile.otlp.is_none() {
            if file.dir.is_empty() {
                return Err(ErrorCode::InvalidConfig(
                    "`dir` or `file.dir` must be set when `profile.dir` is empty".to_string(),
                ));
            } else {
                profile.dir = format!("{}/profiles", &file.dir);
            }
        }

        let mut structlog: InnerStructLogConfig = self.structlog.try_into()?;
        if structlog.on && structlog.dir.is_empty() {
            if file.dir.is_empty() {
                return Err(ErrorCode::InvalidConfig(
                    "`dir` or `file.dir` must be set when `structlog.on` is true".to_string(),
                ));
            } else {
                structlog.dir = format!("{}/structlogs", &file.dir);
            }
        }

        let tracing: InnerTracingConfig = self.tracing.try_into()?;

        let mut history: InnerHistoryConfig = self.history.try_into()?;

        if history.on && history.level.is_empty() && file.on && !file.level.is_empty() {
            history.level = file.level.clone();
        }

        Ok(InnerLogConfig {
            file,
            stderr: self.stderr.try_into()?,
            otlp,
            query,
            profile,
            structlog,
            tracing,
            history,
        })
    }
}

impl From<InnerLogConfig> for LogConfig {
    fn from(inner: InnerLogConfig) -> Self {
        Self {
            level: inner.file.level.clone(),
            dir: inner.file.dir.clone(),
            file: inner.file.into(),
            stderr: inner.stderr.into(),
            otlp: inner.otlp.into(),
            query: inner.query.into(),
            profile: inner.profile.into(),
            structlog: inner.structlog.into(),
            tracing: inner.tracing.into(),
            history: inner.history.into(),

            // Deprecated fields
            log_dir: None,
            log_level: None,
            query_enabled: None,
            log_query_enabled: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FileLogConfig {
    #[clap(
        long = "log-file-on", value_name = "VALUE", default_value = "true", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub file_on: bool,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(long = "log-file-level", value_name = "VALUE", default_value = CONFIG_DEFAULT_LOG_LEVEL)]
    #[serde(rename = "level")]
    pub file_level: String,

    /// Log file dir
    #[clap(
        long = "log-file-dir",
        value_name = "VALUE",
        default_value = "./.databend/logs"
    )]
    #[serde(rename = "dir")]
    pub file_dir: String,

    /// Log file format
    #[clap(long = "log-file-format", value_name = "VALUE", default_value = "json")]
    #[serde(rename = "format")]
    pub file_format: String,

    /// Log file max
    #[clap(long = "log-file-limit", value_name = "VALUE", default_value = "48")]
    #[serde(rename = "limit")]
    pub file_limit: usize,

    /// The max size(bytes) of the log file, default is 4GB.
    #[clap(
        long = "log-file-max-size",
        value_name = "VALUE",
        default_value = "4294967296"
    )]
    #[serde(rename = "max-size")]
    pub file_max_size: usize,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    #[serde(rename = "prefix_filter")]
    pub file_prefix_filter: Option<String>,
}

impl Default for FileLogConfig {
    fn default() -> Self {
        InnerFileLogConfig::default().into()
    }
}

impl TryInto<InnerFileLogConfig> for FileLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerFileLogConfig> {
        if self.file_prefix_filter.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`prefix_filter` is deprecated, use `level` with the syntax of env_logger instead."
                    .to_string(),
            ));
        }
        Ok(InnerFileLogConfig {
            on: self.file_on,
            level: self.file_level,
            dir: self.file_dir,
            format: self.file_format,
            limit: self.file_limit,
            max_size: self.file_max_size,
        })
    }
}

impl From<InnerFileLogConfig> for FileLogConfig {
    fn from(inner: InnerFileLogConfig) -> Self {
        Self {
            file_on: inner.on,
            file_level: inner.level,
            file_dir: inner.dir,
            file_format: inner.format,
            file_limit: inner.limit,
            file_max_size: inner.max_size,

            // Deprecated Fields
            file_prefix_filter: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StderrLogConfig {
    #[clap(
        long = "log-stderr-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub stderr_on: bool,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(
        long = "log-stderr-level",
        value_name = "VALUE",
        default_value = CONFIG_DEFAULT_LOG_LEVEL
    )]
    #[serde(rename = "level")]
    pub stderr_level: String,

    #[clap(
        long = "log-stderr-format",
        value_name = "VALUE",
        default_value = "text"
    )]
    #[serde(rename = "format")]
    pub stderr_format: String,
}

impl Default for StderrLogConfig {
    fn default() -> Self {
        InnerStderrLogConfig::default().into()
    }
}

impl TryInto<InnerStderrLogConfig> for StderrLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStderrLogConfig> {
        Ok(InnerStderrLogConfig {
            on: self.stderr_on,
            level: self.stderr_level,
            format: self.stderr_format,
        })
    }
}

impl From<InnerStderrLogConfig> for StderrLogConfig {
    fn from(inner: InnerStderrLogConfig) -> Self {
        Self {
            stderr_on: inner.on,
            stderr_level: inner.level,
            stderr_format: inner.format,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct OTLPLogConfig {
    #[clap(
        long = "log-otlp-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub otlp_on: bool,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(long = "log-otlp-level", value_name = "VALUE", default_value = CONFIG_DEFAULT_LOG_LEVEL)]
    #[serde(rename = "level")]
    pub otlp_level: String,

    #[clap(skip)]
    #[serde(flatten, with = "prefix_otlp")]
    pub otlp_endpoint: OTLPEndpointConfig,
}

impl Default for OTLPLogConfig {
    fn default() -> Self {
        InnerOTLPLogConfig::default().into()
    }
}

impl TryInto<InnerOTLPLogConfig> for OTLPLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerOTLPLogConfig> {
        Ok(InnerOTLPLogConfig {
            on: self.otlp_on,
            level: self.otlp_level,
            endpoint: self.otlp_endpoint.try_into()?,
        })
    }
}

impl From<InnerOTLPLogConfig> for OTLPLogConfig {
    fn from(inner: InnerOTLPLogConfig) -> Self {
        Self {
            otlp_on: inner.on,
            otlp_level: inner.level,
            otlp_endpoint: inner.endpoint.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct QueryLogConfig {
    #[clap(
        long = "log-query-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub log_query_on: bool,

    /// Query Log file dir
    #[clap(long = "log-query-dir", value_name = "VALUE", default_value = "")]
    #[serde(rename = "dir")]
    pub log_query_dir: String,

    #[clap(skip)]
    #[serde(flatten, with = "prefix_otlp")]
    pub log_query_otlp: Option<OTLPEndpointConfig>,
}

impl Default for QueryLogConfig {
    fn default() -> Self {
        InnerQueryLogConfig::default().into()
    }
}

impl TryInto<InnerQueryLogConfig> for QueryLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerQueryLogConfig> {
        Ok(InnerQueryLogConfig {
            on: self.log_query_on,
            dir: self.log_query_dir,
            otlp: self.log_query_otlp.map(|cfg| cfg.try_into()).transpose()?,
        })
    }
}

impl From<InnerQueryLogConfig> for QueryLogConfig {
    fn from(inner: InnerQueryLogConfig) -> Self {
        Self {
            log_query_on: inner.on,
            log_query_dir: inner.dir,
            log_query_otlp: inner.otlp.map(|cfg| cfg.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct ProfileLogConfig {
    #[clap(
        long = "log-profile-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub log_profile_on: bool,

    /// Profile Log file dir
    #[clap(long = "log-profile-dir", value_name = "VALUE", default_value = "")]
    #[serde(rename = "dir")]
    pub log_profile_dir: String,

    #[clap(skip)]
    #[serde(flatten, with = "prefix_otlp")]
    pub log_profile_otlp: Option<OTLPEndpointConfig>,
}

impl Default for ProfileLogConfig {
    fn default() -> Self {
        InnerProfileLogConfig::default().into()
    }
}

impl TryInto<InnerProfileLogConfig> for ProfileLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerProfileLogConfig> {
        Ok(InnerProfileLogConfig {
            on: self.log_profile_on,
            dir: self.log_profile_dir,
            otlp: self
                .log_profile_otlp
                .map(|cfg| cfg.try_into())
                .transpose()?,
        })
    }
}

impl From<InnerProfileLogConfig> for ProfileLogConfig {
    fn from(inner: InnerProfileLogConfig) -> Self {
        Self {
            log_profile_on: inner.on,
            log_profile_dir: inner.dir,
            log_profile_otlp: inner.otlp.map(|cfg| cfg.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StructLogConfig {
    #[clap(
        long = "log-structlog-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub log_structlog_on: bool,

    /// Struct Log file dir
    #[clap(long = "log-structlog-dir", value_name = "VALUE", default_value = "")]
    #[serde(rename = "dir")]
    pub log_structlog_dir: String,
}

impl Default for StructLogConfig {
    fn default() -> Self {
        InnerStructLogConfig::default().into()
    }
}

impl TryInto<InnerStructLogConfig> for StructLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStructLogConfig> {
        Ok(InnerStructLogConfig {
            on: self.log_structlog_on,
            dir: self.log_structlog_dir,
        })
    }
}

impl From<InnerStructLogConfig> for StructLogConfig {
    fn from(inner: InnerStructLogConfig) -> Self {
        Self {
            log_structlog_on: inner.on,
            log_structlog_dir: inner.dir,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct TracingConfig {
    #[clap(
        long = "log-tracing-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub tracing_on: bool,

    /// Tracing log level <DEBUG|TRACE|INFO|WARN|ERROR>
    #[clap(
        long = "log-tracing-level",
        value_name = "VALUE",
        default_value = CONFIG_DEFAULT_LOG_LEVEL
    )]
    #[serde(rename = "capture_log_level")]
    pub tracing_capture_log_level: String,

    #[clap(skip)]
    #[serde(flatten, with = "prefix_otlp")]
    pub tracing_otlp: OTLPEndpointConfig,
}

impl Default for TracingConfig {
    fn default() -> Self {
        InnerTracingConfig::default().into()
    }
}

impl TryInto<InnerTracingConfig> for TracingConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerTracingConfig> {
        Ok(InnerTracingConfig {
            on: self.tracing_on,
            capture_log_level: self.tracing_capture_log_level,
            otlp: self.tracing_otlp.try_into()?,
        })
    }
}

impl From<InnerTracingConfig> for TracingConfig {
    fn from(inner: InnerTracingConfig) -> Self {
        Self {
            tracing_on: inner.on,
            tracing_capture_log_level: inner.capture_log_level,
            tracing_otlp: inner.otlp.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct HistoryLogConfig {
    #[clap(
        long = "log-history-on", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "on")]
    pub log_history_on: bool,

    /// Enables log-only mode for the history feature.
    ///
    /// When set to true, this node will only record raw log data to the specified stage,
    /// but will not perform the transform and clean process.
    /// Please make sure that the transform and clean process is handled by other nodes
    /// otherwise the raw log data will not be processed and cleaned up.
    ///
    /// Note: This is useful for nodes that should avoid the performance overhead of the
    /// transform and clean process
    #[clap(
        long = "log-history-log-only", value_name = "VALUE", default_value = "false", action = ArgAction::Set, num_args = 0..=1, require_equals = true, default_missing_value = "true"
    )]
    #[serde(rename = "log_only")]
    pub log_history_log_only: bool,

    /// Specifies the interval in seconds for how often the history log is flushed
    #[clap(
        long = "log-history-interval",
        value_name = "VALUE",
        default_value = "2"
    )]
    #[serde(rename = "interval")]
    pub log_history_interval: usize,

    /// Specifies the name of the staging area that temporarily holds log data before it is finally copied into the table
    ///
    /// Note:
    /// The default value uses an uuid to avoid conflicts with existing stages
    #[clap(
        long = "log-history-stage-name",
        value_name = "VALUE",
        default_value = "log_1f93b76af0bd4b1d8e018667865fbc65"
    )]
    #[serde(rename = "stage_name")]
    pub log_history_stage_name: String,

    /// Log level <DEBUG|INFO|WARN|ERROR>
    #[clap(
        long = "log-history-level",
        value_name = "VALUE",
        default_value = "INFO"
    )]
    #[serde(rename = "level")]
    pub log_history_level: String,

    /// The interval (in hours) at which the retention process is triggered.
    /// Specifies how often the retention task runs to clean up old data.
    #[clap(
        long = "log-history-retention-interval",
        value_name = "VALUE",
        default_value = "24"
    )]
    #[serde(rename = "retention_interval")]
    pub log_history_retention_interval: usize,

    /// Specifies which history table to enable
    #[clap(skip)]
    #[serde(rename = "tables")]
    pub log_history_tables: Vec<HistoryLogTableConfig>,

    /// Specifies whether enable external storage
    /// If set to false (default), the default storage parameters from `[storage]` will be used.
    #[clap(skip)]
    #[serde(rename = "storage_on", default)]
    pub log_history_external_storage_on: bool,

    /// Specifies the external storage parameters for the history log
    /// This is used to configure how the history log data is stored.
    #[clap(skip)]
    #[serde(rename = "storage")]
    pub log_history_storage_params: StorageConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryLogTableConfig {
    /// Specifies the history table name
    pub table_name: String,

    /// The retention period (in hours) for history logs.
    /// Data older than this period will be deleted during retention tasks.
    pub retention: usize,
}

impl Default for HistoryLogConfig {
    fn default() -> Self {
        InnerHistoryConfig::default().into()
    }
}

impl Default for HistoryLogTableConfig {
    fn default() -> Self {
        InnerHistoryTableConfig::default().into()
    }
}

impl TryInto<InnerHistoryTableConfig> for HistoryLogTableConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerHistoryTableConfig> {
        Ok(InnerHistoryTableConfig {
            table_name: self.table_name,
            retention: self.retention,
        })
    }
}

impl From<InnerHistoryTableConfig> for HistoryLogTableConfig {
    fn from(inner: InnerHistoryTableConfig) -> Self {
        Self {
            table_name: inner.table_name,
            retention: inner.retention,
        }
    }
}

impl TryInto<InnerHistoryConfig> for HistoryLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerHistoryConfig> {
        let storage_params: Option<InnerStorageConfig> = if self.log_history_external_storage_on {
            Some(self.log_history_storage_params.try_into()?)
        } else {
            None
        };
        Ok(InnerHistoryConfig {
            on: self.log_history_on,
            log_only: self.log_history_log_only,
            interval: self.log_history_interval,
            stage_name: self.log_history_stage_name,
            level: self.log_history_level,
            retention_interval: self.log_history_retention_interval,
            tables: self
                .log_history_tables
                .into_iter()
                .map(|cfg| cfg.try_into())
                .collect::<Result<Vec<_>>>()?,
            storage_params: storage_params.map(|cfg| cfg.params),
        })
    }
}

impl From<InnerHistoryConfig> for HistoryLogConfig {
    fn from(inner: InnerHistoryConfig) -> Self {
        let inner_storage_config: Option<InnerStorageConfig> =
            inner.storage_params.map(|params| InnerStorageConfig {
                params,
                ..Default::default()
            });
        Self {
            log_history_on: inner.on,
            log_history_log_only: inner.log_only,
            log_history_interval: inner.interval,
            log_history_stage_name: inner.stage_name,
            log_history_level: inner.level,
            log_history_retention_interval: inner.retention_interval,
            log_history_tables: inner.tables.into_iter().map(Into::into).collect(),
            log_history_external_storage_on: inner_storage_config.is_some(),
            log_history_storage_params: inner_storage_config.map(Into::into).unwrap_or_default(),
        }
    }
}

with_prefix!(prefix_otlp "otlp_");

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct OTLPEndpointConfig {
    /// Log OpenTelemetry OTLP endpoint
    pub endpoint: String,

    /// Log OpenTelemetry OTLP protocol
    #[clap(skip)]
    pub protocol: OTLPProtocol,

    /// Log OpenTelemetry Labels
    #[clap(skip)]
    pub labels: BTreeMap<String, String>,
}

impl Default for OTLPEndpointConfig {
    fn default() -> Self {
        InnerOTLPEndpointConfig::default().into()
    }
}

impl TryInto<InnerOTLPEndpointConfig> for OTLPEndpointConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerOTLPEndpointConfig> {
        Ok(InnerOTLPEndpointConfig {
            endpoint: self.endpoint,
            protocol: self.protocol,
            labels: self.labels,
        })
    }
}

impl From<InnerOTLPEndpointConfig> for OTLPEndpointConfig {
    fn from(inner: InnerOTLPEndpointConfig) -> Self {
        Self {
            endpoint: inner.endpoint,
            protocol: inner.protocol,
            labels: inner.labels,
        }
    }
}

/// Meta config group.
/// TODO(xuanwo): All meta_xxx should be rename to xxx.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    #[clap(long = "meta-embedded-dir", value_name = "VALUE", default_value_t)]
    pub embedded_dir: String,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub meta_embedded_dir: Option<String>,

    /// MetaStore backend endpoints
    #[clap(
        long = "meta-endpoints",
        value_name = "VALUE",
        help = "MetaStore peers endpoints"
    )]
    pub endpoints: Vec<String>,

    /// MetaStore backend user name
    #[clap(long = "meta-username", value_name = "VALUE", default_value = "root")]
    pub username: String,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub meta_username: Option<String>,

    /// MetaStore backend user password
    #[clap(long = "meta-password", value_name = "VALUE", default_value_t)]
    pub password: String,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub meta_password: Option<String>,

    /// Timeout for each client request, in seconds
    #[clap(
        long = "meta-client-timeout-in-second",
        value_name = "VALUE",
        default_value = "4"
    )]
    pub client_timeout_in_second: u64,

    /// Deprecated fields, used for catching error, will be removed later.
    #[clap(skip)]
    pub meta_client_timeout_in_second: Option<u64>,

    /// AutoSyncInterval is the interval to update endpoints with its latest members.
    /// 0 disables auto-sync. By default auto-sync is disabled.
    #[clap(long = "auto-sync-interval", value_name = "VALUE", default_value = "0")]
    pub auto_sync_interval: u64,

    #[clap(
        long = "unhealth-endpoint-evict-time",
        value_name = "VALUE",
        default_value = "120"
    )]
    pub unhealth_endpoint_evict_time: u64,

    /// Certificate for client to identify meta rpc serve
    #[clap(
        long = "meta-rpc-tls-meta-server-root-ca-cert",
        value_name = "VALUE",
        default_value_t
    )]
    pub rpc_tls_meta_server_root_ca_cert: String,

    #[clap(
        long = "meta-rpc-tls-meta-service-domain-name",
        value_name = "VALUE",
        default_value = "localhost"
    )]
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
        if self.meta_embedded_dir.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`meta_embedded_dir` is deprecated, use `embedded_dir` instead".to_string(),
            ));
        }
        if self.meta_username.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`meta_username` is deprecated, use `username` instead".to_string(),
            ));
        }
        if self.meta_password.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`meta_password` is deprecated, use `password` instead".to_string(),
            ));
        }
        if self.meta_client_timeout_in_second.is_some() {
            return Err(ErrorCode::InvalidConfig(
                "`meta_client_timeout_in_second` is deprecated, use `client_timeout_in_second` instead"
                    .to_string(),
            ));
        }

        Ok(InnerMetaConfig {
            embedded_dir: self.embedded_dir,
            endpoints: self.endpoints,
            username: self.username,
            password: self.password,
            client_timeout_in_second: self.client_timeout_in_second,
            auto_sync_interval: self.auto_sync_interval,
            unhealth_endpoint_evict_time: self.unhealth_endpoint_evict_time,
            rpc_tls_meta_server_root_ca_cert: self.rpc_tls_meta_server_root_ca_cert,
            rpc_tls_meta_service_domain_name: self.rpc_tls_meta_service_domain_name,
        })
    }
}

impl From<InnerMetaConfig> for MetaConfig {
    fn from(inner: InnerMetaConfig) -> Self {
        Self {
            embedded_dir: inner.embedded_dir,
            endpoints: inner.endpoints,
            username: inner.username,
            password: inner.password,
            client_timeout_in_second: inner.client_timeout_in_second,
            auto_sync_interval: inner.auto_sync_interval,
            unhealth_endpoint_evict_time: inner.unhealth_endpoint_evict_time,
            rpc_tls_meta_server_root_ca_cert: inner.rpc_tls_meta_server_root_ca_cert,
            rpc_tls_meta_service_domain_name: inner.rpc_tls_meta_service_domain_name,

            // Deprecated fields
            meta_embedded_dir: None,
            meta_username: None,
            meta_password: None,
            meta_client_timeout_in_second: None,
        }
    }
}

impl Debug for MetaConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("MetaConfig")
            .field("endpoints", &self.endpoints)
            .field("username", &self.username)
            .field("password", &mask_string(&self.password, 3))
            .field("embedded_dir", &self.embedded_dir)
            .field("client_timeout_in_second", &self.client_timeout_in_second)
            .field("auto_sync_interval", &self.auto_sync_interval)
            .field(
                "unhealth_endpoint_evict_time",
                &self.unhealth_endpoint_evict_time,
            )
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct LocalConfig {
    // sql to run
    #[clap(long, value_name = "VALUE", default_value = "SELECT 1")]
    pub sql: String,

    // name1=filepath1,name2=filepath2
    #[clap(long, value_name = "VALUE", default_value = "")]
    pub table: String,
}

impl Default for LocalConfig {
    fn default() -> Self {
        InnerLocalConfig::default().into()
    }
}

impl From<InnerLocalConfig> for LocalConfig {
    fn from(inner: InnerLocalConfig) -> Self {
        Self {
            sql: inner.sql,
            table: inner.table,
        }
    }
}

impl TryInto<InnerLocalConfig> for LocalConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerLocalConfig> {
        Ok(InnerLocalConfig {
            sql: self.sql,
            table: self.table,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct CacheConfig {
    /// The data in meta-service using key `TenantOwnershipObjectIdent`
    #[clap(long = "cache-enable-meta-service-ownership", default_value = "false")]
    #[serde(default = "bool_false")]
    pub meta_service_ownership_cache: bool,

    /// Enable table meta cache. Default is enabled. Set it to false to disable all the table meta caches
    #[clap(long = "cache-enable-table-meta-cache", default_value = "true")]
    #[serde(default = "bool_true")]
    pub enable_table_meta_cache: bool,

    /// Max number of cached table snapshot
    #[clap(
        long = "cache-table-meta-snapshot-count",
        value_name = "VALUE",
        default_value = "256"
    )]
    pub table_meta_snapshot_count: u64,

    /// Max bytes of cached table segment
    #[clap(
        long = "cache-table-meta-segment-bytes",
        value_name = "VALUE",
        default_value = "1073741824"
    )]
    pub table_meta_segment_bytes: u64,

    /// Max number of cached table block meta
    #[clap(
        long = "cache-block-meta-count",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub block_meta_count: u64,

    /// Max number of **segment** which all of its block meta will be cached.
    /// Note that a segment may contain multiple block metadata entries.
    #[clap(
        long = "cache-segment-block-metas-count",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub segment_block_metas_count: u64,

    /// Max number of cached table statistic meta
    #[clap(
        long = "cache-table-meta-statistic-count",
        value_name = "VALUE",
        default_value = "256"
    )]
    pub table_meta_statistic_count: u64,

    /// Enable bloom index cache. Default is enabled. Set it to false to disable all the bloom index caches
    #[clap(
        long = "cache-enable-table-bloom-index-cache",
        value_name = "VALUE",
        default_value = "true"
    )]
    #[serde(default = "bool_true")]
    pub enable_table_bloom_index_cache: bool,

    /// Max number of cached bloom index meta objects. Set it to 0 to disable it.
    #[clap(
        long = "cache-table-bloom-index-meta-count",
        value_name = "VALUE",
        default_value = "3000"
    )]
    pub table_bloom_index_meta_count: u64,

    /// Max bytes of cached bloom index metadata on disk. The Default value is 0.
    // Set it to 0 to disable it.
    #[clap(
        long = "disk-cache-table-bloom-index-meta-size",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub disk_cache_table_bloom_index_meta_size: u64,

    /// DEPRECATING, will be deprecated in the next production release.
    ///
    /// Max number of cached bloom index filters. Set it to 0 to disable it.
    // One bloom index filter per column of data block being indexed will be generated if necessary.
    //
    // For example, a table of 1024 columns, with 800 data blocks, a query that triggers a full
    // table filter on 2 columns, might populate 2 * 800 bloom index filter cache items (at most)
    #[clap(
        long = "cache-table-bloom-index-filter-count",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub table_bloom_index_filter_count: u64,

    /// Max bytes of cached bloom index filters used. Set it to 0 to disable it.
    // One bloom index filter per column of data block being indexed will be generated if necessary.
    #[clap(
        long = "cache-table-bloom-index-filter-size",
        value_name = "VALUE",
        default_value = "2147483648"
    )]
    pub table_bloom_index_filter_size: u64,

    /// Max on-disk bytes of cached bloom index filters used. The default value of it is 0.
    #[clap(
        long = "disk-cache-table-bloom-index-data-size",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub disk_cache_table_bloom_index_data_size: u64,

    /// Max number of cached inverted index meta objects. Set it to 0 to disable it.
    #[clap(
        long = "cache-inverted-index-meta-count",
        value_name = "VALUE",
        default_value = "3000"
    )]
    pub inverted_index_meta_count: u64,

    /// Max bytes of cached inverted index filters used. Set it to 0 to disable it.
    #[clap(
        long = "cache-inverted-index-filter-size",
        value_name = "VALUE",
        default_value = "2147483648"
    )]
    pub inverted_index_filter_size: u64,

    /// Max percentage of in memory inverted index filter cache relative to whole memory. By default it is 0 (disabled).
    #[clap(
        long = "cache-inverted-index-filter-memory-ratio",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub inverted_index_filter_memory_ratio: u64,

    /// Max number of cached vector index meta objects. Set it to 0 to disable it.
    #[clap(
        long = "cache-vector-index-meta-count",
        value_name = "VALUE",
        default_value = "3000"
    )]
    pub vector_index_meta_count: u64,

    /// Max bytes of cached vector index filters used. Set it to 0 to disable it.
    #[clap(
        long = "cache-vector-index-filter-size",
        value_name = "VALUE",
        default_value = "2147483648"
    )]
    pub vector_index_filter_size: u64,

    /// Max percentage of in memory vector index filter cache relative to whole memory. By default it is 0 (disabled).
    #[clap(
        long = "cache-vector-index-filter-memory-ratio",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub vector_index_filter_memory_ratio: u64,

    #[clap(
        long = "cache-table-prune-partitions-count",
        value_name = "VALUE",
        default_value = "256"
    )]
    pub table_prune_partitions_count: u64,

    /// Type of data cache storage
    #[clap(
        long = "cache-data-cache-storage",
        value_name = "VALUE",
        value_enum,
        default_value_t
    )]
    pub data_cache_storage: CacheStorageTypeConfig,

    /// Policy of disk cache restart
    #[clap(
        long = "cache-data-cache-key-reload-policy",
        value_name = "VALUE",
        value_enum,
        default_value_t
    )]
    pub data_cache_key_reload_policy: DiskCacheKeyReloadPolicy,

    /// Max size of external cache population queue length
    ///
    /// the items being queued reference table column raw data, which are
    /// un-deserialized and usually compressed (depends on table compression options).
    ///
    /// - please monitor the 'table_data_cache_population_pending_count' metric
    ///   if it is too high, and takes too much memory, please consider decrease this value
    ///
    /// - please monitor the 'population_overflow_count' metric
    ///   if it keeps increasing, and disk cache hits rate is not as expected. please consider
    ///   increase this value.
    ///
    /// default value is 0, which means queue size will be adjusted automatically based on
    /// number of CPU cores.
    #[clap(
        long = "cache-data-cache-population-queue-size",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub table_data_cache_population_queue_size: u32,

    /// Bytes of data cache in-memory
    #[clap(
        long = "cache-data-cache-in-memory-bytes",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub data_cache_in_memory_bytes: u64,

    /// Storage that hold the data caches
    #[clap(flatten)]
    #[serde(rename = "disk")]
    pub disk_cache_config: DiskCacheConfig,

    /// Max size of in memory table column object cache. By default it is 0 (disabled)
    ///
    /// CAUTION: The cache items are deserialized table column objects, may take a lot of memory.
    ///
    /// Only if query nodes have plenty of unused memory, the working set can be fitted into,
    /// and the access pattern will benefit from caching, consider enabled this cache.
    #[clap(
        long = "cache-table-data-deserialized-data-bytes",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub table_data_deserialized_data_bytes: u64,

    /// Max percentage of in memory table column object cache relative to whole memory. By default it is 0 (disabled)
    ///
    /// CAUTION: The cache items are deserialized table column objects, may take a lot of memory.
    ///
    /// Only if query nodes have plenty of unused memory, the working set can be fitted into,
    /// and the access pattern will benefit from caching, consider enabled this cache.
    #[clap(
        long = "cache-table-data-deserialized-memory-ratio",
        value_name = "VALUE",
        default_value = "0"
    )]
    pub table_data_deserialized_memory_ratio: u64,

    #[clap(
        long = "cache-iceberg-table-meta-count",
        value_name = "VALUE",
        default_value = "1024"
    )]
    pub iceberg_table_meta_count: u64,

    // ----- the following options/args are all deprecated               ----
    /// Max number of cached table segment
    #[clap(long = "cache-table-meta-segment-count", value_name = "VALUE")]
    pub table_meta_segment_count: Option<u64>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        // Here we should (have to) convert self from the default value of  inner::CacheConfig :(
        super::inner::CacheConfig::default().into()
    }
}

#[inline]
fn bool_true() -> bool {
    true
}

#[inline]
fn bool_false() -> bool {
    false
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum CacheStorageTypeConfig {
    None,
    Disk,
    // Redis,
}

impl Default for CacheStorageTypeConfig {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum DiskCacheKeyReloadPolicy {
    // remove all the disk cache during restart
    Reset,
    // recovery the cache keys during restart,
    // but cache capacity will not be checked
    Fuzzy,
}

impl Default for DiskCacheKeyReloadPolicy {
    fn default() -> Self {
        Self::Reset
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct DiskCacheConfig {
    /// Max bytes of cached raw table data. Default 20GB, set it to 0 to disable it.
    #[clap(
        long = "cache-disk-max-bytes",
        value_name = "VALUE",
        default_value = "21474836480"
    )]
    pub max_bytes: u64,

    /// Table disk cache root path
    #[clap(
        long = "cache-disk-path",
        value_name = "VALUE",
        default_value = "./.databend/_cache"
    )]
    pub path: String,

    /// Controls whether to synchronize data to disk after write operations.
    ///
    /// When enabled, this option forces written data to be flushed to physical storage,
    /// reducing the amount of dirty pages in memory. This is particularly important in
    /// containerized environments where:
    ///
    /// 1. Memory limits are enforced by cgroups (v1, maybe v2 as well, though accounting differs)
    /// 2. Dirty pages are counted against the memory limit, which increases the possibility of triggering OOM kills
    ///
    /// Setting this to true improves stability in memory-constrained / containerized environments at the cost
    /// of potentially reduced write performance.
    #[clap(
        long = "cache-disk-sync-data",
        value_name = "VALUE",
        default_value = "true"
    )]
    #[serde(default = "bool_true")]
    pub sync_data: bool,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        inner::DiskCacheConfig::default().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct SpillConfig {
    /// Path of spill to local disk. disable if it's empty.
    #[clap(long, value_name = "VALUE", default_value = "")]
    pub spill_local_disk_path: String,

    #[clap(long, value_name = "VALUE", default_value = "30")]
    /// Percentage of reserve disk space that won't be used for spill to local disk.
    pub spill_local_disk_reserved_space_percentage: OrderedFloat<f64>,

    #[clap(long, value_name = "VALUE", default_value = "18446744073709551615")]
    /// Allow space in bytes to spill to local disk.
    pub spill_local_disk_max_bytes: u64,

    // TODO: We need to fix StorageConfig so that it supports environment variables and command line injections.
    #[clap(skip)]
    pub storage: Option<StorageConfig>,
}

impl Default for SpillConfig {
    fn default() -> Self {
        inner::SpillConfig::default().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args, Default)]
#[serde(default)]
pub struct ResourcesManagementConfig {
    #[clap(long = "type", value_name = "VALUE", default_value = "self_managed")]
    #[serde(rename = "type")]
    pub typ: String,

    #[clap(long, value_name = "VALUE")]
    pub node_group: Option<String>,
}

mod cache_config_converters {
    use log::warn;

    use super::*;

    impl From<InnerConfig> for Config {
        fn from(inner: InnerConfig) -> Self {
            Self {
                subcommand: inner.subcommand,
                cmd: None,
                config_file: inner.config_file,
                query: inner.query.into(),
                log: inner.log.into(),
                meta: inner.meta.into(),
                storage: inner.storage.into(),
                catalog: HiveCatalogConfig::default(),

                catalogs: inner
                    .catalogs
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                cache: inner.cache.into(),
                spill: inner.spill.into(),
            }
        }
    }

    impl TryInto<InnerConfig> for Config {
        type Error = ErrorCode;

        fn try_into(self) -> Result<InnerConfig> {
            let Config {
                subcommand,
                config_file,
                query,
                log,
                meta,
                storage,
                catalog,
                cache,
                spill,
                catalogs: input_catalogs,
                ..
            } = self;

            let mut catalogs = HashMap::new();
            for (k, v) in input_catalogs.into_iter() {
                let catalog = v.try_into()?;
                catalogs.insert(k, catalog);
            }
            if !catalog.address.is_empty() || !catalog.protocol.is_empty() {
                warn!(
                    "`catalog` is planned to be deprecated, please add catalog in `catalogs` instead"
                );
                let hive = catalog.try_into()?;
                let catalog = InnerCatalogConfig::Hive(hive);
                catalogs.insert(CATALOG_HIVE.to_string(), catalog);
            }

            let spill = convert_local_spill_config(spill, &cache.disk_cache_config)?;

            Ok(InnerConfig {
                subcommand,
                config_file,
                query: query.try_into()?,
                log: log.try_into()?,
                meta: meta.try_into()?,
                storage: storage.try_into()?,
                catalogs,
                cache: cache.try_into()?,
                spill,
            })
        }
    }

    impl TryFrom<CacheConfig> for inner::CacheConfig {
        type Error = ErrorCode;

        fn try_from(value: CacheConfig) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                meta_service_ownership_cache: value.meta_service_ownership_cache,
                enable_table_meta_cache: value.enable_table_meta_cache,
                table_meta_snapshot_count: value.table_meta_snapshot_count,
                table_meta_segment_bytes: value.table_meta_segment_bytes,
                block_meta_count: value.block_meta_count,
                segment_block_metas_count: value.segment_block_metas_count,
                table_meta_statistic_count: value.table_meta_statistic_count,
                enable_table_index_bloom: value.enable_table_bloom_index_cache,
                table_bloom_index_meta_count: value.table_bloom_index_meta_count,
                table_bloom_index_filter_count: value.table_bloom_index_filter_count,
                table_bloom_index_filter_size: value.table_bloom_index_filter_size,
                disk_cache_table_bloom_index_data_size: value
                    .disk_cache_table_bloom_index_data_size,
                inverted_index_meta_count: value.inverted_index_meta_count,
                inverted_index_filter_size: value.inverted_index_filter_size,
                inverted_index_filter_memory_ratio: value.inverted_index_filter_memory_ratio,
                vector_index_meta_count: value.vector_index_meta_count,
                vector_index_filter_size: value.vector_index_filter_size,
                vector_index_filter_memory_ratio: value.vector_index_filter_memory_ratio,
                table_prune_partitions_count: value.table_prune_partitions_count,
                data_cache_storage: value.data_cache_storage.try_into()?,
                table_data_cache_population_queue_size: value
                    .table_data_cache_population_queue_size,
                data_cache_in_memory_bytes: value.data_cache_in_memory_bytes,
                disk_cache_config: value.disk_cache_config.try_into()?,
                data_cache_key_reload_policy: value.data_cache_key_reload_policy.try_into()?,
                table_data_deserialized_data_bytes: value.table_data_deserialized_data_bytes,
                table_data_deserialized_memory_ratio: value.table_data_deserialized_memory_ratio,
                iceberg_table_meta_count: value.iceberg_table_meta_count,
                disk_cache_table_bloom_index_meta_size: value
                    .disk_cache_table_bloom_index_meta_size,
            })
        }
    }

    impl From<inner::CacheConfig> for CacheConfig {
        fn from(value: inner::CacheConfig) -> Self {
            Self {
                meta_service_ownership_cache: value.meta_service_ownership_cache,
                enable_table_meta_cache: value.enable_table_meta_cache,
                table_meta_snapshot_count: value.table_meta_snapshot_count,
                table_meta_segment_bytes: value.table_meta_segment_bytes,
                table_meta_statistic_count: value.table_meta_statistic_count,
                block_meta_count: value.block_meta_count,
                enable_table_bloom_index_cache: value.enable_table_index_bloom,
                table_bloom_index_meta_count: value.table_bloom_index_meta_count,
                disk_cache_table_bloom_index_meta_size: value
                    .disk_cache_table_bloom_index_meta_size,
                table_bloom_index_filter_count: value.table_bloom_index_filter_count,
                table_bloom_index_filter_size: value.table_bloom_index_filter_size,
                disk_cache_table_bloom_index_data_size: value
                    .disk_cache_table_bloom_index_data_size,
                inverted_index_meta_count: value.inverted_index_meta_count,
                inverted_index_filter_size: value.inverted_index_filter_size,
                inverted_index_filter_memory_ratio: value.inverted_index_filter_memory_ratio,
                vector_index_meta_count: value.vector_index_meta_count,
                vector_index_filter_size: value.vector_index_filter_size,
                vector_index_filter_memory_ratio: value.vector_index_filter_memory_ratio,
                table_prune_partitions_count: value.table_prune_partitions_count,
                data_cache_storage: value.data_cache_storage.into(),
                data_cache_key_reload_policy: value.data_cache_key_reload_policy.into(),
                table_data_cache_population_queue_size: value
                    .table_data_cache_population_queue_size,
                data_cache_in_memory_bytes: value.data_cache_in_memory_bytes,
                disk_cache_config: value.disk_cache_config.into(),
                table_data_deserialized_data_bytes: value.table_data_deserialized_data_bytes,
                table_data_deserialized_memory_ratio: value.table_data_deserialized_memory_ratio,
                iceberg_table_meta_count: value.iceberg_table_meta_count,
                table_meta_segment_count: None,
                segment_block_metas_count: value.segment_block_metas_count,
            }
        }
    }

    fn convert_local_spill_config(
        spill: SpillConfig,
        cache: &DiskCacheConfig,
    ) -> Result<inner::SpillConfig> {
        // Trick for cloud, perhaps we should introduce a new configuration for the local writeable root.
        let local_writeable_root = if cache.path != DiskCacheConfig::default().path
            && spill.spill_local_disk_path.is_empty()
        {
            Some(cache.path.clone())
        } else {
            None
        };

        let storage_params = spill
            .storage
            .map(|storage| {
                let storage: InnerStorageConfig = storage.try_into()?;
                Ok::<_, ErrorCode>(storage.params)
            })
            .transpose()?;

        Ok(inner::SpillConfig {
            local_writeable_root,
            path: spill.spill_local_disk_path,
            reserved_disk_ratio: spill.spill_local_disk_reserved_space_percentage / 100.0,
            global_bytes_limit: spill.spill_local_disk_max_bytes,
            storage_params,
        })
    }

    impl From<inner::SpillConfig> for SpillConfig {
        fn from(value: inner::SpillConfig) -> Self {
            let storage = value.storage_params.map(|params| {
                InnerStorageConfig {
                    params,
                    ..Default::default()
                }
                .into()
            });

            Self {
                spill_local_disk_path: value.path,
                spill_local_disk_reserved_space_percentage: value.reserved_disk_ratio * 100.0,
                spill_local_disk_max_bytes: value.global_bytes_limit,
                storage,
            }
        }
    }

    impl TryFrom<DiskCacheConfig> for inner::DiskCacheConfig {
        type Error = ErrorCode;
        fn try_from(value: DiskCacheConfig) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                max_bytes: value.max_bytes,
                path: value.path,
                sync_data: value.sync_data,
            })
        }
    }

    impl From<inner::DiskCacheConfig> for DiskCacheConfig {
        fn from(value: inner::DiskCacheConfig) -> Self {
            Self {
                max_bytes: value.max_bytes,
                path: value.path,
                sync_data: value.sync_data,
            }
        }
    }

    impl TryFrom<CacheStorageTypeConfig> for inner::CacheStorageTypeConfig {
        type Error = ErrorCode;
        fn try_from(value: CacheStorageTypeConfig) -> std::result::Result<Self, Self::Error> {
            Ok(match value {
                CacheStorageTypeConfig::None => inner::CacheStorageTypeConfig::None,
                CacheStorageTypeConfig::Disk => inner::CacheStorageTypeConfig::Disk,
            })
        }
    }

    impl From<inner::CacheStorageTypeConfig> for CacheStorageTypeConfig {
        fn from(value: inner::CacheStorageTypeConfig) -> Self {
            match value {
                inner::CacheStorageTypeConfig::None => CacheStorageTypeConfig::None,
                inner::CacheStorageTypeConfig::Disk => CacheStorageTypeConfig::Disk,
            }
        }
    }

    impl TryFrom<DiskCacheKeyReloadPolicy> for inner::DiskCacheKeyReloadPolicy {
        type Error = ErrorCode;
        fn try_from(value: DiskCacheKeyReloadPolicy) -> std::result::Result<Self, Self::Error> {
            Ok(match value {
                DiskCacheKeyReloadPolicy::Reset => inner::DiskCacheKeyReloadPolicy::Reset,
                DiskCacheKeyReloadPolicy::Fuzzy => inner::DiskCacheKeyReloadPolicy::Fuzzy,
            })
        }
    }

    impl From<inner::DiskCacheKeyReloadPolicy> for DiskCacheKeyReloadPolicy {
        fn from(value: inner::DiskCacheKeyReloadPolicy) -> Self {
            match value {
                inner::DiskCacheKeyReloadPolicy::Reset => DiskCacheKeyReloadPolicy::Reset,
                inner::DiskCacheKeyReloadPolicy::Fuzzy => DiskCacheKeyReloadPolicy::Fuzzy,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::ffi::OsString;

    use clap::Parser;
    use pretty_assertions::assert_eq;

    use crate::Config;
    use crate::InnerConfig;

    /// It's required to make sure setting's default value is the same with clap.
    #[test]
    fn test_config_default() {
        let setting_default = InnerConfig::default();
        let config_default: InnerConfig = Config::parse_from(Vec::<OsString>::new())
            .try_into()
            .expect("parse from args must succeed");

        assert_eq!(
            setting_default, config_default,
            "default setting is different from default config, please check again"
        )
    }
}
