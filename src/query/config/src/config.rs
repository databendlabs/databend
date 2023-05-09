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
use std::env;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use clap::Args;
use clap::Parser;
use clap::ValueEnum;
use common_base::base::mask_string;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::AuthType;
use common_meta_app::storage::StorageAzblobConfig as InnerStorageAzblobConfig;
use common_meta_app::storage::StorageFsConfig as InnerStorageFsConfig;
use common_meta_app::storage::StorageGcsConfig as InnerStorageGcsConfig;
use common_meta_app::storage::StorageHdfsConfig as InnerStorageHdfsConfig;
use common_meta_app::storage::StorageMokaConfig as InnerStorageMokaConfig;
use common_meta_app::storage::StorageObsConfig as InnerStorageObsConfig;
use common_meta_app::storage::StorageOssConfig as InnerStorageOssConfig;
use common_meta_app::storage::StorageParams;
use common_meta_app::storage::StorageRedisConfig as InnerStorageRedisConfig;
use common_meta_app::storage::StorageS3Config as InnerStorageS3Config;
use common_meta_app::storage::StorageWebhdfsConfig as InnerStorageWebhdfsConfig;
use common_meta_app::tenant::TenantQuota;
use common_storage::StorageConfig as InnerStorageConfig;
use common_tracing::Config as InnerLogConfig;
use common_tracing::FileConfig as InnerFileLogConfig;
use common_tracing::StderrConfig as InnerStderrLogConfig;
use common_users::idm_config::IDMConfig as InnerIDMConfig;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;

use super::inner;
use super::inner::CatalogConfig as InnerCatalogConfig;
use super::inner::CatalogHiveConfig as InnerCatalogHiveConfig;
use super::inner::InnerConfig;
use super::inner::LocalConfig as InnerLocalConfig;
use super::inner::MetaConfig as InnerMetaConfig;
use super::inner::QueryConfig as InnerQueryConfig;
use crate::DATABEND_COMMIT_VERSION;

// FIXME: too much boilerplate here

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
#[clap(name = "databend-query", about, version = &**DATABEND_COMMIT_VERSION, author)]
#[serde(default)]
pub struct Config {
    /// Run a command and quit
    #[clap(long, default_value_t)]
    pub cmd: String,

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

    /// Note: Legacy Config API
    ///
    /// When setting its all fields to empty strings, it will be ignored
    ///
    /// external catalog config.
    /// - Later, catalog information SHOULD be kept in KV Service
    /// - currently only supports HIVE (via hive meta store)
    #[clap(flatten)]
    pub catalog: HiveCatalogConfig,

    // Local query config.
    #[clap(flatten)]
    pub local: LocalConfig,

    // cache configs
    #[clap(flatten)]
    pub cache: CacheConfig,

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

            if !config_file.is_empty() {
                builder = builder.collect(from_file(Toml, &config_file));
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
/// [storage.cache]
/// type = "redis"
///
/// [storage.temporary]
/// type = "s3"
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StorageConfig {
    #[clap(long, default_value = "fs")]
    #[serde(rename = "type", alias = "storage_type")]
    pub storage_type: String,

    #[clap(long, default_value_t)]
    #[serde(rename = "num_cpus", alias = "storage_num_cpus")]
    pub storage_num_cpus: u64,

    #[clap(long = "storage-allow-insecure")]
    pub allow_insecure: bool,

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
            StorageParams::Gcs(v) => {
                cfg.storage_type = "gcs".to_string();
                cfg.gcs = v.into()
            }
            StorageParams::Obs(v) => {
                cfg.storage_type = "obs".to_string();
                cfg.obs = v.into()
            }
            StorageParams::Oss(v) => {
                cfg.storage_type = "oss".to_string();
                cfg.oss = v.into()
            }
            StorageParams::Webhdfs(v) => {
                cfg.storage_type = "webhdfs".to_string();
                cfg.webhdfs = v.into()
            }
            v => unreachable!("{v:?} should not be used as storage backend"),
        }

        cfg
    }
}

impl TryInto<InnerStorageConfig> for StorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageConfig> {
        Ok(InnerStorageConfig {
            num_cpus: self.storage_num_cpus,
            allow_insecure: self.allow_insecure,
            params: {
                match self.storage_type.as_str() {
                    "azblob" => StorageParams::Azblob(self.azblob.try_into()?),
                    "fs" => StorageParams::Fs(self.fs.try_into()?),
                    "gcs" => StorageParams::Gcs(self.gcs.try_into()?),
                    #[cfg(feature = "storage-hdfs")]
                    "hdfs" => StorageParams::Hdfs(self.hdfs.try_into()?),
                    "memory" => StorageParams::Memory,
                    "s3" => StorageParams::S3(self.s3.try_into()?),
                    "obs" => StorageParams::Obs(self.obs.try_into()?),
                    "oss" => StorageParams::Oss(self.oss.try_into()?),
                    "webhdfs" => StorageParams::Webhdfs(self.webhdfs.try_into()?),
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
    #[serde(alias = "meta_store_address")]
    pub address: String,
    pub protocol: String,
}

/// this is the legacy version of external catalog configuration
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct HiveCatalogConfig {
    #[clap(long = "hive-meta-store-address", default_value_t)]
    #[serde(rename = "address", alias = "meta_store_address")]
    pub meta_store_address: String,
    #[clap(long = "hive-thrift-protocol", default_value_t)]
    pub protocol: String,
}

impl TryInto<InnerCatalogConfig> for CatalogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerCatalogConfig, Self::Error> {
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
    fn try_into(self) -> Result<InnerCatalogHiveConfig, Self::Error> {
        Ok(InnerCatalogHiveConfig {
            address: self.address,
            protocol: self.protocol.parse()?,
        })
    }
}

impl From<InnerCatalogHiveConfig> for CatalogsHiveConfig {
    fn from(inner: InnerCatalogHiveConfig) -> Self {
        Self {
            address: inner.address,
            protocol: inner.protocol.to_string(),
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
    fn try_into(self) -> Result<InnerCatalogHiveConfig, Self::Error> {
        Ok(InnerCatalogHiveConfig {
            address: self.meta_store_address,
            protocol: self.protocol.parse()?,
        })
    }
}

impl From<InnerCatalogHiveConfig> for HiveCatalogConfig {
    fn from(inner: InnerCatalogHiveConfig) -> Self {
        Self {
            meta_store_address: inner.address,
            protocol: inner.protocol.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct GcsStorageConfig {
    #[clap(
        long = "storage-gcs-endpoint-url",
        default_value = "https://storage.googleapis.com"
    )]
    #[serde(rename = "endpoint_url")]
    pub gcs_endpoint_url: String,

    #[clap(long = "storage-gcs-bucket", default_value_t)]
    #[serde(rename = "bucket")]
    pub gcs_bucket: String,

    #[clap(long = "storage-gcs-root", default_value_t)]
    #[serde(rename = "root")]
    pub gcs_root: String,

    #[clap(long = "storage-gcs-credential", default_value_t)]
    pub credential: String,
}

impl Default for GcsStorageConfig {
    fn default() -> Self {
        InnerStorageGcsConfig::default().into()
    }
}

impl Debug for GcsStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

    fn try_into(self) -> Result<InnerStorageGcsConfig, Self::Error> {
        Ok(InnerStorageGcsConfig {
            endpoint_url: self.gcs_endpoint_url,
            bucket: self.gcs_bucket,
            root: self.gcs_root,
            credential: self.credential,
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

    /// Security token for S3 storage
    ///
    /// Check out [documents](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) for details
    #[clap(long = "storage-s3-security-token", default_value_t)]
    pub security_token: String,

    /// S3 Bucket to use for storage
    #[clap(long = "storage-s3-bucket", default_value_t)]
    pub bucket: String,

    /// <root>
    #[clap(long = "storage-s3-root", default_value_t)]
    pub root: String,

    // TODO(xuanwo): We should support both AWS SSE and CSE in the future.
    #[clap(long = "storage-s3-master-key", default_value_t)]
    pub master_key: String,

    #[clap(long = "storage-s3-enable-virtual-host-style")]
    pub enable_virtual_host_style: bool,

    #[clap(long = "storage-s3-role-arn", default_value_t)]
    #[serde(rename = "role_arn")]
    pub s3_role_arn: String,

    #[clap(long = "storage-s3-external-id", default_value_t)]
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
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args, Debug)]
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct ObsStorageConfig {
    /// Access key for OBS storage
    #[clap(long = "storage-obs-access-key-id", default_value_t)]
    #[serde(rename = "access_key_id")]
    pub obs_access_key_id: String,

    /// Secret key for OBS storage
    #[clap(long = "storage-obs-secret-access-key", default_value_t)]
    #[serde(rename = "secret_access_key")]
    pub obs_secret_access_key: String,

    /// Bucket for OBS
    #[clap(long = "storage-obs-bucket", default_value_t)]
    #[serde(rename = "bucket")]
    pub obs_bucket: String,

    /// Endpoint URL for OBS
    ///
    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use endpoint_url directly.
    #[clap(long = "storage-obs-endpoint-url", default_value_t)]
    #[serde(rename = "endpoint_url")]
    pub obs_endpoint_url: String,

    /// # TODO(xuanwo)
    ///
    /// Clap doesn't allow us to use root directly.
    #[clap(long = "storage-obs-root", default_value_t)]
    #[serde(rename = "root")]
    pub obs_root: String,
}

impl Default for ObsStorageConfig {
    fn default() -> Self {
        InnerStorageObsConfig::default().into()
    }
}

impl Debug for ObsStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct OssStorageConfig {
    /// Access key id for OSS storage
    #[clap(long = "storage-oss-access-key-id", default_value_t)]
    #[serde(rename = "access_key_id")]
    pub oss_access_key_id: String,

    /// Access Key Secret for OSS storage
    #[clap(long = "storage-oss-access-key-secret", default_value_t)]
    #[serde(rename = "access_key_secret")]
    pub oss_access_key_secret: String,

    /// Bucket for OSS
    #[clap(long = "storage-oss-bucket", default_value_t)]
    #[serde(rename = "bucket")]
    pub oss_bucket: String,

    #[clap(long = "storage-oss-endpoint-url", default_value_t)]
    #[serde(rename = "endpoint_url")]
    pub oss_endpoint_url: String,

    #[clap(long = "storage-oss-presign-endpoint-url", default_value_t)]
    #[serde(rename = "presign_endpoint_url")]
    pub oss_presign_endpoint_url: String,

    #[clap(long = "storage-oss-root", default_value_t)]
    #[serde(rename = "root")]
    pub oss_root: String,
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
        }
    }
}

impl TryInto<InnerStorageOssConfig> for OssStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageOssConfig, Self::Error> {
        Ok(InnerStorageOssConfig {
            endpoint_url: self.oss_endpoint_url,
            presign_endpoint_url: self.oss_presign_endpoint_url,
            bucket: self.oss_bucket,
            access_key_id: self.oss_access_key_id,
            access_key_secret: self.oss_access_key_secret,
            root: self.oss_root,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct RedisStorageConfig {
    pub endpoint_url: String,
    pub username: String,
    pub password: String,
    pub root: String,
    pub db: i64,
    /// TTL in seconds
    pub default_ttl: i64,
}

impl Default for RedisStorageConfig {
    fn default() -> Self {
        InnerStorageRedisConfig::default().into()
    }
}

impl From<InnerStorageRedisConfig> for RedisStorageConfig {
    fn from(v: InnerStorageRedisConfig) -> Self {
        Self {
            endpoint_url: v.endpoint_url.clone(),
            username: v.username.unwrap_or_default(),
            password: v.password.unwrap_or_default(),
            root: v.root.clone(),
            db: v.db,
            default_ttl: v.default_ttl.unwrap_or_default(),
        }
    }
}

impl TryInto<InnerStorageRedisConfig> for RedisStorageConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerStorageRedisConfig> {
        Ok(InnerStorageRedisConfig {
            endpoint_url: self.endpoint_url.clone(),
            username: if self.username.is_empty() {
                None
            } else {
                Some(self.username.clone())
            },
            password: if self.password.is_empty() {
                None
            } else {
                Some(self.password.clone())
            },
            root: self.root.clone(),
            db: self.db,
            default_ttl: if self.default_ttl == 0 {
                None
            } else {
                Some(self.default_ttl)
            },
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct WebhdfsStorageConfig {
    /// delegation token for webhdfs storage
    #[clap(long = "storage-webhdfs-delegation", default_value_t)]
    #[serde(rename = "delegation")]
    pub webhdfs_delegation: String,
    /// endpoint url for webhdfs storage
    #[clap(long = "storage-webhdfs-endpoint", default_value_t)]
    #[serde(rename = "endpoint_url")]
    pub webhdfs_endpoint_url: String,
    /// working directory root for webhdfs storage
    #[clap(long = "storage-webhdfs-root", default_value_t)]
    #[serde(rename = "root")]
    pub webhdfs_root: String,
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
            .field("webhdfs_root", &self.webhdfs_root)
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
        }
    }
}

impl TryFrom<WebhdfsStorageConfig> for InnerStorageWebhdfsConfig {
    type Error = ErrorCode;

    fn try_from(value: WebhdfsStorageConfig) -> Result<Self, Self::Error> {
        Ok(InnerStorageWebhdfsConfig {
            delegation: value.webhdfs_delegation,
            endpoint_url: value.webhdfs_endpoint_url,
            root: value.webhdfs_root,
        })
    }
}

/// Query config group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default, deny_unknown_fields)]
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

    #[clap(long, default_value = "120")]
    pub mysql_handler_tcp_keepalive_timeout_secs: u64,

    #[clap(long, default_value = "256")]
    pub max_active_sessions: u64,

    /// The max total memory in bytes that can be used by this process.
    #[clap(long, default_value = "0")]
    pub max_server_memory_usage: u64,

    #[clap(long, parse(try_from_str), default_value = "false")]
    pub max_memory_limit_enabled: bool,

    #[deprecated(note = "clickhouse tcp support is deprecated")]
    #[clap(long, default_value = "127.0.0.1")]
    pub clickhouse_handler_host: String,

    #[deprecated(note = "clickhouse tcp support is deprecated")]
    #[clap(long, default_value = "9000")]
    pub clickhouse_handler_port: u16,

    #[clap(long, default_value = "127.0.0.1")]
    pub clickhouse_http_handler_host: String,

    #[clap(long, default_value = "8124")]
    pub clickhouse_http_handler_port: u16,

    #[clap(long, default_value = "127.0.0.1")]
    pub http_handler_host: String,

    #[clap(long, default_value = "8000")]
    pub http_handler_port: u16,

    #[clap(long, default_value = "60")]
    pub http_handler_result_timeout_secs: u64,

    #[clap(long, default_value = "127.0.0.1")]
    pub flight_sql_handler_host: String,

    #[clap(long, default_value = "8900")]
    pub flight_sql_handler_port: u16,

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
    pub flight_sql_tls_server_cert: String,

    #[clap(long, default_value_t)]
    pub flight_sql_tls_server_key: String,

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

    #[clap(long, default_value = "5000")]
    pub wait_timeout_mills: u64,

    #[clap(long, default_value = "10000")]
    pub max_query_log_size: usize,
    /// Parquet file with smaller size will be read as a whole file, instead of column by column.
    /// For example:
    /// parquet_fast_read_bytes = 52428800
    /// will let databend read whole file for parquet file less than 50MB and read column by column
    /// if file size is greater than 50MB
    #[clap(long)]
    pub parquet_fast_read_bytes: Option<u64>,

    #[clap(long)]
    pub max_storage_io_requests: Option<u64>,

    /// If in management mode, only can do some meta level operations(database/table/user/stage etc.) with metasrv.
    #[clap(long)]
    pub management_mode: bool,

    /// Deprecated: jwt_key_file is deprecated, use jwt_key_files to add a list of available jwks url
    #[clap(long, default_value_t)]
    pub jwt_key_file: String,

    /// If there are multiple trusted jwt provider put it into additional_jwt_key_files configuration
    #[clap(skip)]
    pub jwt_key_files: Vec<String>,

    #[clap(long, default_value = "auto")]
    pub default_storage_format: String,

    #[clap(long, default_value = "auto")]
    pub default_compression: String,

    #[clap(skip)]
    users: Vec<UserConfig>,

    #[clap(long, default_value = "")]
    pub share_endpoint_address: String,

    #[clap(long, default_value = "")]
    pub share_endpoint_auth_token_file: String,

    #[clap(skip)]
    quota: Option<TenantQuota>,

    #[clap(long)]
    pub internal_enable_sandbox_tenant: bool,

    /// Experiment config options, DO NOT USE IT IN PRODUCTION ENV
    #[clap(long)]
    pub internal_merge_on_read_mutation: bool,

    // ----- the following options/args are all deprecated               ----
    // ----- and turned into Option<T>, to help user migrate the configs ----
    /// OBSOLETED: Table disk cache size (mb).
    #[clap(long)]
    pub table_disk_cache_mb_size: Option<u64>,

    /// OBSOLETED: Table Meta Cached enabled
    #[clap(long)]
    pub table_meta_cache_enabled: Option<bool>,

    /// OBSOLETED: Max number of cached table block meta
    #[clap(long)]
    pub table_cache_block_meta_count: Option<u64>,

    /// OBSOLETED: Table memory cache size (mb)
    #[clap(long)]
    pub table_memory_cache_mb_size: Option<u64>,

    /// OBSOLETED: Table disk cache folder root
    #[clap(long)]
    pub table_disk_cache_root: Option<String>,

    /// OBSOLETED: Max number of cached table snapshot
    #[clap(long)]
    pub table_cache_snapshot_count: Option<u64>,

    /// OBSOLETED: Max number of cached table snapshot statistics
    #[clap(long)]
    pub table_cache_statistic_count: Option<u64>,

    /// OBSOLETED: Max number of cached table segment
    #[clap(long)]
    pub table_cache_segment_count: Option<u64>,

    /// OBSOLETED: Max number of cached bloom index meta objects
    #[clap(long)]
    pub table_cache_bloom_index_meta_count: Option<u64>,

    /// OBSOLETED:
    /// Max number of cached bloom index filters, default value is 1024 * 1024 items.
    /// One bloom index filter per column of data block being indexed will be generated if necessary.
    ///
    /// For example, a table of 1024 columns, with 800 data blocks, a query that triggers a full
    /// table filter on 2 columns, might populate 2 * 800 bloom index filter cache items (at most)
    #[clap(long)]
    pub table_cache_bloom_index_filter_count: Option<u64>,

    /// OBSOLETED: (cache of raw bloom filter data is no longer supported)
    /// Max bytes of cached bloom filter bytes.
    #[clap(long)]
    pub(crate) table_cache_bloom_index_data_bytes: Option<u64>,

    /// Disable some system load(For example system.configs) for cloud security.
    #[clap(long)]
    pub disable_system_table_load: bool,

    #[clap(long, default_value = "https://api.openai.com/v1/")]
    pub openai_api_base_url: String,

    // This will not show in system.configs, put it to mask.rs.
    #[clap(long, default_value = "")]
    pub openai_api_key: String,

    /// https://platform.openai.com/docs/models/embeddings
    #[clap(long, default_value = "text-embedding-ada-002")]
    pub openai_api_embedding_model: String,

    /// https://platform.openai.com/docs/guides/chat
    #[clap(long, default_value = "gpt-3.5-turbo")]
    pub openai_api_completion_model: String,
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
            mysql_handler_tcp_keepalive_timeout_secs: self.mysql_handler_tcp_keepalive_timeout_secs,
            max_active_sessions: self.max_active_sessions,
            max_server_memory_usage: self.max_server_memory_usage,
            max_memory_limit_enabled: self.max_memory_limit_enabled,
            clickhouse_http_handler_host: self.clickhouse_http_handler_host,
            clickhouse_http_handler_port: self.clickhouse_http_handler_port,
            http_handler_host: self.http_handler_host,
            http_handler_port: self.http_handler_port,
            http_handler_result_timeout_secs: self.http_handler_result_timeout_secs,
            flight_api_address: self.flight_api_address,
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
            table_engine_memory_enabled: self.table_engine_memory_enabled,
            wait_timeout_mills: self.wait_timeout_mills,
            max_query_log_size: self.max_query_log_size,
            management_mode: self.management_mode,
            parquet_fast_read_bytes: self.parquet_fast_read_bytes,
            max_storage_io_requests: self.max_storage_io_requests,
            jwt_key_file: self.jwt_key_file,
            jwt_key_files: self.jwt_key_files,
            default_storage_format: self.default_storage_format,
            default_compression: self.default_compression,
            idm: InnerIDMConfig {
                users: users_to_inner(self.users)?,
            },
            share_endpoint_address: self.share_endpoint_address,
            share_endpoint_auth_token_file: self.share_endpoint_auth_token_file,
            tenant_quota: self.quota,
            internal_enable_sandbox_tenant: self.internal_enable_sandbox_tenant,
            internal_merge_on_read_mutation: self.internal_merge_on_read_mutation,
            disable_system_table_load: self.disable_system_table_load,
            openai_api_base_url: self.openai_api_base_url,
            openai_api_key: self.openai_api_key,
            openai_api_completion_model: self.openai_api_completion_model,
            openai_api_embedding_model: self.openai_api_embedding_model,
        })
    }
}

#[allow(deprecated)]
impl From<InnerQueryConfig> for QueryConfig {
    fn from(inner: InnerQueryConfig) -> Self {
        Self {
            tenant_id: inner.tenant_id,
            cluster_id: inner.cluster_id,
            num_cpus: inner.num_cpus,
            mysql_handler_host: inner.mysql_handler_host,
            mysql_handler_port: inner.mysql_handler_port,
            mysql_handler_tcp_keepalive_timeout_secs: inner
                .mysql_handler_tcp_keepalive_timeout_secs,
            max_active_sessions: inner.max_active_sessions,
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
            flight_api_address: inner.flight_api_address,
            flight_sql_handler_host: inner.flight_sql_handler_host,
            flight_sql_handler_port: inner.flight_sql_handler_port,
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
            table_engine_memory_enabled: inner.table_engine_memory_enabled,
            wait_timeout_mills: inner.wait_timeout_mills,
            max_query_log_size: inner.max_query_log_size,
            management_mode: inner.management_mode,
            parquet_fast_read_bytes: inner.parquet_fast_read_bytes,
            max_storage_io_requests: inner.max_storage_io_requests,
            jwt_key_file: inner.jwt_key_file,
            jwt_key_files: inner.jwt_key_files,
            default_storage_format: inner.default_storage_format,
            default_compression: inner.default_compression,
            users: users_from_inner(inner.idm.users),
            share_endpoint_address: inner.share_endpoint_address,
            share_endpoint_auth_token_file: inner.share_endpoint_auth_token_file,
            quota: inner.tenant_quota,
            internal_enable_sandbox_tenant: inner.internal_enable_sandbox_tenant,
            internal_merge_on_read_mutation: false,
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
            disable_system_table_load: inner.disable_system_table_load,
            openai_api_base_url: inner.openai_api_base_url,
            openai_api_key: inner.openai_api_key,
            openai_api_completion_model: inner.openai_api_completion_model,
            openai_api_embedding_model: inner.openai_api_embedding_model,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
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

    #[clap(flatten)]
    pub file: FileLogConfig,

    #[clap(flatten)]
    pub stderr: StderrLogConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        InnerLogConfig::default().into()
    }
}

impl TryInto<InnerLogConfig> for LogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerLogConfig> {
        let mut file: InnerFileLogConfig = self.file.try_into()?;
        if self.level != "INFO" {
            file.level = self.level.to_string();
        }
        if self.dir != "./.databend/logs" {
            file.dir = self.dir.to_string();
        }

        Ok(InnerLogConfig {
            file,
            stderr: self.stderr.try_into()?,
        })
    }
}

impl From<InnerLogConfig> for LogConfig {
    fn from(inner: InnerLogConfig) -> Self {
        Self {
            level: inner.file.level.clone(),
            dir: inner.file.dir.clone(),
            query_enabled: false,
            file: inner.file.into(),
            stderr: inner.stderr.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FileLogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-file-on", default_value = "true")]
    #[serde(rename = "on")]
    pub file_on: bool,

    #[clap(long = "log-file-level", default_value = "INFO")]
    #[serde(rename = "level")]
    pub file_level: String,

    /// Log file dir
    #[clap(long = "log-file-dir", default_value = "./.databend/logs")]
    #[serde(rename = "dir")]
    pub file_dir: String,

    /// Log file format
    #[clap(long = "log-file-format", default_value = "json")]
    #[serde(rename = "format")]
    pub file_format: String,
}

impl Default for FileLogConfig {
    fn default() -> Self {
        InnerFileLogConfig::default().into()
    }
}

impl TryInto<InnerFileLogConfig> for FileLogConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerFileLogConfig> {
        Ok(InnerFileLogConfig {
            on: self.file_on,
            level: self.file_level,
            dir: self.file_dir,
            format: self.file_format,
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
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StderrLogConfig {
    /// Log level <DEBUG|INFO|ERROR>
    #[clap(long = "log-stderr-on")]
    #[serde(rename = "on")]
    pub stderr_on: bool,

    #[clap(long = "log-stderr-level", default_value = "INFO")]
    #[serde(rename = "level")]
    pub stderr_level: String,

    #[clap(long = "log-stderr-format", default_value = "text")]
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

/// Meta config group.
/// deny_unknown_fields to check unknown field, like the deprecated `address`.
/// TODO(xuanwo): All meta_xxx should be rename to xxx.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default, deny_unknown_fields)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    #[clap(long = "meta-embedded-dir", default_value_t)]
    #[serde(alias = "meta_embedded_dir")]
    pub embedded_dir: String,

    /// MetaStore backend endpoints
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

    /// AutoSyncInterval is the interval to update endpoints with its latest members.
    /// 0 disables auto-sync. By default auto-sync is disabled.
    #[clap(long = "auto-sync-interval", default_value = "0")]
    #[serde(alias = "auto_sync_interval")]
    pub auto_sync_interval: u64,

    #[clap(long = "unhealth-endpoint-evict-time", default_value = "120")]
    #[serde(alias = "unhealth_endpoint_evict_time")]
    pub unhealth_endpoint_evict_time: u64,

    /// Certificate for client to identify meta rpc serve
    #[clap(long = "meta-rpc-tls-meta-server-root-ca-cert", default_value_t)]
    pub rpc_tls_meta_server_root_ca_cert: String,

    #[clap(
        long = "meta-rpc-tls-meta-service-domain-name",
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

fn users_from_inner(inner: HashMap<String, AuthInfo>) -> Vec<UserConfig> {
    inner
        .into_iter()
        .map(|(name, auth)| UserConfig {
            name,
            auth: auth.into(),
        })
        .collect()
}

fn users_to_inner(outer: Vec<UserConfig>) -> Result<HashMap<String, AuthInfo>> {
    let mut inner = HashMap::new();
    for c in outer.into_iter() {
        inner.insert(c.name.clone(), c.auth.try_into()?);
    }
    Ok(inner)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserConfig {
    pub name: String,
    #[serde(flatten)]
    pub auth: UserAuthConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserAuthConfig {
    auth_type: String,
    auth_string: Option<String>,
}

fn check_no_auth_string(auth_string: Option<String>, auth_info: AuthInfo) -> Result<AuthInfo> {
    match auth_string {
        Some(s) if !s.is_empty() => Err(ErrorCode::InvalidConfig(format!(
            "should not set auth_string for auth_type {}",
            auth_info.get_type().to_str()
        ))),
        _ => Ok(auth_info),
    }
}

impl TryInto<AuthInfo> for UserAuthConfig {
    type Error = ErrorCode;

    fn try_into(self) -> Result<AuthInfo> {
        let auth_type = AuthType::from_str(&self.auth_type)?;
        match auth_type {
            AuthType::NoPassword => check_no_auth_string(self.auth_string, AuthInfo::None),
            AuthType::JWT => check_no_auth_string(self.auth_string, AuthInfo::JWT),
            AuthType::Sha256Password | AuthType::DoubleSha1Password => {
                let password_type = auth_type.get_password_type().expect("must success");
                match self.auth_string {
                    None => Err(ErrorCode::InvalidConfig("must set auth_string")),
                    Some(s) => {
                        let p = hex::decode(s).map_err(|e| {
                            ErrorCode::InvalidConfig(format!("password is not hex: {e:?}"))
                        })?;
                        Ok(AuthInfo::Password {
                            hash_value: p,
                            hash_method: password_type,
                        })
                    }
                }
            }
        }
    }
}

impl From<AuthInfo> for UserAuthConfig {
    fn from(inner: AuthInfo) -> Self {
        let auth_type = inner.get_type().to_str().to_owned();
        let auth_string = inner.get_auth_string();
        let auth_string = if auth_string.is_empty() {
            None
        } else {
            Some(hex::encode(auth_string))
        };
        UserAuthConfig {
            auth_type,
            auth_string,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct LocalConfig {
    // sql to run
    #[clap(long, default_value = "SELECT 1")]
    pub sql: String,

    // name1=filepath1,name2=filepath2
    #[clap(long, default_value = "")]
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
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Enable table meta cache. Default is enabled. Set it to false to disable all the table meta caches
    #[clap(long = "cache-enable-table-meta-cache", default_value = "true")]
    #[serde(default = "bool_true")]
    pub enable_table_meta_cache: bool,

    /// Max number of cached table snapshot
    #[clap(long = "cache-table-meta-snapshot-count", default_value = "256")]
    pub table_meta_snapshot_count: u64,

    /// Max bytes of cached table segment
    #[clap(long = "cache-table-meta-segment-bytes", default_value = "1073741824")]
    pub table_meta_segment_bytes: u64,

    /// Max number of cached table statistic meta
    #[clap(long = "cache-table-meta-statistic-count", default_value = "256")]
    pub table_meta_statistic_count: u64,

    /// Enable bloom index cache. Default is enabled. Set it to false to disable all the bloom index caches
    #[clap(long = "cache-enable-table-bloom-index-cache", default_value = "true")]
    #[serde(default = "bool_true")]
    pub enable_table_bloom_index_cache: bool,

    /// Max number of cached bloom index meta objects. Set it to 0 to disable it.
    #[clap(long = "cache-table-bloom-index-meta-count", default_value = "3000")]
    pub table_bloom_index_meta_count: u64,

    /// Max number of cached bloom index filters. Set it to 0 to disable it.
    // One bloom index filter per column of data block being indexed will be generated if necessary.
    //
    // For example, a table of 1024 columns, with 800 data blocks, a query that triggers a full
    // table filter on 2 columns, might populate 2 * 800 bloom index filter cache items (at most)
    #[clap(
        long = "cache-table-bloom-index-filter-count",
        default_value = "1048576"
    )]
    pub table_bloom_index_filter_count: u64,

    #[clap(long = "cache-table-prune-partitions-count", default_value = "256")]
    pub table_prune_partitions_count: u64,

    /// Type of data cache storage
    #[clap(long = "cache-data-cache-storage", value_enum, default_value_t)]
    pub data_cache_storage: CacheStorageTypeConfig,

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
    #[clap(
        long = "cache-data-cache-population-queue-size",
        default_value = "65536"
    )]
    pub table_data_cache_population_queue_size: u32,

    /// Storage that hold the data caches
    #[clap(flatten)]
    #[serde(rename = "disk")]
    pub disk_cache_config: DiskCacheConfig,

    /// Max size of in memory table column object cache. By default it is 0 (disabled)
    ///
    /// CAUTION: The cached items are deserialized table column objects, may take a lot of memory.
    ///
    /// Only if query nodes have plenty of un-utilized memory, the working set can be fitted into,
    /// and the access pattern will benefit from caching, consider enabled this cache.
    #[clap(long = "cache-table-data-deserialized-data-bytes", default_value = "0")]
    pub table_data_deserialized_data_bytes: u64,

    // ----- the following options/args are all deprecated               ----
    /// Max number of cached table segment
    #[clap(long = "cache-table-meta-segment-count")]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Args, Default)]
#[serde(default, deny_unknown_fields)]
pub struct DiskCacheConfig {
    /// Max bytes of cached raw table data. Default 20GB, set it to 0 to disable it.
    #[clap(long = "cache-disk-max-bytes", default_value = "21474836480")]
    pub max_bytes: u64,

    /// Table disk cache root path
    #[clap(long = "cache-disk-path", default_value = "./.databend/_cache")]
    pub path: String,
}

mod cache_config_converters {
    use super::*;

    impl From<InnerConfig> for Config {
        fn from(inner: InnerConfig) -> Self {
            Self {
                cmd: inner.cmd,
                config_file: inner.config_file,
                query: inner.query.into(),
                log: inner.log.into(),
                meta: inner.meta.into(),
                storage: inner.storage.into(),
                catalog: HiveCatalogConfig::default(),
                local: inner.local.into(),

                catalogs: inner
                    .catalogs
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                cache: inner.cache.into(),
            }
        }
    }

    impl TryInto<InnerConfig> for Config {
        type Error = ErrorCode;

        fn try_into(self) -> Result<InnerConfig> {
            let mut catalogs = HashMap::new();
            for (k, v) in self.catalogs.into_iter() {
                let catalog = v.try_into()?;
                catalogs.insert(k, catalog);
            }
            if !self.catalog.meta_store_address.is_empty() || !self.catalog.protocol.is_empty() {
                tracing::warn!(
                    "`catalog` is planned to be deprecated, please add catalog in `catalogs` instead"
                );
                let hive = self.catalog.try_into()?;
                let catalog = InnerCatalogConfig::Hive(hive);
                catalogs.insert(CATALOG_HIVE.to_string(), catalog);
            }

            Ok(InnerConfig {
                cmd: self.cmd,
                config_file: self.config_file,
                query: self.query.try_into()?,
                log: self.log.try_into()?,
                meta: self.meta.try_into()?,
                storage: self.storage.try_into()?,
                local: self.local.try_into()?,
                catalogs,
                cache: self.cache.try_into()?,
            })
        }
    }

    impl TryFrom<CacheConfig> for inner::CacheConfig {
        type Error = ErrorCode;

        fn try_from(value: CacheConfig) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                enable_table_meta_cache: value.enable_table_meta_cache,
                table_meta_snapshot_count: value.table_meta_snapshot_count,
                table_meta_segment_bytes: value.table_meta_segment_bytes,
                table_meta_statistic_count: value.table_meta_statistic_count,
                enable_table_index_bloom: value.enable_table_bloom_index_cache,
                table_bloom_index_meta_count: value.table_bloom_index_meta_count,
                table_bloom_index_filter_count: value.table_bloom_index_filter_count,
                table_prune_partitions_count: value.table_prune_partitions_count,
                data_cache_storage: value.data_cache_storage.try_into()?,
                table_data_cache_population_queue_size: value
                    .table_data_cache_population_queue_size,
                disk_cache_config: value.disk_cache_config.try_into()?,
                table_data_deserialized_data_bytes: value.table_data_deserialized_data_bytes,
            })
        }
    }

    impl From<inner::CacheConfig> for CacheConfig {
        fn from(value: inner::CacheConfig) -> Self {
            Self {
                enable_table_meta_cache: value.enable_table_meta_cache,
                table_meta_snapshot_count: value.table_meta_snapshot_count,
                table_meta_segment_bytes: value.table_meta_segment_bytes,
                table_meta_statistic_count: value.table_meta_statistic_count,
                enable_table_bloom_index_cache: value.enable_table_index_bloom,
                table_bloom_index_meta_count: value.table_bloom_index_meta_count,
                table_bloom_index_filter_count: value.table_bloom_index_filter_count,
                table_prune_partitions_count: value.table_prune_partitions_count,
                data_cache_storage: value.data_cache_storage.into(),
                table_data_cache_population_queue_size: value
                    .table_data_cache_population_queue_size,
                disk_cache_config: value.disk_cache_config.into(),
                table_data_deserialized_data_bytes: value.table_data_deserialized_data_bytes,
                table_meta_segment_count: None,
            }
        }
    }

    impl TryFrom<DiskCacheConfig> for inner::DiskCacheConfig {
        type Error = ErrorCode;
        fn try_from(value: DiskCacheConfig) -> std::result::Result<Self, Self::Error> {
            Ok(Self {
                max_bytes: value.max_bytes,
                path: value.path,
            })
        }
    }

    impl From<inner::DiskCacheConfig> for DiskCacheConfig {
        fn from(value: inner::DiskCacheConfig) -> Self {
            Self {
                max_bytes: value.max_bytes,
                path: value.path,
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
}
