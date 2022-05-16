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

use clap::Args;
use clap::Parser;
use common_base::base::mask_string;
use common_configs::HiveCatalogConfig;
use common_configs::LogConfig;
use common_configs::MetaConfig;
use common_configs::QueryConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::StorageAzblobConfig as InnerStorageAzblobConfig;
use common_io::prelude::StorageConfig as InnerStorageConfig;
use common_io::prelude::StorageFsConfig as InnerStorageFsConfig;
use common_io::prelude::StorageHdfsConfig as InnerStorageHdfsConfig;
use common_io::prelude::StorageParams;
use common_io::prelude::StorageS3Config as InnerStorageS3Config;
use serde::Deserialize;
use serde::Serialize;
use serfig::collectors::from_env;
use serfig::collectors::from_file;
use serfig::collectors::from_self;
use serfig::parsers::Toml;

use super::inner::Config as InnerConfig;

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
            query: inner.query,
            log: inner.log,
            meta: inner.meta,
            storage: StorageConfig::from(inner.storage),
            catalog: inner.catalog,
        }
    }
}

impl TryInto<InnerConfig> for Config {
    type Error = ErrorCode;

    fn try_into(self) -> Result<InnerConfig> {
        Ok(InnerConfig {
            config_file: self.config_file,
            query: self.query,
            log: self.log,
            meta: self.meta,
            storage: self.storage.try_into()?,
            catalog: self.catalog,
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
