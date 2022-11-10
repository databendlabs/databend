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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use common_auth::RefreshableToken;
use common_auth::TokenFile;
use common_base::base::tokio::sync::RwLock;
use common_base::base::Singleton;
use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use serde::Serialize;

use super::utils::mask_string;

/// Config for storage backend.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    pub num_cpus: u64,
    pub allow_insecure: bool,

    pub params: StorageParams,
}

/// Config for cache backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheConfig {
    pub num_cpus: u64,

    pub params: StorageParams,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            num_cpus: 0,
            params: StorageParams::Moka(StorageMokaConfig::default()),
        }
    }
}

/// Storage params which contains the detailed storage info.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageParams {
    Azblob(StorageAzblobConfig),
    Fs(StorageFsConfig),
    Ftp(StorageFtpConfig),
    Gcs(StorageGcsConfig),
    #[cfg(feature = "storage-hdfs")]
    Hdfs(StorageHdfsConfig),
    Http(StorageHttpConfig),
    Ipfs(StorageIpfsConfig),
    Memory,
    Moka(StorageMokaConfig),
    Obs(StorageObsConfig),
    Oss(StorageOssConfig),
    S3(StorageS3Config),
}

impl Default for StorageParams {
    fn default() -> Self {
        StorageParams::Fs(StorageFsConfig::default())
    }
}

/// StorageParams will be displayed by `{protocol}://{key1=value1},{key2=value2}`
impl Display for StorageParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageParams::Azblob(v) => write!(
                f,
                "azblob | container={},root={},endpoint={}",
                v.container, v.root, v.endpoint_url
            ),
            StorageParams::Fs(v) => write!(f, "fs | root={}", v.root),
            StorageParams::Ftp(v) => {
                write!(f, "ftp | root={},endpoint={}", v.root, v.endpoint)
            }
            StorageParams::Gcs(v) => write!(
                f,
                "gcs | bucket={},root={},endpoint={}",
                v.bucket, v.root, v.endpoint_url
            ),
            #[cfg(feature = "storage-hdfs")]
            StorageParams::Hdfs(v) => {
                write!(f, "hdfs | root={},name_node={}", v.root, v.name_node)
            }
            StorageParams::Http(v) => {
                write!(f, "http | endpoint={},paths={:?}", v.endpoint_url, v.paths)
            }
            StorageParams::Ipfs(c) => {
                write!(f, "ipfs | endpoint={},root={}", c.endpoint_url, c.root)
            }
            StorageParams::Memory => write!(f, "memory"),
            StorageParams::Moka(_) => write!(f, "moka"),
            StorageParams::Obs(v) => write!(
                f,
                "obs | bucket={},root={},endpoint={}",
                v.bucket, v.root, v.endpoint_url
            ),
            StorageParams::Oss(v) => write!(
                f,
                "oss | bucket={},root={},endpoint={}",
                v.bucket, v.root, v.endpoint_url
            ),
            StorageParams::S3(v) => {
                write!(
                    f,
                    "s3 | bucket={},root={},endpoint={}",
                    v.bucket, v.root, v.endpoint_url
                )
            }
        }
    }
}

impl StorageParams {
    /// Whether this storage params is secure.
    ///
    /// Query will forbid this storage config unless `allow_insecure` has been enabled.
    pub fn is_secure(&self) -> bool {
        match self {
            StorageParams::Azblob(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Fs(_) => false,
            StorageParams::Ftp(v) => v.endpoint.starts_with("ftps://"),
            #[cfg(feature = "storage-hdfs")]
            StorageParams::Hdfs(_) => false,
            StorageParams::Http(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Ipfs(c) => c.endpoint_url.starts_with("https://"),
            StorageParams::Memory => false,
            StorageParams::Moka(_) => false,
            StorageParams::Obs(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Oss(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::S3(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Gcs(v) => v.endpoint_url.starts_with("https://"),
        }
    }
}

/// Config for storage backend azblob.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAzblobConfig {
    pub endpoint_url: String,
    pub container: String,
    pub account_name: String,
    pub account_key: String,
    pub root: String,
}

impl Debug for StorageAzblobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageAzblobConfig")
            .field("endpoint_url", &self.endpoint_url)
            .field("container", &self.container)
            .field("root", &self.root)
            .field("account_name", &self.account_name)
            .field("account_key", &mask_string(&self.account_key, 3))
            .finish()
    }
}

/// Config for storage backend fs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFsConfig {
    pub root: String,
}

impl Default for StorageFsConfig {
    fn default() -> Self {
        Self {
            root: "_data".to_string(),
        }
    }
}

pub const STORAGE_FTP_DEFAULT_ENDPOINT: &str = "ftps://127.0.0.1";
/// Config for FTP and FTPS data source
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFtpConfig {
    pub endpoint: String,
    pub root: String,
    pub username: String,
    pub password: String,
}

impl Default for StorageFtpConfig {
    fn default() -> Self {
        Self {
            endpoint: STORAGE_FTP_DEFAULT_ENDPOINT.to_string(),
            username: "".to_string(),
            password: "".to_string(),
            root: "/".to_string(),
        }
    }
}

impl Debug for StorageFtpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageFtpConfig")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("username", &self.username)
            .field("password", &mask_string(self.password.as_str(), 3))
            .finish()
    }
}

pub static STORAGE_GCS_DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

/// Config for storage backend GCS.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct StorageGcsConfig {
    pub endpoint_url: String,
    pub bucket: String,
    pub root: String,
    pub credential: String,
}

impl Default for StorageGcsConfig {
    fn default() -> Self {
        Self {
            endpoint_url: STORAGE_GCS_DEFAULT_ENDPOINT.to_string(),
            bucket: String::new(),
            root: String::new(),
            credential: String::new(),
        }
    }
}

impl Debug for StorageGcsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageGcsConfig")
            .field("endpoint", &self.endpoint_url)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("credential", &mask_string(&self.credential, 3))
            .finish()
    }
}

/// Config for storage backend hdfs.
///
/// # Notes
///
/// Ideally, we should export this config only when hdfs feature enabled.
/// But export this struct without hdfs feature is safe and no harm. So we
/// export it to make crates' lives that depend on us easier.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHdfsConfig {
    pub name_node: String,
    pub root: String,
}

pub static STORAGE_S3_DEFAULT_ENDPOINT: &str = "https://s3.amazonaws.com";

/// Config for storage backend s3.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageS3Config {
    pub endpoint_url: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Temporary security token used for authentications
    ///
    /// This recommended to use since users don't need to store their permanent credentials in their
    /// scripts or worksheets.
    ///
    /// refer to [documentations](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) for details.
    pub security_token: String,
    pub master_key: String,
    pub root: String,
    /// This flag is used internally to control whether databend load
    /// credentials from environment like env, profile and web token.
    pub disable_credential_loader: bool,
    /// Enable this flag to send API in virtual host style.
    ///
    /// - Virtual Host Style: `https://bucket.s3.amazonaws.com`
    /// - Path Style: `https://s3.amazonaws.com/bucket`
    pub enable_virtual_host_style: bool,
    /// The RoleArn that used for AssumeRole.
    pub role_arn: String,
    /// The ExternalId that used for AssumeRole.
    pub external_id: String,
}

impl Default for StorageS3Config {
    fn default() -> Self {
        StorageS3Config {
            endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
            region: "".to_string(),
            bucket: "".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            security_token: "".to_string(),
            master_key: "".to_string(),
            root: "".to_string(),
            disable_credential_loader: false,
            enable_virtual_host_style: false,
            role_arn: "".to_string(),
            external_id: "".to_string(),
        }
    }
}

impl Debug for StorageS3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageS3Config")
            .field("endpoint_url", &self.endpoint_url)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("disable_credential_loader", &self.disable_credential_loader)
            .field("enable_virtual_host_style", &self.enable_virtual_host_style)
            .field("role_arn", &self.role_arn)
            .field("external_id", &self.external_id)
            .field("access_key_id", &mask_string(&self.access_key_id, 3))
            .field(
                "secret_access_key",
                &mask_string(&self.secret_access_key, 3),
            )
            .field("security_token", &mask_string(&self.security_token, 3))
            .field("master_key", &mask_string(&self.master_key, 3))
            .finish()
    }
}

/// Config for storage backend http.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHttpConfig {
    pub endpoint_url: String,
    pub paths: Vec<String>,
}

pub const STORAGE_IPFS_DEFAULT_ENDPOINT: &str = "https://ipfs.io";
/// Config for IPFS storage backend
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIpfsConfig {
    pub endpoint_url: String,
    pub root: String,
}

/// Config for storage backend obs.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageObsConfig {
    pub endpoint_url: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub root: String,
}

impl Debug for StorageObsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageObsConfig")
            .field("endpoint_url", &self.endpoint_url)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("access_key_id", &mask_string(&self.access_key_id, 3))
            .field(
                "secret_access_key",
                &mask_string(&self.secret_access_key, 3),
            )
            .finish()
    }
}

/// config for Aliyun Object Storage Service
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageOssConfig {
    pub endpoint_url: String,
    pub bucket: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub root: String,
}

impl Debug for StorageOssConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageOssConfig")
            .field("endpoint_url", &self.endpoint_url)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("access_key_id", &mask_string(&self.access_key_id, 3))
            .field(
                "access_key_secret",
                &mask_string(&self.access_key_secret, 3),
            )
            .finish()
    }
}

/// config for Moka Object Storage Service
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMokaConfig {}

static SHARE_TABLE_CONFIG: OnceCell<Singleton<ShareTableConfig>> = OnceCell::new();

// TODO: This config should be moved out of common-storage crate.
#[derive(Clone)]
pub struct ShareTableConfig {
    pub share_endpoint_address: Option<String>,
    pub share_endpoint_token: RefreshableToken,
}

impl ShareTableConfig {
    pub fn init(
        share_endpoint_address: &str,
        token_file: &str,
        default_token: String,
        v: Singleton<ShareTableConfig>,
    ) -> common_exception::Result<()> {
        v.init(Self::try_create(
            share_endpoint_address,
            token_file,
            default_token,
        )?)?;

        SHARE_TABLE_CONFIG.set(v).ok();
        Ok(())
    }

    pub fn try_create(
        share_endpoint_address: &str,
        token_file: &str,
        default_token: String,
    ) -> common_exception::Result<ShareTableConfig> {
        let share_endpoint_address = if share_endpoint_address.is_empty() {
            None
        } else {
            Some(share_endpoint_address.to_owned())
        };
        let share_endpoint_token = if token_file.is_empty() {
            RefreshableToken::Direct(default_token)
        } else {
            let s = String::from(token_file);
            let f = TokenFile::new(Path::new(&s))?;
            RefreshableToken::File(Arc::new(RwLock::new(f)))
        };
        Ok(ShareTableConfig {
            share_endpoint_address,
            share_endpoint_token,
        })
    }

    pub fn share_endpoint_address() -> Option<String> {
        ShareTableConfig::instance().share_endpoint_address
    }

    pub fn share_endpoint_token() -> RefreshableToken {
        ShareTableConfig::instance().share_endpoint_token
    }

    pub fn instance() -> ShareTableConfig {
        match SHARE_TABLE_CONFIG.get() {
            None => panic!("ShareTableConfig is not init"),
            Some(config) => config.get(),
        }
    }
}

#[derive(Clone, Default, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct CatalogConfig {
    pub catalogs: HashMap<String, CatalogDescription>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CatalogDescription {
    Hive(HiveCatalogConfig),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "protocol")]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
