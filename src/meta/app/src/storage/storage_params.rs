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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

/// Storage params which contains the detailed storage info.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageParams {
    Azblob(StorageAzblobConfig),
    Fs(StorageFsConfig),
    Ftp(StorageFtpConfig),
    Gcs(StorageGcsConfig),
    Hdfs(StorageHdfsConfig),
    Http(StorageHttpConfig),
    Ipfs(StorageIpfsConfig),
    Memory,
    Moka(StorageMokaConfig),
    Obs(StorageObsConfig),
    Oss(StorageOssConfig),
    S3(StorageS3Config),
    Webhdfs(StorageWebhdfsConfig),
    Cos(StorageCosConfig),
    Huggingface(StorageHuggingfaceConfig),

    /// None means this storage type is none.
    ///
    /// This type is mostly for cache which mean bypass the cache logic.
    None,
}

impl Default for StorageParams {
    fn default() -> Self {
        StorageParams::Fs(StorageFsConfig::default())
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
            StorageParams::Hdfs(_) => false,
            StorageParams::Http(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Ipfs(c) => c.endpoint_url.starts_with("https://"),
            StorageParams::Memory => false,
            StorageParams::Moka(_) => false,
            StorageParams::Obs(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Oss(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::S3(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Gcs(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Webhdfs(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Cos(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Huggingface(_) => true,
            StorageParams::None => false,
        }
    }

    /// map the given root with.
    pub fn map_root(mut self, f: impl Fn(&str) -> String) -> Self {
        match &mut self {
            StorageParams::Azblob(v) => v.root = f(&v.root),
            StorageParams::Fs(v) => v.root = f(&v.root),
            StorageParams::Ftp(v) => v.root = f(&v.root),
            StorageParams::Hdfs(v) => v.root = f(&v.root),
            StorageParams::Http(_) => {}
            StorageParams::Ipfs(v) => v.root = f(&v.root),
            StorageParams::Memory => {}
            StorageParams::Moka(_) => {}
            StorageParams::Obs(v) => v.root = f(&v.root),
            StorageParams::Oss(v) => v.root = f(&v.root),
            StorageParams::S3(v) => v.root = f(&v.root),
            StorageParams::Gcs(v) => v.root = f(&v.root),
            StorageParams::Webhdfs(v) => v.root = f(&v.root),
            StorageParams::Cos(v) => v.root = f(&v.root),
            StorageParams::Huggingface(v) => v.root = f(&v.root),
            StorageParams::None => {}
        };

        self
    }

    pub fn is_fs(&self) -> bool {
        matches!(self, StorageParams::Fs(_))
    }

    /// Whether this storage params need encryption feature to start.
    pub fn need_encryption_feature(&self) -> bool {
        match &self {
            StorageParams::Oss(v) => {
                !v.server_side_encryption.is_empty() || !v.server_side_encryption_key_id.is_empty()
            }
            _ => false,
        }
    }

    /// auto_detect is used to do auto detect for some storage params under async context.
    ///
    /// - This action should be taken before storage params been passed out.
    pub async fn auto_detect(self) -> Result<Self> {
        let sp = match self {
            StorageParams::S3(mut s3) if s3.region.is_empty() => {
                // TODO: endpoint related logic should be moved out from opendal as a new API.
                // Remove the possible trailing `/` in endpoint.
                let endpoint = s3.endpoint_url.trim_end_matches('/');

                // Make sure the endpoint contains the scheme.
                let endpoint = if endpoint.starts_with("http") {
                    endpoint.to_string()
                } else {
                    // Prefix https if endpoint doesn't start with scheme.
                    format!("https://{}", endpoint)
                };

                // We should not return error if client create failed, just ignore it.
                if let Ok(client) = opendal::raw::HttpClient::new() {
                    // The response itself doesn't important.
                    let _ = client
                        .client()
                        .get(&endpoint)
                        .timeout(Duration::from_secs(10))
                        .send()
                        .await
                        .map_err(|err| {
                            ErrorCode::InvalidConfig(format!(
                                "s3 endpoint_url {} is invalid or incomplete: {err:?}",
                                s3.endpoint_url
                            ))
                        })?;
                }
                s3.region = opendal::services::S3::detect_region(&s3.endpoint_url, &s3.bucket)
                    .await
                    .unwrap_or_default();
                StorageParams::S3(s3)
            }
            v => v,
        };

        Ok(sp)
    }
}

/// StorageParams will be displayed by `{protocol}://{key1=value1},{key2=value2}`
impl Display for StorageParams {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
            StorageParams::Moka(v) => write!(f, "moka | max_capacity={}", v.max_capacity),
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
            StorageParams::Cos(v) => write!(
                f,
                "cos | bucket={},root={},endpoint={}",
                v.bucket, v.root, v.endpoint_url
            ),
            StorageParams::S3(v) => {
                write!(
                    f,
                    "s3 | bucket={},root={},endpoint={}",
                    v.bucket, v.root, v.endpoint_url
                )
            }
            StorageParams::Webhdfs(v) => {
                write!(f, "webhdfs | root={},endpoint={}", v.root, v.endpoint_url)
            }
            StorageParams::Huggingface(v) => {
                write!(
                    f,
                    "huggingface | repo_type={}, repo_id={}, root={}",
                    v.repo_type, v.repo_id, v.root
                )
            }
            StorageParams::None => {
                write!(f, "none",)
            }
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    pub presign_endpoint_url: String,
    pub bucket: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub root: String,
    /// Server-side encryption for OSS
    ///
    /// Available values: "AES256", "KMS"
    pub server_side_encryption: String,
    /// Server-side encryption key id for OSS
    ///
    /// Only effective when `server_side_encryption` is "KMS"
    pub server_side_encryption_key_id: String,
}

impl Debug for StorageOssConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageOssConfig")
            .field("endpoint_url", &self.endpoint_url)
            .field("presign_endpoint_url", &self.presign_endpoint_url)
            .field("bucket", &self.bucket)
            .field("root", &self.root)
            .field("access_key_id", &mask_string(&self.access_key_id, 3))
            .field(
                "access_key_secret",
                &mask_string(&self.access_key_secret, 3),
            )
            .field(
                "server_side_encryption",
                &mask_string(&self.server_side_encryption, 3),
            )
            .field(
                "server_side_encryption_key_id",
                &mask_string(&self.server_side_encryption_key_id, 3),
            )
            .finish()
    }
}

/// config for Moka Object Storage Service
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMokaConfig {
    pub max_capacity: u64,
    pub time_to_live: i64,
    pub time_to_idle: i64,
}

impl Default for StorageMokaConfig {
    #[no_sanitize(address)]
    fn default() -> Self {
        Self {
            // Use 1G as default.
            max_capacity: 1024 * 1024 * 1024,
            // Use 1 hour as default time to live
            time_to_live: 3600,
            // Use 10 minutes as default time to idle.
            time_to_idle: 600,
        }
    }
}

/// config for WebHDFS Storage Service
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWebhdfsConfig {
    pub endpoint_url: String,
    pub root: String,
    pub delegation: String,
}

impl Debug for StorageWebhdfsConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut ds = f.debug_struct("StorageWebhdfsConfig");

        ds.field("endpoint_url", &self.endpoint_url)
            .field("root", &self.root);

        ds.field("delegation", &mask_string(&self.delegation, 3));

        ds.finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCosConfig {
    pub secret_id: String,
    pub secret_key: String,
    pub bucket: String,
    pub endpoint_url: String,
    pub root: String,
}

impl Debug for StorageCosConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut ds = f.debug_struct("StorageCosConfig");

        ds.field("bucket", &self.bucket);
        ds.field("endpoint_url", &self.endpoint_url);
        ds.field("root", &self.root);
        ds.field("secret_id", &mask_string(&self.secret_id, 3));
        ds.field("secret_key", &mask_string(&self.secret_key, 3));

        ds.finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHuggingfaceConfig {
    /// repo_id for huggingface repo, looks like `opendal/huggingface-testdata`
    pub repo_id: String,
    /// repo_type for huggingface repo
    ///
    /// available value: `dataset`, `model`
    /// default value: `dataset`
    pub repo_type: String,
    /// revision for huggingface repo
    ///
    /// available value: branches, tags or commits in the repo.
    /// default value: `main`
    pub revision: String,
    /// token for huggingface
    ///
    /// Only needed for private repo.
    pub token: String,
    pub root: String,
}

impl Debug for StorageHuggingfaceConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut ds = f.debug_struct("StorageHuggingFaceConfig");

        ds.field("repo_id", &self.repo_id);
        ds.field("repo_type", &self.repo_type);
        ds.field("revision", &self.revision);
        ds.field("root", &self.root);
        ds.field("token", &mask_string(&self.token, 3));

        ds.finish()
    }
}

/// Mask a string by "******", but keep `unmask_len` of suffix.
///
/// Copied from `common-base` so that we don't need to depend on it.
#[inline]
pub fn mask_string(s: &str, unmask_len: usize) -> String {
    if s.len() <= unmask_len {
        s.to_string()
    } else {
        let mut ret = "******".to_string();
        ret.push_str(&s[(s.len() - unmask_len)..]);
        ret
    }
}
