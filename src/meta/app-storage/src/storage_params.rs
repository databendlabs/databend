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
use tokio::time::timeout;

const DEFAULT_DETECT_REGION_TIMEOUT_SEC: u64 = 10;

/// Storage params which contains the detailed storage info.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// Get the storage type as a string.
    pub fn storage_type(&self) -> &'static str {
        match self {
            StorageParams::Azblob(_) => "azblob",
            StorageParams::Fs(_) => "fs",
            StorageParams::Ftp(_) => "ftp",
            StorageParams::Gcs(_) => "gcs",
            StorageParams::Hdfs(_) => "hdfs",
            StorageParams::Http(_) => "http",
            StorageParams::Ipfs(_) => "ipfs",
            StorageParams::Memory => "memory",
            StorageParams::Moka(_) => "moka",
            StorageParams::Obs(_) => "obs",
            StorageParams::Oss(_) => "oss",
            StorageParams::S3(_) => "s3",
            StorageParams::Webhdfs(_) => "webhdfs",
            StorageParams::Cos(_) => "cos",
            StorageParams::Huggingface(_) => "huggingface",
            StorageParams::None => "none",
        }
    }

    /// Return a redacted clone suitable for display/logging.
    ///
    /// Sensitive credential fields are masked in the returned config so callers can
    /// serialize it for user-visible contexts without leaking plaintext secrets.
    pub fn redacted_for_display(&self) -> Self {
        let mut clone = self.clone();
        clone.redact_sensitive_fields();
        clone
    }

    fn redact_sensitive_fields(&mut self) {
        fn mask_if_not_empty(value: &mut String) {
            if !value.is_empty() {
                *value = mask_string(value, 3);
            }
        }

        match self {
            StorageParams::Azblob(cfg) => {
                mask_if_not_empty(&mut cfg.account_key);
            }
            StorageParams::Ftp(cfg) => {
                mask_if_not_empty(&mut cfg.password);
            }
            StorageParams::Gcs(cfg) => {
                mask_if_not_empty(&mut cfg.credential);
            }
            StorageParams::Obs(cfg) => {
                mask_if_not_empty(&mut cfg.access_key_id);
                mask_if_not_empty(&mut cfg.secret_access_key);
            }
            StorageParams::Oss(cfg) => {
                mask_if_not_empty(&mut cfg.access_key_id);
                mask_if_not_empty(&mut cfg.access_key_secret);
                mask_if_not_empty(&mut cfg.server_side_encryption);
                mask_if_not_empty(&mut cfg.server_side_encryption_key_id);
            }
            StorageParams::S3(cfg) => {
                mask_if_not_empty(&mut cfg.access_key_id);
                mask_if_not_empty(&mut cfg.secret_access_key);
                mask_if_not_empty(&mut cfg.security_token);
                mask_if_not_empty(&mut cfg.master_key);
                mask_if_not_empty(&mut cfg.external_id);
            }
            StorageParams::Cos(cfg) => {
                mask_if_not_empty(&mut cfg.secret_id);
                mask_if_not_empty(&mut cfg.secret_key);
            }
            StorageParams::Huggingface(cfg) => {
                mask_if_not_empty(&mut cfg.token);
            }
            StorageParams::Webhdfs(cfg) => {
                mask_if_not_empty(&mut cfg.delegation);
            }
            _ => {}
        }
    }

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
                // Remove the possible trailing `/` in endpoint.
                let endpoint = s3.endpoint_url.trim_end_matches('/');

                // Make sure the endpoint contains the scheme.
                let endpoint = if endpoint.starts_with("http") {
                    endpoint.to_string()
                } else {
                    // Prefix https if endpoint doesn't start with scheme.
                    format!("https://{}", endpoint)
                };

                s3.region = timeout(
                    Duration::from_secs(DEFAULT_DETECT_REGION_TIMEOUT_SEC),
                    opendal::services::S3::detect_region(&endpoint, &s3.bucket),
                )
                .await
                .map_err(|e| {
                    ErrorCode::StorageOther(format!(
                        "detect region timeout: {}s, endpoint: {}, elapsed: {}",
                        DEFAULT_DETECT_REGION_TIMEOUT_SEC, endpoint, e
                    ))
                })?
                .unwrap_or_default();

                StorageParams::S3(s3)
            }
            v => v,
        };

        Ok(sp)
    }

    /// Apply the update from another StorageParams.
    ///
    /// Only specific storage params like `credential` can be updated.
    pub fn apply_update(self, other: Self) -> Result<Self> {
        match (self, other) {
            (StorageParams::Azblob(mut s1), StorageParams::Azblob(s2)) => {
                s1.account_name = s2.account_name;
                s1.account_key = s2.account_key;
                s1.network_config = s2.network_config;
                Ok(Self::Azblob(s1))
            }
            (StorageParams::Gcs(mut s1), StorageParams::Gcs(s2)) => {
                s1.credential = s2.credential;
                s1.network_config = s2.network_config;
                Ok(Self::Gcs(s1))
            }
            (StorageParams::S3(mut s1), StorageParams::S3(s2)) => {
                s1.access_key_id = s2.access_key_id;
                s1.secret_access_key = s2.secret_access_key;
                s1.security_token = s2.security_token;
                s1.role_arn = s2.role_arn;
                s1.external_id = s2.external_id;
                s1.master_key = s2.master_key;
                s1.network_config = s2.network_config;
                s1.disable_credential_loader = s2.disable_credential_loader;
                s1.allow_credential_chain = s2.allow_credential_chain;
                Ok(Self::S3(s1))
            }
            (s1, s2) => Err(ErrorCode::StorageOther(format!(
                "Cannot apply update from {:?} to {:?}",
                &s1, &s2
            ))),
        }
    }

    /// Try to reconstruct the URL that was used when the storage params were created.
    ///
    /// This is primarily used for displaying stage/connection information. It best-effort
    /// recreates the `CreateStageStmt.location` string for bucket-style backends (S3, GCS,
    /// OSS, OBS, COS, Azblob), file-like backends (FS, WebHDFS, HDFS), as well as HuggingFace
    /// and IPFS locations. HTTP locations render the expanded glob patterns (if any), so the
    /// output can differ from the original shorthand. Backends that don't carry enough context
    /// (Memory, Moka, None) return `None`.
    pub fn url(&self) -> Option<String> {
        match self {
            StorageParams::Azblob(cfg) => bucket_style_url("azblob", &cfg.container, &cfg.root),
            StorageParams::Fs(cfg) => {
                Some(format!("fs://{}", normalized_dir_path(&cfg.root, false)))
            }
            StorageParams::Ftp(cfg) => {
                if cfg.endpoint.is_empty() {
                    None
                } else {
                    Some(concat_endpoint_and_root(&cfg.endpoint, &cfg.root))
                }
            }
            StorageParams::Gcs(cfg) => bucket_style_url("gcs", &cfg.bucket, &cfg.root),
            StorageParams::Hdfs(cfg) => {
                if cfg.name_node.is_empty() {
                    None
                } else {
                    let host = cfg
                        .name_node
                        .strip_prefix("hdfs://")
                        .unwrap_or(cfg.name_node.as_str())
                        .trim_end_matches('/');
                    if host.is_empty() {
                        None
                    } else {
                        Some(format!(
                            "hdfs://{}{}",
                            host,
                            normalized_dir_path(&cfg.root, true)
                        ))
                    }
                }
            }
            StorageParams::Http(cfg) => {
                if cfg.paths.is_empty() {
                    None
                } else {
                    Some(render_http_url(cfg))
                }
            }
            StorageParams::Ipfs(cfg) => {
                let suffix = cfg.root.strip_prefix("/ipfs").unwrap_or(cfg.root.as_str());
                Some(format!("ipfs://ipfs{}", normalized_dir_path(suffix, true)))
            }
            StorageParams::Memory => None,
            StorageParams::Moka(_) => None,
            StorageParams::Obs(cfg) => bucket_style_url("obs", &cfg.bucket, &cfg.root),
            StorageParams::Oss(cfg) => bucket_style_url("oss", &cfg.bucket, &cfg.root),
            StorageParams::S3(cfg) => bucket_style_url("s3", &cfg.bucket, &cfg.root),
            StorageParams::Webhdfs(cfg) => {
                let host = strip_scheme(&cfg.endpoint_url).trim_end_matches('/');
                if host.is_empty() {
                    None
                } else {
                    Some(format!(
                        "webhdfs://{}{}",
                        host,
                        normalized_dir_path(&cfg.root, true)
                    ))
                }
            }
            StorageParams::Cos(cfg) => bucket_style_url("cos", &cfg.bucket, &cfg.root),
            StorageParams::Huggingface(cfg) => {
                let (owner, repo) = cfg.repo_id.split_once('/')?;
                Some(format!(
                    "hf://{}/{}{}",
                    owner,
                    repo,
                    normalized_dir_path(&cfg.root, true)
                ))
            }
            StorageParams::None => None,
        }
    }

    /// Return the endpoint URL if this storage exposes one.
    pub fn endpoint(&self) -> Option<String> {
        fn some_if_not_empty(s: &str) -> Option<String> {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        }

        match self {
            StorageParams::Azblob(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Ftp(cfg) => some_if_not_empty(&cfg.endpoint),
            StorageParams::Gcs(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Http(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Ipfs(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Obs(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Oss(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::S3(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Cos(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Webhdfs(cfg) => some_if_not_empty(&cfg.endpoint_url),
            StorageParams::Hdfs(cfg) => some_if_not_empty(&cfg.name_node),
            _ => None,
        }
    }

    /// Return true if this storage params contains any embedded credentials.
    pub fn has_credentials(&self) -> bool {
        match self {
            StorageParams::Azblob(cfg) => {
                !cfg.account_name.is_empty() || !cfg.account_key.is_empty()
            }
            StorageParams::Ftp(cfg) => !cfg.username.is_empty() || !cfg.password.is_empty(),
            StorageParams::Gcs(cfg) => !cfg.credential.is_empty(),
            StorageParams::Obs(cfg) => {
                !cfg.access_key_id.is_empty() || !cfg.secret_access_key.is_empty()
            }
            StorageParams::Oss(cfg) => {
                !cfg.access_key_id.is_empty() || !cfg.access_key_secret.is_empty()
            }
            StorageParams::S3(cfg) => {
                !cfg.access_key_id.is_empty()
                    || !cfg.secret_access_key.is_empty()
                    || !cfg.security_token.is_empty()
                    || !cfg.role_arn.is_empty()
                    || !cfg.external_id.is_empty()
            }
            StorageParams::Cos(cfg) => !cfg.secret_id.is_empty() || !cfg.secret_key.is_empty(),
            StorageParams::Huggingface(cfg) => !cfg.token.is_empty(),
            StorageParams::Webhdfs(cfg) => !cfg.delegation.is_empty(),
            StorageParams::Fs(_)
            | StorageParams::Hdfs(_)
            | StorageParams::Http(_)
            | StorageParams::Ipfs(_)
            | StorageParams::Memory
            | StorageParams::Moka(_)
            | StorageParams::None => false,
        }
    }

    /// Return true if this storage params has any user-provided encryption key material.
    pub fn has_encryption_key(&self) -> bool {
        match self {
            StorageParams::S3(cfg) => !cfg.master_key.is_empty(),
            StorageParams::Oss(cfg) => {
                !cfg.server_side_encryption.is_empty()
                    || !cfg.server_side_encryption_key_id.is_empty()
            }
            _ => false,
        }
    }
}

/// StorageParams will be displayed by `{protocol}://{key1=value1},{key2=value2}`
impl Display for StorageParams {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            StorageParams::Memory => write!(f, "memory"),
            StorageParams::Moka(v) => write!(f, "moka | max_capacity={}", v.max_capacity),
            StorageParams::None => write!(f, "none"),
            StorageParams::Azblob(v) => write!(
                f,
                "azblob | container={},root={},endpoint={}",
                v.container, v.root, v.endpoint_url
            ),
            StorageParams::Fs(v) => write!(f, "fs | root={}", v.root),
            StorageParams::Ftp(v) => write!(f, "ftp | root={},endpoint={}", v.root, v.endpoint),
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
                    "s3 | bucket={},root={},endpoint={},ak={},iam_role={}",
                    v.bucket,
                    v.root,
                    v.endpoint_url,
                    &mask_string(&v.access_key_id, 3),
                    v.role_arn,
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
        }
    }
}

fn normalized_dir_path(root: &str, absolute: bool) -> String {
    if root.is_empty() {
        return "/".to_string();
    }
    let mut normalized = root.to_string();
    if absolute && !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

fn bucket_style_url(protocol: &str, name: &str, root: &str) -> Option<String> {
    if name.is_empty() {
        None
    } else {
        Some(format!(
            "{}://{}{}",
            protocol,
            name,
            normalized_dir_path(root, true)
        ))
    }
}

fn concat_endpoint_and_root(endpoint: &str, root: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    format!("{}{}", endpoint, normalized_dir_path(root, true))
}

fn ensure_path_prefix(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

fn render_http_url(cfg: &StorageHttpConfig) -> String {
    let endpoint = cfg.endpoint_url.trim_end_matches('/');
    if cfg.paths.len() <= 1 {
        let path = cfg.paths.first().map(|p| p.as_str()).unwrap_or("/");
        format!("{}{}", endpoint, ensure_path_prefix(path))
    } else {
        let joined = cfg
            .paths
            .iter()
            .map(|p| ensure_path_prefix(p))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}{{{}}}", endpoint, joined)
    }
}

fn strip_scheme(endpoint: &str) -> &str {
    endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint)
}

/// Config for storage backend azblob.
#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageAzblobConfig {
    pub endpoint_url: String,
    pub container: String,
    pub account_name: String,
    pub account_key: String,
    pub root: String,
    pub network_config: Option<StorageNetworkParams>,
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
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageFtpConfig {
    pub endpoint: String,
    pub root: String,
    pub username: String,
    pub password: String,
    pub network_config: Option<StorageNetworkParams>,
}

impl Default for StorageFtpConfig {
    fn default() -> Self {
        Self {
            endpoint: STORAGE_FTP_DEFAULT_ENDPOINT.to_string(),
            username: "".to_string(),
            password: "".to_string(),
            root: "/".to_string(),
            network_config: Default::default(),
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
#[derive(Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct StorageGcsConfig {
    pub endpoint_url: String,
    pub bucket: String,
    pub root: String,
    pub credential: String,
    pub network_config: Option<StorageNetworkParams>,
}

impl Default for StorageGcsConfig {
    fn default() -> Self {
        Self {
            endpoint_url: STORAGE_GCS_DEFAULT_ENDPOINT.to_string(),
            bucket: String::new(),
            root: String::new(),
            credential: String::new(),
            network_config: Default::default(),
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
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageHdfsConfig {
    pub name_node: String,
    pub root: String,
    pub network_config: Option<StorageNetworkParams>,
}

pub static STORAGE_S3_DEFAULT_ENDPOINT: &str = "https://s3.amazonaws.com";

/// The S3 Storage Classes we utilizes
///
/// Applies to:
/// - Fuse tables (including external fuse tables with S3 locations)
///
/// **Important:** Only effective for S3 object storage. Some S3-compatible storage systems
/// (e.g., MinIO) do not support AWS S3 storage classes and will return errors during
/// PutObject operations if IntelligentTiering is specified.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Debug, Copy, Default)]
pub enum S3StorageClass {
    #[default]
    Standard,
    IntelligentTiering,
}

pub fn set_s3_storage_class(storage_params: &mut StorageParams, s3_storage_class: S3StorageClass) {
    if let StorageParams::S3(config) = storage_params {
        config.storage_class = s3_storage_class;
    };
}

const S3_STORAGE_CLASS_STANDARD: &str = "STANDARD";
const S3_STORAGE_CLASS_INTELLIGENT_TIERING: &str = "INTELLIGENT_TIERING";
impl Display for S3StorageClass {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            S3StorageClass::Standard => {
                write!(f, "{}", S3_STORAGE_CLASS_STANDARD)
            }
            S3StorageClass::IntelligentTiering => {
                write!(f, "{}", S3_STORAGE_CLASS_INTELLIGENT_TIERING)
            }
        }
    }
}
impl std::str::FromStr for S3StorageClass {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            S3_STORAGE_CLASS_STANDARD => Ok(S3StorageClass::Standard),
            S3_STORAGE_CLASS_INTELLIGENT_TIERING => Ok(S3StorageClass::IntelligentTiering),
            _ => Err(format!(
                "Unsupported S3 storage class '{}'. Supported values: 'standard', 'intelligent_tiering'",
                s
            )),
        }
    }
}

/// Config for storage backend s3.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    ///
    /// Deprecated: prefer the runtime `allow_credential_chain` policy plus global
    /// `storage.disable_config_load` / `storage.disable_instance_profile`.
    #[serde(skip)]
    pub disable_credential_loader: bool,
    /// Runtime-only override for whether ambient credential chains are allowed.
    ///
    /// This value is never serialized/persisted and is only used when building
    /// operators in the current process.
    #[serde(skip)]
    pub allow_credential_chain: Option<bool>,
    /// Enable this flag to send API in virtual host style.
    ///
    /// - Virtual Host Style: `https://bucket.s3.amazonaws.com`
    /// - Path Style: `https://s3.amazonaws.com/bucket`
    pub enable_virtual_host_style: bool,
    /// The RoleArn that used for AssumeRole.
    pub role_arn: String,
    /// The ExternalId that used for AssumeRole.
    pub external_id: String,
    pub network_config: Option<StorageNetworkParams>,
    pub storage_class: S3StorageClass,
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
            allow_credential_chain: None,
            enable_virtual_host_style: false,
            role_arn: "".to_string(),
            external_id: "".to_string(),
            network_config: Default::default(),
            storage_class: S3StorageClass::default(),
        }
    }
}

impl Debug for StorageS3Config {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageS3Config")
            .field("endpoint_url", &self.endpoint_url)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("storage_class", &self.storage_class)
            .field("root", &self.root)
            .field("disable_credential_loader", &self.disable_credential_loader)
            .field("allow_credential_chain", &self.allow_credential_chain)
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
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageHttpConfig {
    pub endpoint_url: String,
    pub paths: Vec<String>,
    pub network_config: Option<StorageNetworkParams>,
}

pub const STORAGE_IPFS_DEFAULT_ENDPOINT: &str = "https://ipfs.io";

/// Config for IPFS storage backend
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageIpfsConfig {
    pub endpoint_url: String,
    pub root: String,
    pub network_config: Option<StorageNetworkParams>,
}

/// Config for storage backend obs.
#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageObsConfig {
    pub endpoint_url: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub root: String,
    pub network_config: Option<StorageNetworkParams>,
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
#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    pub network_config: Option<StorageNetworkParams>,
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
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageMokaConfig {
    pub max_capacity: u64,
    pub time_to_live: i64,
    pub time_to_idle: i64,
}

impl Default for StorageMokaConfig {
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
#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageWebhdfsConfig {
    pub endpoint_url: String,
    pub root: String,
    pub delegation: String,
    pub disable_list_batch: bool,
    pub user_name: String,
    pub network_config: Option<StorageNetworkParams>,
}

impl Debug for StorageWebhdfsConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut ds = f.debug_struct("StorageWebhdfsConfig");

        ds.field("endpoint_url", &self.endpoint_url)
            .field("root", &self.root)
            .field("disable_list_batch", &self.disable_list_batch)
            .field("user_name", &self.user_name);

        ds.field("delegation", &mask_string(&self.delegation, 3));

        ds.finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageCosConfig {
    pub secret_id: String,
    pub secret_key: String,
    pub bucket: String,
    pub endpoint_url: String,
    pub root: String,
    pub network_config: Option<StorageNetworkParams>,
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

#[derive(Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    pub network_config: Option<StorageNetworkParams>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageNetworkParams {
    pub retry_timeout: u64,
    pub retry_io_timeout: u64,
    pub tcp_keepalive: u64,
    pub connect_timeout: u64,
    pub pool_max_idle_per_host: usize,
    pub max_concurrent_io_requests: usize,
}
