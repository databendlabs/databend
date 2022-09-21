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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

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

/// Storage params which contains the detailed storage info.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageParams {
    Azblob(StorageAzblobConfig),
    Fs(StorageFsConfig),
    Gcs(StorageGcsConfig),
    #[cfg(feature = "storage-hdfs")]
    Hdfs(StorageHdfsConfig),
    Http(StorageHttpConfig),
    Memory,
    Obs(StorageObsConfig),
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
                "azblob://container={},root={},endpoint={}",
                v.container, v.root, v.endpoint_url
            ),
            StorageParams::Fs(v) => write!(f, "fs://root={}", v.root),
            StorageParams::Gcs(v) => write!(
                f,
                "gcs://bucket={},root={},endpoint={}",
                v.bucket, v.root, v.endpoint_url
            ),
            #[cfg(feature = "storage-hdfs")]
            StorageParams::Hdfs(v) => {
                write!(f, "hdfs://root={},name_node={}", v.root, v.name_node)
            }
            StorageParams::Http(v) => {
                write!(f, "http://endpoint={},paths={:?}", v.endpoint_url, v.paths)
            }
            StorageParams::Memory => write!(f, "memory://"),
            StorageParams::Obs(v) => write!(
                f,
                "obs://bucket={},root={},endpoint={}",
                v.bucket, v.root, v.endpoint_url
            ),
            StorageParams::S3(v) => {
                write!(
                    f,
                    "s3://bucket={},root={},endpoint={}",
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
            #[cfg(feature = "storage-hdfs")]
            StorageParams::Hdfs(_) => false,
            StorageParams::Http(v) => v.endpoint_url.starts_with("https://"),
            StorageParams::Memory => false,
            StorageParams::Obs(v) => v.endpoint_url.starts_with("https://"),
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
}

impl Default for StorageS3Config {
    fn default() -> Self {
        StorageS3Config {
            endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
            region: "".to_string(),
            bucket: "".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            master_key: "".to_string(),
            root: "".to_string(),
            disable_credential_loader: false,
            enable_virtual_host_style: false,
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
            .field("access_key_id", &mask_string(&self.access_key_id, 3))
            .field(
                "secret_access_key",
                &mask_string(&self.secret_access_key, 3),
            )
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
