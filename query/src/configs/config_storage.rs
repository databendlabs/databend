// Copyright 2021 Datafuse Labs.
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
use std::str::FromStr;

use clap::Args;
use common_base::mask_string;
use serde::Deserialize;
use serde::Serialize;

use crate::configs::Config;

pub const STORAGE_TYPE: &str = "STORAGE_TYPE";
pub const STORAGE_NUM_CPUS: &str = "STORAGE_NUM_CPUS";

// Fs Storage env.
pub const FS_STORAGE_DATA_PATH: &str = "FS_STORAGE_DATA_PATH";

// Azure Storage Blob env.
const AZURE_STORAGE_ACCOUNT: &str = "AZURE_STORAGE_ACCOUNT";
const AZURE_BLOB_MASTER_KEY: &str = "AZURE_BLOB_MASTER_KEY";
const AZURE_BLOB_CONTAINER: &str = "AZURE_BLOB_CONTAINER";

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum StorageType {
    Fs,
    S3,
    AzureStorageBlob,
}

// Implement the trait
impl FromStr for StorageType {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<StorageType, &'static str> {
        match s {
            "fs" => Ok(StorageType::Fs),
            "s3" => Ok(StorageType::S3),
            "azure_storage_blob" => Ok(StorageType::AzureStorageBlob),
            _ => Err("no match for storage type"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct FsStorageConfig {
    /// fs storage backend data path
    #[clap(long, env = FS_STORAGE_DATA_PATH, default_value = "_data")]
    pub data_path: String,
}

impl Default for FsStorageConfig {
    fn default() -> Self {
        Self {
            data_path: "_data".to_string(),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct S3StorageConfig {
    /// Region for S3 storage
    #[clap(long = "storage-s3-region", default_value = "")]
    pub region: String,

    /// Endpoint URL for S3 storage
    #[clap(
        long = "storage-s3-endpoint-url",
        default_value = "https://s3.amazonaws.com"
    )]
    pub endpoint_url: String,

    // Access key for S3 storage
    #[clap(long = "storage-s3-access-key-id", default_value = "")]
    pub access_key_id: String,

    /// Secret key for S3 storage
    #[clap(long = "storage-s3-secret-access-key", default_value = "")]
    pub secret_access_key: String,

    /// S3 Bucket to use for storage
    #[clap(long = "storage-s3-bucket", default_value = "")]
    pub bucket: String,

    /// <root>
    #[clap(long = "storage-s3-root", default_value = "")]
    pub root: String,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            region: "".to_string(),
            endpoint_url: "https://s3.amazonaws.com".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            bucket: "".to_string(),
            root: "".to_string(),
        }
    }
}

impl fmt::Debug for S3StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "s3.storage.region: \"{}\", ", self.region)?;
        write!(f, "s3.storage.endpoint_url: \"{}\", ", self.endpoint_url)?;
        write!(f, "s3.storage.bucket: \"{}\", ", self.bucket)?;
        write!(
            f,
            "s3.storage.access_key_id: \"{}\", ",
            mask_string(&self.access_key_id[..], 3)
        )?;
        write!(
            f,
            "s3.storage.secret_access_key: \"{}\", ",
            mask_string(&self.secret_access_key[..], 3)
        )?;
        write!(f, "}}")
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct AzureStorageBlobConfig {
    /// Account for Azure storage
    #[clap(long, env = AZURE_STORAGE_ACCOUNT, default_value = "")]
    pub account: String,

    /// Master key for Azure storage
    #[clap(long, env = AZURE_BLOB_MASTER_KEY, default_value = "")]
    pub master_key: String,

    /// Container for Azure storage
    #[clap(long, env = AZURE_BLOB_CONTAINER, default_value = "")]
    pub container: String,
}

impl Default for AzureStorageBlobConfig {
    fn default() -> Self {
        Self {
            account: "".to_string(),
            master_key: "".to_string(),
            container: "".to_string(),
        }
    }
}

impl fmt::Debug for AzureStorageBlobConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "Azure.storage.container: \"{}\", ", self.container)?;
        write!(f, "}}")
    }
}

/// Storage config group.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct StorageConfig {
    /// Current storage type: fs|s3
    #[clap(long, env = STORAGE_TYPE, default_value = "fs")]
    pub storage_type: String,

    #[clap(long, env = STORAGE_NUM_CPUS, default_value = "0")]
    pub storage_num_cpus: u64,

    // Fs storage backend config.
    #[clap(flatten)]
    pub fs: FsStorageConfig,

    // S3 storage backend config.
    #[clap(flatten)]
    pub s3: S3StorageConfig,

    // azure storage blob config.
    #[clap(flatten)]
    pub azure_storage_blob: AzureStorageBlobConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_type: "fs".to_string(),
            fs: FsStorageConfig::default(),
            s3: S3StorageConfig::default(),
            azure_storage_blob: AzureStorageBlobConfig::default(),
            storage_num_cpus: 0,
        }
    }
}

impl StorageConfig {
    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(mut_config, storage, storage_type, String, STORAGE_TYPE);
        env_helper!(mut_config, storage, storage_num_cpus, u64, STORAGE_NUM_CPUS);

        // DISK.
        env_helper!(
            mut_config.storage,
            fs,
            data_path,
            String,
            FS_STORAGE_DATA_PATH
        );

        // Azure Storage Blob.
        env_helper!(
            mut_config.storage,
            azure_storage_blob,
            account,
            String,
            AZURE_BLOB_MASTER_KEY
        );
    }
}
