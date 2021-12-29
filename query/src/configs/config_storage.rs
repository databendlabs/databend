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
use serde::Deserialize;
use serde::Serialize;

use crate::configs::Config;

pub const STORAGE_TYPE: &str = "STORAGE_TYPE";

// Disk Storage env.
pub const DISK_STORAGE_DATA_PATH: &str = "DISK_STORAGE_DATA_PATH";
pub const DISK_STORAGE_TEMP_DATA_PATH: &str = "DISK_STORAGE_TEMP_DATA_PATH";

// S3 Storage env.
const S3_STORAGE_REGION: &str = "S3_STORAGE_REGION";
const S3_STORAGE_ENDPOINT_URL: &str = "S3_STORAGE_ENDPOINT_URL";

const S3_STORAGE_ACCESS_KEY_ID: &str = "S3_STORAGE_ACCESS_KEY_ID";
const S3_STORAGE_SECRET_ACCESS_KEY: &str = "S3_STORAGE_SECRET_ACCESS_KEY";
const S3_STORAGE_ENABLE_POD_IAM_POLICY: &str = "S3_STORAGE_ENABLE_POD_IAM_POLICY";
const S3_STORAGE_BUCKET: &str = "S3_STORAGE_BUCKET";

// Azure Storage Blob env.
const AZURE_STORAGE_ACCOUNT: &str = "AZURE_STORAGE_ACCOUNT";
const AZURE_BLOB_MASTER_KEY: &str = "AZURE_BLOB_MASTER_KEY";
const AZURE_BLOB_CONTAINER: &str = "AZURE_BLOB_CONTAINER";

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum StorageType {
    Disk,
    S3,
    AzureStorageBlob,
}

// Implement the trait
impl FromStr for StorageType {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<StorageType, &'static str> {
        match s {
            "disk" => Ok(StorageType::Disk),
            "s3" => Ok(StorageType::S3),
            "azure_storage_blob" => Ok(StorageType::AzureStorageBlob),
            _ => Err("no match for storage type"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct DiskStorageConfig {
    /// Disk storage backend data path
    #[clap(long, env = DISK_STORAGE_DATA_PATH, default_value = "_data")]
    pub data_path: String,

    /// Disk storage temporary data path for external data
    #[clap(long, env = DISK_STORAGE_TEMP_DATA_PATH, default_value = "")]
    pub temp_data_path: String,
}

impl Default for DiskStorageConfig {
    fn default() -> Self {
        Self {
            data_path: "_data".to_string(),
            temp_data_path: "".to_string(),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Args)]
#[serde(default)]
pub struct S3StorageConfig {
    /// Region for S3 storage
    #[clap(long, env = S3_STORAGE_REGION, default_value = "")]
    pub region: String,

    /// Endpoint URL for S3 storage
    #[clap(long, env = S3_STORAGE_ENDPOINT_URL, default_value = "")]
    pub endpoint_url: String,

    // Access key for S3 storage
    #[clap(long, env = S3_STORAGE_ACCESS_KEY_ID, default_value = "")]
    pub access_key_id: String,

    /// Secret key for S3 storage
    #[clap(long, env = S3_STORAGE_SECRET_ACCESS_KEY, default_value = "")]
    pub secret_access_key: String,

    /// Use iam role service account token to access S3 resource
    #[clap(long, env = S3_STORAGE_ENABLE_POD_IAM_POLICY)]
    pub enable_pod_iam_policy: bool,

    /// S3 Bucket to use for storage
    #[clap(long, env = S3_STORAGE_BUCKET, default_value = "")]
    pub bucket: String,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            region: "".to_string(),
            endpoint_url: "".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            bucket: "".to_string(),
            enable_pod_iam_policy: false,
        }
    }
}

impl fmt::Debug for S3StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "s3.storage.region: \"{}\", ", self.region)?;
        write!(f, "s3.storage.endpoint_url: \"{}\", ", self.endpoint_url)?;
        write!(f, "s3.storage.bucket: \"{}\", ", self.bucket)?;
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
    /// Current storage type: disk|s3
    #[clap(long, env = STORAGE_TYPE, default_value = "disk")]
    pub storage_type: String,

    // Disk storage backend config.
    #[clap(flatten)]
    pub disk: DiskStorageConfig,

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
            storage_type: "disk".to_string(),
            disk: DiskStorageConfig::default(),
            s3: S3StorageConfig::default(),
            azure_storage_blob: AzureStorageBlobConfig::default(),
        }
    }
}

impl StorageConfig {
    pub fn load_from_env(mut_config: &mut Config) {
        env_helper!(mut_config, storage, storage_type, String, STORAGE_TYPE);

        // DISK.
        env_helper!(
            mut_config.storage,
            disk,
            data_path,
            String,
            DISK_STORAGE_DATA_PATH
        );

        env_helper!(
            mut_config.storage,
            disk,
            temp_data_path,
            String,
            DISK_STORAGE_TEMP_DATA_PATH
        );

        // S3.
        env_helper!(mut_config.storage, s3, region, String, S3_STORAGE_REGION);
        env_helper!(
            mut_config.storage,
            s3,
            endpoint_url,
            String,
            S3_STORAGE_ENDPOINT_URL
        );
        env_helper!(
            mut_config.storage,
            s3,
            access_key_id,
            String,
            S3_STORAGE_ACCESS_KEY_ID
        );
        env_helper!(
            mut_config.storage,
            s3,
            secret_access_key,
            String,
            S3_STORAGE_SECRET_ACCESS_KEY
        );
        env_helper!(
            mut_config.storage,
            s3,
            enable_pod_iam_policy,
            bool,
            S3_STORAGE_ENABLE_POD_IAM_POLICY
        );
        env_helper!(mut_config.storage, s3, bucket, String, S3_STORAGE_BUCKET);

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
