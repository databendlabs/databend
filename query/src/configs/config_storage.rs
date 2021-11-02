// Copyright 2020 Datafuse Labs.
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

use structopt::StructOpt;
use structopt_toml::StructOptToml;

use crate::configs::Config;

pub const STORAGE_TYPE: &str = "STORAGE_TYPE";

// Disk Storage env.
pub const DISK_STORAGE_DATA_PATH: &str = "DISK_STORAGE_DATA_PATH";

// S3 Storage env.
const S3_STORAGE_REGION: &str = "S3_STORAGE_REGION";
const S3_STORAGE_ENDPOINT_URL: &str = "S3_STORAGE_ENDPOINT_URL";

const S3_STORAGE_ACCESS_KEY_ID: &str = "S3_STORAGE_ACCESS_KEY_ID";
const S3_STORAGE_SECRET_ACCESS_KEY: &str = "S3_STORAGE_SECRET_ACCESS_KEY";
const S3_STORAGE_BUCKET: &str = "S3_STORAGE_BUCKET";

// Azure Storage Blob env.
const AZURE_STORAGE_ACCOUNT: &str = "AZURE_STORAGE_ACCOUNT";
const AZURE_BLOB_MASTER_KEY: &str = "AZURE_BLOB_MASTER_KEY";
const AZURE_BLOB_CONTAINER: &str = "AZURE_BLOB_CONTAINER";

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq)]
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

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct DiskStorageConfig {
    #[structopt(long, env = DISK_STORAGE_DATA_PATH, default_value = "", help = "Disk storage backend address")]
    #[serde(default)]
    pub data_path: String,
}

impl DiskStorageConfig {
    pub fn default() -> Self {
        DiskStorageConfig {
            data_path: "".to_string(),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct S3StorageConfig {
    #[structopt(long, env = S3_STORAGE_REGION, default_value = "", help = "Region for S3 storage")]
    #[serde(default)]
    pub region: String,

    #[structopt(long, env = S3_STORAGE_ENDPOINT_URL, default_value = "", help = "Endpoint URL for S3 storage")]
    #[serde(default)]
    pub endpoint_url: String,

    #[structopt(long, env = S3_STORAGE_ACCESS_KEY_ID, default_value = "", help = "Access key for S3 storage")]
    #[serde(default)]
    pub access_key_id: String,

    #[structopt(long, env = S3_STORAGE_SECRET_ACCESS_KEY, default_value = "", help = "Secret key for S3 storage")]
    #[serde(default)]
    pub secret_access_key: String,

    #[structopt(long, env = S3_STORAGE_BUCKET, default_value = "", help = "S3 Bucket to use for storage")]
    #[serde(default)]
    pub bucket: String,
}

impl S3StorageConfig {
    pub fn default() -> Self {
        S3StorageConfig {
            region: "".to_string(),
            endpoint_url: "".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            bucket: "".to_string(),
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

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct AzureStorageBlobConfig {
    #[structopt(long, env = AZURE_STORAGE_ACCOUNT, default_value = "", help = "Account for Azure storage")]
    #[serde(default)]
    pub account: String,

    #[structopt(long, env = AZURE_BLOB_MASTER_KEY, default_value = "", help = "Master key for Azure storage")]
    #[serde(default)]
    pub master_key: String,

    #[structopt(long, env = AZURE_BLOB_CONTAINER, default_value = "", help = "Container for Azure storage")]
    #[serde(default)]
    pub container: String,
}

impl AzureStorageBlobConfig {
    pub fn default() -> Self {
        AzureStorageBlobConfig {
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
/// serde(default) make the toml de to default working.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct StorageConfig {
    #[structopt(long, env = STORAGE_TYPE, default_value = "", help = "Current storage type: disk|s3")]
    #[serde(default)]
    pub storage_type: String,

    // Disk storage backend config.
    #[structopt(flatten)]
    pub disk: DiskStorageConfig,

    // S3 storage backend config.
    #[structopt(flatten)]
    pub s3: S3StorageConfig,

    // azure storage blob config.
    #[structopt(flatten)]
    pub azure_storage_blob: AzureStorageBlobConfig,
}

impl StorageConfig {
    pub fn default() -> Self {
        StorageConfig {
            storage_type: "disk".to_string(),
            disk: DiskStorageConfig::default(),
            s3: S3StorageConfig::default(),
            azure_storage_blob: AzureStorageBlobConfig::default(),
        }
    }

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
