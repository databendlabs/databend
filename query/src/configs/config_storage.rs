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

use structopt::StructOpt;
use structopt_toml::StructOptToml;

use crate::configs::Config;

const STORAGE_TYPE: &str = "STORAGE_TYPE";

// Disk Storage env.
const DISK_STORAGE_DATA_PATH: &str = "DISK_STORAGE_DATA_PATH";

// S3 Storage env.
const S3_STORAGE_REGION: &str = "S3_STORAGE_REGION";
const S3_STORAGE_ACCESS_KEY_ID: &str = "S3_STORAGE_ACCESS_KEY_ID";
const S3_STORAGE_SECRET_ACCESS_KEY: &str = "S3_STORAGE_SECRET_ACCESS_KEY";
const S3_STORAGE_BUCKET: &str = "S3_STORAGE_BUCKET";

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum StorageType {
    Disk,
    S3,
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
        write!(f, "}}")
    }
}

/// Storage config group.
/// serde(default) make the toml de to default working.
#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, StructOpt, StructOptToml,
)]
pub struct StorageConfig {
    #[structopt(long, env = STORAGE_TYPE, default_value = "", help = "Current storage type: dfs|disk|s3")]
    #[serde(default)]
    pub storage_type: String,

    // Disk storage backend config.
    #[structopt(flatten)]
    pub disk: DiskStorageConfig,

    // S3 storage backend config.
    #[structopt(flatten)]
    pub s3: S3StorageConfig,
}

impl StorageConfig {
    pub fn default() -> Self {
        StorageConfig {
            storage_type: "disk".to_string(),
            disk: DiskStorageConfig::default(),
            s3: S3StorageConfig::default(),
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
    }
}
