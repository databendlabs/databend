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

use serde::Deserialize;
use serde::Serialize;

/// Config for storage backend.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageConfig {
    pub num_cpus: u64,

    pub params: StorageParams,
}

/// Storage params which contains the detailed storage info.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageParams {
    Azblob(StorageAzblobConfig),
    Fs(StorageFsConfig),
    #[cfg(feature = "storage-hdfs")]
    Hdfs(StorageHdfsConfig),
    Memory,
    S3(StorageS3Config),
}

impl Default for StorageParams {
    fn default() -> Self {
        StorageParams::Memory
    }
}

/// Config for storage backend azblob.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAzblobConfig {
    pub endpoint_url: String,
    pub container: String,
    pub account_name: String,
    pub account_key: String,
    pub root: String,
}

/// Config for storage backend fs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFsConfig {
    pub root: String,
}

/// Config for storage backend hdfs.
///
/// # Notes
///
/// Ideally, we should export this config only when hdfs feature enabled.
/// But export this struct without hdfs feature is safe and no harm. So we
/// export it to make crates' lives that depend on us easier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageHdfsConfig {
    pub name_node: String,
    pub root: String,
}

/// Config for storage backend s3.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageS3Config {
    pub endpoint_url: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub master_key: String,
    pub root: String,
}

impl Default for StorageS3Config {
    fn default() -> Self {
        StorageS3Config {
            endpoint_url: "https://s3.amazonaws.com".to_string(),
            region: "".to_string(),
            bucket: "".to_string(),
            access_key_id: "".to_string(),
            secret_access_key: "".to_string(),
            master_key: "".to_string(),
            root: "".to_string(),
        }
    }
}
