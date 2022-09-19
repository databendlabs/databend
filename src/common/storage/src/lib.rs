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

//! `common_storage` will provide storage related types and functions.
//!
//! This crate will return `std::io::Result`.

mod config;
pub use config::StorageAzblobConfig;
pub use config::StorageConfig;
pub use config::StorageFsConfig;
pub use config::StorageGcsConfig;
pub use config::StorageHdfsConfig;
pub use config::StorageHttpConfig;
pub use config::StorageIpfsConfig;
pub use config::StorageObsConfig;
pub use config::StorageParams;
pub use config::StorageS3Config;
pub use config::STORAGE_GCS_DEFAULT_ENDPOINT;
pub use config::STORAGE_IPFS_DEFAULT_ENDPOINT;
pub use config::STORAGE_S3_DEFAULT_ENDPOINT;

mod operator;
pub use operator::init_azblob_operator;
pub use operator::init_fs_operator;
pub use operator::init_gcs_operator;
#[cfg(feature = "storage-hdfs")]
pub use operator::init_hdfs_operator;
pub use operator::init_http_operator;
pub use operator::init_memory_operator;
pub use operator::init_obs_operator;
pub use operator::init_operator;
pub use operator::init_s3_operator;
pub use operator::StorageOperator;

mod location;
pub use location::parse_uri_location;
pub use location::UriLocation;

mod utils;
