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

use std::env;

use common_exception::Result;
use opendal::services::azblob;
use opendal::services::fs;
use opendal::services::memory;
use opendal::services::s3;
use opendal::Operator;

use super::configs::StorageAzblobConfig;
use super::configs::StorageConfig;
use super::configs::StorageFsConfig;
use super::configs::StorageParams;
use super::configs::StorageS3Config;

/// init_operator will init an opendal operator based on storage config.
pub async fn init_operator(cfg: &StorageConfig) -> Result<Operator> {
    Ok(match &cfg.params {
        StorageParams::Azblob(cfg) => init_azblob_operator(cfg).await?,
        StorageParams::Fs(cfg) => init_fs_operator(cfg).await?,
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => init_hdfs_operator(cfg).await?,
        StorageParams::Memory => init_memory_operator().await?,
        StorageParams::S3(cfg) => init_s3_operator(cfg).await?,
    })
}

/// init_azblob_operator will init an opendal azblob operator.
pub async fn init_azblob_operator(cfg: &StorageAzblobConfig) -> Result<Operator> {
    let mut builder = azblob::Backend::build();

    // Endpoint
    builder.endpoint(&cfg.endpoint_url);

    // Container
    builder.container(&cfg.container);

    // Root
    builder.root(&cfg.root);

    // Credential
    builder.account_name(&cfg.account_name);
    builder.account_key(&cfg.account_key);

    Ok(Operator::new(builder.finish().await?))
}

/// init_fs_operator will init a opendal fs operator.
pub async fn init_fs_operator(cfg: &StorageFsConfig) -> Result<Operator> {
    let mut builder = fs::Backend::build();

    let mut path = cfg.root.clone();
    if !path.starts_with('/') {
        path = env::current_dir().unwrap().join(path).display().to_string();
    }
    builder.root(&path);

    Ok(Operator::new(builder.finish().await?))
}

/// init_hdfs_operator will init a opendal hdfs operator.
#[cfg(feature = "storage-hdfs")]
pub async fn init_hdfs_operator(cfg: &super::configs::StorageHdfsConfig) -> Result<Operator> {
    use opendal::services::hdfs;

    let mut builder = hdfs::Backend::build();

    // Endpoint.
    builder.name_node(&cfg.name_node);

    // Root
    builder.root(&cfg.root);

    Ok(Operator::new(builder.finish().await?))
}

/// init_memory_operator will init a opendal memory operator.
pub async fn init_memory_operator() -> Result<Operator> {
    let mut builder = memory::Backend::build();

    Ok(Operator::new(builder.finish().await?))
}

/// init_s3_operator will init a opendal s3 operator with input s3 config.
pub async fn init_s3_operator(cfg: &StorageS3Config) -> Result<Operator> {
    let mut builder = s3::Backend::build();

    // Endpoint.
    builder.endpoint(&cfg.endpoint_url);

    // Region
    builder.region(&cfg.region);

    // Credential.
    builder.access_key_id(&cfg.access_key_id);
    builder.secret_access_key(&cfg.secret_access_key);

    // Bucket.
    builder.bucket(&cfg.bucket);

    // Root.
    builder.root(&cfg.root);

    // Disable credential loader
    if cfg.disable_credential_loader {
        builder.disable_credential_loader();
    }

    Ok(Operator::new(builder.finish().await?))
}
