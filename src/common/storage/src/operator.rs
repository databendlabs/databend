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
use std::io::Result;

use opendal::services::azblob;
use opendal::services::fs;
use opendal::services::http;
use opendal::services::memory;
use opendal::services::s3;
use opendal::Operator;

use super::StorageAzblobConfig;
use super::StorageFsConfig;
use super::StorageParams;
use super::StorageS3Config;
use crate::config::StorageHttpConfig;

/// init_operator will init an opendal operator based on storage config.
pub fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    Ok(match &cfg {
        StorageParams::Azblob(cfg) => init_azblob_operator(cfg)?,
        StorageParams::Fs(cfg) => init_fs_operator(cfg)?,
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => init_hdfs_operator(cfg)?,
        StorageParams::Http(cfg) => init_http_operator(cfg)?,
        StorageParams::Memory => init_memory_operator()?,
        StorageParams::S3(cfg) => init_s3_operator(cfg)?,
    })
}

/// init_azblob_operator will init an opendal azblob operator.
pub fn init_azblob_operator(cfg: &StorageAzblobConfig) -> Result<Operator> {
    let mut builder = azblob::Builder::default();

    // Endpoint
    builder.endpoint(&cfg.endpoint_url);

    // Container
    builder.container(&cfg.container);

    // Root
    builder.root(&cfg.root);

    // Credential
    builder.account_name(&cfg.account_name);
    builder.account_key(&cfg.account_key);

    Ok(Operator::new(builder.build()?))
}

/// init_fs_operator will init a opendal fs operator.
pub fn init_fs_operator(cfg: &StorageFsConfig) -> Result<Operator> {
    let mut builder = fs::Builder::default();

    let mut path = cfg.root.clone();
    if !path.starts_with('/') {
        path = env::current_dir().unwrap().join(path).display().to_string();
    }
    builder.root(&path);

    Ok(Operator::new(builder.build()?))
}

/// init_hdfs_operator will init an opendal hdfs operator.
#[cfg(feature = "storage-hdfs")]
pub fn init_hdfs_operator(cfg: &super::StorageHdfsConfig) -> Result<Operator> {
    use opendal::services::hdfs;

    let mut builder = hdfs::Builder::default();

    // Endpoint.
    builder.name_node(&cfg.name_node);

    // Root
    builder.root(&cfg.root);

    Ok(Operator::new(builder.build()?))
}

pub fn init_http_operator(cfg: &StorageHttpConfig) -> Result<Operator> {
    let mut builder = http::Builder::default();

    // Endpoint.
    builder.endpoint(&cfg.endpoint_url);

    // Update index.
    builder.extend_index(cfg.paths.iter().map(|v| v.as_str()));

    Ok(Operator::new(builder.build()?))
}

/// init_memory_operator will init a opendal memory operator.
pub fn init_memory_operator() -> Result<Operator> {
    let mut builder = memory::Builder::default();

    Ok(Operator::new(builder.build()?))
}

/// init_s3_operator will init a opendal s3 operator with input s3 config.
pub fn init_s3_operator(cfg: &StorageS3Config) -> Result<Operator> {
    let mut builder = s3::Builder::default();

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

    // Enable virtual host style
    if cfg.enable_virtual_host_style {
        builder.enable_virtual_host_style();
    }

    Ok(Operator::new(builder.build()?))
}
