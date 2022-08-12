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
use std::sync::Arc;
use once_cell::sync::OnceCell;

use opendal::services::azblob;
use opendal::services::fs;
use opendal::services::http;
use opendal::services::memory;
use opendal::services::s3;
use opendal::Operator;
use common_base::base::GlobalIORuntime;
use common_contexts::DalRuntime;
use common_exception::ErrorCode;

use super::StorageAzblobConfig;
use super::StorageFsConfig;
use super::StorageParams;
use super::StorageS3Config;
use crate::config::StorageHttpConfig;
use crate::StorageConfig;

/// init_operator will init an opendal operator based on storage config.
pub async fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    Ok(match &cfg {
        StorageParams::Azblob(cfg) => init_azblob_operator(cfg).await?,
        StorageParams::Fs(cfg) => init_fs_operator(cfg).await?,
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => init_hdfs_operator(cfg).await?,
        StorageParams::Http(cfg) => init_http_operator(cfg).await?,
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

/// init_hdfs_operator will init an opendal hdfs operator.
#[cfg(feature = "storage-hdfs")]
pub async fn init_hdfs_operator(cfg: &super::StorageHdfsConfig) -> Result<Operator> {
    use opendal::services::hdfs;

    let mut builder = hdfs::Backend::build();

    // Endpoint.
    builder.name_node(&cfg.name_node);

    // Root
    builder.root(&cfg.root);

    Ok(Operator::new(builder.finish().await?))
}

pub async fn init_http_operator(cfg: &StorageHttpConfig) -> Result<Operator> {
    let mut builder = http::Backend::build();

    // Endpoint.
    builder.endpoint(&cfg.endpoint_url);

    // Update index.
    builder.extend_index(cfg.paths.iter().map(|v| v.as_str()));

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

    // Enable virtual host style
    if cfg.enable_virtual_host_style {
        builder.enable_virtual_host_style();
    }

    Ok(Operator::new(builder.finish().await?))
}

pub struct StorageOperator;

static STORAGE_OPERATOR: OnceCell<Operator> = OnceCell::new();

impl StorageOperator {
    pub async fn init(conf: &StorageConfig) -> common_exception::Result<()> {
        let io_runtime = GlobalIORuntime::instance();
        let operator = init_operator(&conf.params).await?;
        // Enable exponential backoff by default
        let operator = operator.with_backoff(backon::ExponentialBackoff::default());

        // OpenDAL will send a real request to underlying storage to check whether it works or not.
        // If this check failed, it's highly possible that the users have configured it wrongly.
        if let Err(cause) = operator.check().await {
            return Err(ErrorCode::StorageUnavailable(format!(
                "current configured storage is not available: {cause}"
            )));
        }

        // NOTE: Magic happens here. We will add a layer upon original storage operator
        // so that all underlying storage operations will send to storage runtime.
        let operator = operator.layer(DalRuntime::new(io_runtime.inner()));

        match STORAGE_OPERATOR.set(operator) {
            Ok(_) => Ok(()),
            Err(_) => Err(ErrorCode::LogicalError("Cannot init StorageOperator twice"))
        }
    }

    pub fn instance() -> Operator {
        match STORAGE_OPERATOR.get() {
            None => panic!("StorageOperator is not init"),
            Some(storage_operator) => storage_operator.clone(),
        }
    }
}

