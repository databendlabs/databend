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

use backon::ExponentialBackoff;
use common_base::base::GlobalIORuntime;
use common_base::base::Singleton;
use common_contexts::DalRuntime;
use common_exception::ErrorCode;
use once_cell::sync::OnceCell;
use opendal::layers::ImmutableIndexLayer;
use opendal::layers::LoggingLayer;
use opendal::layers::MetricsLayer;
use opendal::layers::RetryLayer;
use opendal::layers::TracingLayer;
use opendal::services::azblob;
use opendal::services::fs;
use opendal::services::gcs;
use opendal::services::http;
use opendal::services::memory;
use opendal::services::obs;
use opendal::services::s3;
use opendal::Operator;

use super::StorageAzblobConfig;
use super::StorageFsConfig;
use super::StorageParams;
use super::StorageS3Config;
use crate::config::StorageGcsConfig;
use crate::config::StorageHttpConfig;
use crate::config::StorageObsConfig;
use crate::StorageConfig;

/// init_operator will init an opendal operator based on storage config.
pub fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    Ok(match &cfg {
        StorageParams::Azblob(cfg) => init_azblob_operator(cfg)?,
        StorageParams::Fs(cfg) => init_fs_operator(cfg)?,
        StorageParams::Gcs(cfg) => init_gcs_operator(cfg)?,
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => init_hdfs_operator(cfg)?,
        StorageParams::Http(cfg) => init_http_operator(cfg)?,
        StorageParams::Memory => init_memory_operator()?,
        StorageParams::Obs(cfg) => init_obs_operator(cfg)?,
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

/// init_gcs_operator will init a opendal gcs operator.
pub fn init_gcs_operator(cfg: &StorageGcsConfig) -> Result<Operator> {
    let mut builder = gcs::Builder::default();

    let accessor = builder
        .endpoint(&cfg.endpoint_url)
        .bucket(&cfg.bucket)
        .root(&cfg.root)
        .credential(&cfg.credential)
        .build()?;

    Ok(Operator::new(accessor))
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

    // HTTP Service is read-only and doesn't support list operation.
    // ImmutableIndexLayer will build an in-memory immutable index for it.
    let mut immutable_layer = ImmutableIndexLayer::default();
    let files: Vec<String> = cfg.paths.iter().map(|v| v.to_string()).collect();
    // TODO: should be replace by `immutable_layer.extend_iter()` after fix
    for i in files {
        immutable_layer.insert(i);
    }

    Ok(Operator::new(builder.build()?).layer(immutable_layer))
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

/// init_obs_operator will init a opendal obs operator with input obs config.
pub fn init_obs_operator(cfg: &StorageObsConfig) -> Result<Operator> {
    let mut builder = obs::Builder::default();
    // Endpoint
    builder.endpoint(&cfg.endpoint_url);
    // Bucket
    builder.bucket(&cfg.bucket);
    // Root
    builder.root(&cfg.root);
    // Credential
    builder.access_key_id(&cfg.access_key_id);
    builder.secret_access_key(&cfg.secret_access_key);

    Ok(Operator::new(builder.build()?))
}

pub struct StorageOperator;

static STORAGE_OPERATOR: OnceCell<Singleton<Operator>> = OnceCell::new();

impl StorageOperator {
    pub async fn init(
        conf: &StorageConfig,
        v: Singleton<Operator>,
    ) -> common_exception::Result<()> {
        v.init(Self::try_create(conf).await?)?;

        STORAGE_OPERATOR.set(v).ok();
        Ok(())
    }

    pub async fn try_create(conf: &StorageConfig) -> common_exception::Result<Operator> {
        let io_runtime = GlobalIORuntime::instance();
        let operator = init_operator(&conf.params)?
            .layer(RetryLayer::new(ExponentialBackoff::default()))
            .layer(LoggingLayer)
            .layer(TracingLayer)
            .layer(MetricsLayer);

        // OpenDAL will send a real request to underlying storage to check whether it works or not.
        // If this check failed, it's highly possible that the users have configured it wrongly.
        if let Err(cause) = operator.check().await {
            return Err(ErrorCode::StorageUnavailable(format!(
                "current configured storage is not available: config: {:?}, cause: {cause}",
                conf
            )));
        }

        // NOTE: Magic happens here. We will add a layer upon original storage operator
        // so that all underlying storage operations will send to storage runtime.
        Ok(operator.layer(DalRuntime::new(io_runtime.inner())))
    }

    pub fn instance() -> Operator {
        match STORAGE_OPERATOR.get() {
            None => panic!("StorageOperator is not init"),
            Some(storage_operator) => storage_operator.get(),
        }
    }
}
