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
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::time::Duration;

use anyhow::anyhow;
use common_base::base::GlobalInstance;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_meta_app::storage::StorageAzblobConfig;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageGcsConfig;
#[cfg(feature = "storage-hdfs")]
use common_meta_app::storage::StorageHdfsConfig;
use common_meta_app::storage::StorageHttpConfig;
use common_meta_app::storage::StorageIpfsConfig;
use common_meta_app::storage::StorageMokaConfig;
use common_meta_app::storage::StorageObsConfig;
use common_meta_app::storage::StorageOssConfig;
use common_meta_app::storage::StorageParams;
use common_meta_app::storage::StorageRedisConfig;
use common_meta_app::storage::StorageS3Config;
use opendal::layers::ImmutableIndexLayer;
use opendal::layers::LoggingLayer;
use opendal::layers::MetricsLayer;
use opendal::layers::RetryLayer;
use opendal::layers::TracingLayer;
use opendal::raw::Accessor;
use opendal::raw::Layer;
use opendal::services;
use opendal::Builder;
use opendal::Operator;

use crate::runtime_layer::RuntimeLayer;
use crate::CacheConfig;
use crate::StorageConfig;

/// init_operator will init an opendal operator based on storage config.
pub fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    let op = match &cfg {
        StorageParams::Azblob(cfg) => build_operator(init_azblob_operator(cfg)?),
        StorageParams::Fs(cfg) => build_operator(init_fs_operator(cfg)?),
        StorageParams::Gcs(cfg) => build_operator(init_gcs_operator(cfg)?),
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => build_operator(init_hdfs_operator(cfg)?),
        StorageParams::Http(cfg) => build_operator(init_http_operator(cfg)?),
        StorageParams::Ipfs(cfg) => build_operator(init_ipfs_operator(cfg)?),
        StorageParams::Memory => build_operator(init_memory_operator()?),
        StorageParams::Moka(cfg) => build_operator(init_moka_operator(cfg)?),
        StorageParams::Obs(cfg) => build_operator(init_obs_operator(cfg)?),
        StorageParams::S3(cfg) => build_operator(init_s3_operator(cfg)?),
        StorageParams::Oss(cfg) => build_operator(init_oss_operator(cfg)?),
        StorageParams::Redis(cfg) => build_operator(init_redis_operator(cfg)?),
        v => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("Unsupported storage type: {:?}", v),
            ));
        }
    };

    Ok(op)
}

pub fn build_operator<A: Accessor>(acc: A) -> Operator {
    let ob = Operator::new(acc);

    ob
        // Add retry
        .layer(RetryLayer::new().with_jitter())
        // Add metrics
        .layer(MetricsLayer)
        // Add logging
        .layer(LoggingLayer::default())
        // Add tracing
        .layer(TracingLayer)
        // NOTE
        //
        // Magic happens here. We will add a layer upon original
        // storage operator so that all underlying storage operations
        // will send to storage runtime.
        .layer(RuntimeLayer::new(GlobalIORuntime::instance().inner()))
        .finish()
}

/// init_azblob_operator will init an opendal azblob operator.
pub fn init_azblob_operator(cfg: &StorageAzblobConfig) -> Result<impl Accessor> {
    let mut builder = services::Azblob::default();

    // Endpoint
    builder.endpoint(&cfg.endpoint_url);

    // Container
    builder.container(&cfg.container);

    // Root
    builder.root(&cfg.root);

    // Credential
    builder.account_name(&cfg.account_name);
    builder.account_key(&cfg.account_key);

    Ok(builder.build()?)
}

/// init_fs_operator will init a opendal fs operator.
fn init_fs_operator(cfg: &StorageFsConfig) -> Result<impl Accessor> {
    let mut builder = services::Fs::default();

    let mut path = cfg.root.clone();
    if !path.starts_with('/') {
        path = env::current_dir().unwrap().join(path).display().to_string();
    }
    builder.root(&path);

    Ok(builder.build()?)
}

/// init_gcs_operator will init a opendal gcs operator.
fn init_gcs_operator(cfg: &StorageGcsConfig) -> Result<impl Accessor> {
    let mut builder = services::Gcs::default();

    builder
        .endpoint(&cfg.endpoint_url)
        .bucket(&cfg.bucket)
        .root(&cfg.root)
        .credential(&cfg.credential);

    Ok(builder.build()?)
}

/// init_hdfs_operator will init an opendal hdfs operator.
#[cfg(feature = "storage-hdfs")]
fn init_hdfs_operator(cfg: &StorageHdfsConfig) -> Result<impl Accessor> {
    let mut builder = services::Hdfs::default();

    // Endpoint.
    builder.name_node(&cfg.name_node);

    // Root
    builder.root(&cfg.root);

    Ok(builder.build()?)
}

fn init_ipfs_operator(cfg: &StorageIpfsConfig) -> Result<impl Accessor> {
    let mut builder = services::Ipfs::default();

    builder.root(&cfg.root);
    builder.endpoint(&cfg.endpoint_url);

    Ok(builder.build()?)
}

fn init_http_operator(cfg: &StorageHttpConfig) -> Result<impl Accessor> {
    let mut builder = services::Http::default();

    // Endpoint.
    builder.endpoint(&cfg.endpoint_url);

    // HTTP Service is read-only and doesn't support list operation.
    // ImmutableIndexLayer will build an in-memory immutable index for it.
    let mut immutable_layer = ImmutableIndexLayer::default();
    let files: Vec<String> = cfg
        .paths
        .iter()
        .map(|v| v.trim_start_matches('/').to_string())
        .collect();
    // TODO: should be replace by `immutable_layer.extend_iter()` after fix
    for i in files {
        immutable_layer.insert(i);
    }

    Ok(immutable_layer.layer(builder.build()?))
}

/// init_memory_operator will init a opendal memory operator.
fn init_memory_operator() -> Result<impl Accessor> {
    let mut builder = services::Memory::default();

    Ok(builder.build()?)
}

/// init_s3_operator will init a opendal s3 operator with input s3 config.
fn init_s3_operator(cfg: &StorageS3Config) -> Result<impl Accessor> {
    let mut builder = services::S3::default();

    // Endpoint.
    builder.endpoint(&cfg.endpoint_url);

    // Region
    builder.region(&cfg.region);

    // Credential.
    builder.access_key_id(&cfg.access_key_id);
    builder.secret_access_key(&cfg.secret_access_key);
    builder.security_token(&cfg.security_token);
    builder.role_arn(&cfg.role_arn);
    builder.external_id(&cfg.external_id);

    // Bucket.
    builder.bucket(&cfg.bucket);

    // Root.
    builder.root(&cfg.root);

    // Disable credential loader
    if cfg.disable_credential_loader {
        builder.disable_config_load();
    }

    // Enable virtual host style
    if cfg.enable_virtual_host_style {
        builder.enable_virtual_host_style();
    }

    Ok(builder.build()?)
}

/// init_obs_operator will init a opendal obs operator with input obs config.
fn init_obs_operator(cfg: &StorageObsConfig) -> Result<impl Accessor> {
    let mut builder = services::Obs::default();
    // Endpoint
    builder.endpoint(&cfg.endpoint_url);
    // Bucket
    builder.bucket(&cfg.bucket);
    // Root
    builder.root(&cfg.root);
    // Credential
    builder.access_key_id(&cfg.access_key_id);
    builder.secret_access_key(&cfg.secret_access_key);

    Ok(builder.build()?)
}

/// init_oss_operator will init an opendal OSS operator with input oss config.
fn init_oss_operator(cfg: &StorageOssConfig) -> Result<impl Accessor> {
    let mut builder = services::Oss::default();

    builder
        .endpoint(&cfg.endpoint_url)
        .presign_endpoint(&cfg.presign_endpoint_url)
        .access_key_id(&cfg.access_key_id)
        .access_key_secret(&cfg.access_key_secret)
        .bucket(&cfg.bucket)
        .root(&cfg.root);

    Ok(builder.build()?)
}

/// init_moka_operator will init a moka operator.
fn init_moka_operator(v: &StorageMokaConfig) -> Result<impl Accessor> {
    let mut builder = services::Moka::default();

    builder.max_capacity(v.max_capacity);
    builder.time_to_live(std::time::Duration::from_secs(v.time_to_live as u64));
    builder.time_to_idle(std::time::Duration::from_secs(v.time_to_idle as u64));

    Ok(builder.build()?)
}

/// init_redis_operator will init a reids operator.
fn init_redis_operator(v: &StorageRedisConfig) -> Result<impl Accessor> {
    let mut builder = services::Redis::default();

    builder.endpoint(&v.endpoint_url);
    builder.root(&v.root);
    builder.db(v.db);
    if let Some(v) = v.default_ttl {
        builder.default_ttl(Duration::from_secs(v as u64));
    }
    if let Some(v) = &v.username {
        builder.username(v);
    }
    if let Some(v) = &v.password {
        builder.password(v);
    }

    Ok(builder.build()?)
}

/// DataOperator is the operator to access persist data services.
///
/// # Notes
///
/// All data accessed via this operator will be persisted.
#[derive(Clone, Debug)]
pub struct DataOperator {
    operator: Operator,
    params: StorageParams,
}

impl DataOperator {
    /// Get the operator from PersistOperator
    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn params(&self) -> StorageParams {
        self.params.clone()
    }

    pub async fn init(conf: &StorageConfig) -> common_exception::Result<()> {
        GlobalInstance::set(Self::try_create(&conf.params).await?);

        Ok(())
    }

    pub async fn try_create(sp: &StorageParams) -> common_exception::Result<DataOperator> {
        let operator = init_operator(sp)?;

        // OpenDAL will send a real request to underlying storage to check whether it works or not.
        // If this check failed, it's highly possible that the users have configured it wrongly.
        //
        // Make sure the check is called inside GlobalIORuntime to prevent
        // IO hang on reuse connection.
        let op = operator.clone();
        if let Err(cause) = GlobalIORuntime::instance()
            .spawn(async move { op.check().await })
            .await
            .expect("join must succeed")
        {
            return Err(ErrorCode::StorageUnavailable(format!(
                "current configured storage is not available: config: {:?}, cause: {cause}",
                sp
            )));
        }

        Ok(DataOperator {
            operator,
            params: sp.clone(),
        })
    }

    pub fn instance() -> DataOperator {
        GlobalInstance::get()
    }
}

/// CacheOperator is the operator to access cache services.
///
/// # Notes
///
/// As described in [RFC: Cache](https://databend.rs/doc/contributing/rfcs/cache):
///
/// All data stored in cache operator should be non-persist and could be GC or
/// background auto evict at any time.
#[derive(Clone, Debug)]
pub struct CacheOperator {
    op: Option<Operator>,
}

impl CacheOperator {
    pub async fn init(conf: &CacheConfig) -> common_exception::Result<()> {
        GlobalInstance::set(Self::try_create(conf).await?);

        Ok(())
    }

    pub async fn try_create(conf: &CacheConfig) -> common_exception::Result<CacheOperator> {
        if conf.params == StorageParams::None {
            return Ok(CacheOperator { op: None });
        }

        let operator = init_operator(&conf.params)?;

        // OpenDAL will send a real request to underlying storage to check whether it works or not.
        // If this check failed, it's highly possible that the users have configured it wrongly.
        //
        // Make sure the check is called inside GlobalIORuntime to prevent
        // IO hang on reuse connection.
        let op = operator.clone();
        if let Err(cause) = GlobalIORuntime::instance()
            .spawn(async move { op.object("health_check").create().await })
            .await
            .expect("join must succeed")
        {
            return Err(ErrorCode::StorageUnavailable(format!(
                "current configured cache is not available: config: {:?}, cause: {cause}",
                conf
            )));
        }

        Ok(CacheOperator { op: Some(operator) })
    }

    pub fn instance() -> Option<Operator> {
        let v: CacheOperator = GlobalInstance::get();
        v.inner()
    }

    fn inner(&self) -> Option<Operator> {
        self.op.clone()
    }
}
