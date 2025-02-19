// Copyright 2021 Datafuse Labs
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
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::anyhow;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::storage::StorageAzblobConfig;
use databend_common_meta_app::storage::StorageCosConfig;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageGcsConfig;
#[cfg(feature = "storage-hdfs")]
use databend_common_meta_app::storage::StorageHdfsConfig;
use databend_common_meta_app::storage::StorageHttpConfig;
use databend_common_meta_app::storage::StorageHuggingfaceConfig;
use databend_common_meta_app::storage::StorageIpfsConfig;
use databend_common_meta_app::storage::StorageMokaConfig;
use databend_common_meta_app::storage::StorageObsConfig;
use databend_common_meta_app::storage::StorageOssConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_app::storage::StorageWebhdfsConfig;
use databend_enterprise_storage_encryption::get_storage_encryption_handler;
use log::warn;
use opendal::layers::AsyncBacktraceLayer;
use opendal::layers::ConcurrentLimitLayer;
use opendal::layers::FastraceLayer;
use opendal::layers::ImmutableIndexLayer;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryInterceptor;
use opendal::layers::RetryLayer;
use opendal::layers::TimeoutLayer;
use opendal::raw::HttpClient;
use opendal::services;
use opendal::Builder;
use opendal::Operator;

use crate::metrics_layer::METRICS_LAYER;
use crate::runtime_layer::RuntimeLayer;
use crate::StorageConfig;
use crate::StorageHttpClient;

static METRIC_OPENDAL_RETRIES_COUNT: LazyLock<FamilyCounter<Vec<(&'static str, String)>>> =
    LazyLock::new(|| register_counter_family("opendal_retries_count"));

/// init_operator will init an opendal operator based on storage config.
pub fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    let op = match &cfg {
        StorageParams::Azblob(cfg) => build_operator(init_azblob_operator(cfg)?)?,
        StorageParams::Fs(cfg) => build_operator(init_fs_operator(cfg)?)?,
        StorageParams::Gcs(cfg) => build_operator(init_gcs_operator(cfg)?)?,
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => build_operator(init_hdfs_operator(cfg)?)?,
        StorageParams::Http(cfg) => {
            let (builder, layer) = init_http_operator(cfg)?;
            build_operator(builder)?.layer(layer)
        }
        StorageParams::Ipfs(cfg) => build_operator(init_ipfs_operator(cfg)?)?,
        StorageParams::Memory => build_operator(init_memory_operator()?)?,
        StorageParams::Moka(cfg) => build_operator(init_moka_operator(cfg)?)?,
        StorageParams::Obs(cfg) => build_operator(init_obs_operator(cfg)?)?,
        StorageParams::S3(cfg) => build_operator(init_s3_operator(cfg)?)?,
        StorageParams::Oss(cfg) => build_operator(init_oss_operator(cfg)?)?,
        StorageParams::Webhdfs(cfg) => build_operator(init_webhdfs_operator(cfg)?)?,
        StorageParams::Cos(cfg) => build_operator(init_cos_operator(cfg)?)?,
        StorageParams::Huggingface(cfg) => build_operator(init_huggingface_operator(cfg)?)?,
        v => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("Unsupported storage type: {:?}", v),
            ));
        }
    };

    Ok(op)
}

/// Please take care about the timing of calling opendal's `finish`.
///
/// Layers added before `finish` will use static dispatch, and layers added after `finish`
/// will use dynamic dispatch. Adding too many layers via static dispatch will increase
/// the compile time of rustc or even results in a compile error.
///
/// ```txt
/// error[E0275]: overflow evaluating the requirement `http::response::Response<()>: std::marker::Send`
///      |
///      = help: consider increasing the recursion limit by adding a `#![recursion_limit = "256"]` attribute to your crate (`databend_common_storage`)
/// note: required because it appears within the type `h2::proto::peer::PollMessage`
///     --> /home/xuanwo/.cargo/registry/src/index.crates.io-6f17d22bba15001f/h2-0.4.5/src/proto/peer.rs:43:10
///      |
/// 43   | pub enum PollMessage {
///      |          ^^^^^^^^^^^
/// ```
///
/// Please balance the performance and compile time.
pub fn build_operator<B: Builder>(builder: B) -> Result<Operator> {
    let ob = Operator::new(builder)?
        // NOTE
        //
        // Magic happens here. We will add a layer upon original
        // storage operator so that all underlying storage operations
        // will send to storage runtime.
        .layer(RuntimeLayer::new(GlobalIORuntime::instance()))
        .finish();

    let mut op = ob
        .layer({
            let retry_timeout = env::var("_DATABEND_INTERNAL_RETRY_TIMEOUT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(10);
            let retry_io_timeout = env::var("_DATABEND_INTERNAL_RETRY_IO_TIMEOUT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60);

            let mut timeout_layer = TimeoutLayer::new();

            if retry_timeout != 0 {
                // Return timeout error if the operation timeout
                timeout_layer = timeout_layer.with_timeout(Duration::from_secs(retry_timeout));
            }

            if retry_io_timeout != 0 {
                // Return timeout error if the io operation timeout
                timeout_layer =
                    timeout_layer.with_io_timeout(Duration::from_secs(retry_io_timeout));
            }

            timeout_layer
        })
        // Add retry
        .layer(
            RetryLayer::new()
                .with_jitter()
                .with_notify(DatabendRetryInterceptor),
        )
        // Add async backtrace
        .layer(AsyncBacktraceLayer)
        // Add logging
        .layer(LoggingLayer::default())
        // Add tracing
        .layer(FastraceLayer)
        // Add PrometheusClientLayer
        .layer(METRICS_LAYER.clone());

    if let Ok(permits) = env::var("_DATABEND_INTERNAL_MAX_CONCURRENT_IO_REQUEST") {
        if let Ok(permits) = permits.parse::<usize>() {
            op = op.layer(ConcurrentLimitLayer::new(permits));
        }
    }

    Ok(op)
}

/// init_azblob_operator will init an opendal azblob operator.
pub fn init_azblob_operator(cfg: &StorageAzblobConfig) -> Result<impl Builder> {
    let builder = services::Azblob::default()
        // Endpoint
        .endpoint(&cfg.endpoint_url)
        // Container
        .container(&cfg.container)
        // Root
        .root(&cfg.root)
        // Credential
        .account_name(&cfg.account_name)
        .account_key(&cfg.account_key)
        .http_client(HttpClient::with(StorageHttpClient::default()));

    Ok(builder)
}

/// init_fs_operator will init a opendal fs operator.
fn init_fs_operator(cfg: &StorageFsConfig) -> Result<impl Builder> {
    let mut builder = services::Fs::default();

    let mut path = cfg.root.clone();
    if !path.starts_with('/') {
        path = env::current_dir().unwrap().join(path).display().to_string();
    }
    builder = builder.root(&path);

    Ok(builder)
}

/// init_gcs_operator will init a opendal gcs operator.
fn init_gcs_operator(cfg: &StorageGcsConfig) -> Result<impl Builder> {
    let builder = services::Gcs::default()
        .endpoint(&cfg.endpoint_url)
        .bucket(&cfg.bucket)
        .root(&cfg.root)
        .credential(&cfg.credential)
        .http_client(HttpClient::with(StorageHttpClient::default()));

    Ok(builder)
}

/// init_hdfs_operator will init an opendal hdfs operator.
#[cfg(feature = "storage-hdfs")]
fn init_hdfs_operator(cfg: &StorageHdfsConfig) -> Result<impl Builder> {
    let builder = services::Hdfs::default()
        // Endpoint.
        .name_node(&cfg.name_node)
        // Root
        .root(&cfg.root);

    Ok(builder)
}

fn init_ipfs_operator(cfg: &StorageIpfsConfig) -> Result<impl Builder> {
    let builder = services::Ipfs::default()
        .root(&cfg.root)
        .endpoint(&cfg.endpoint_url);

    Ok(builder)
}

fn init_http_operator(cfg: &StorageHttpConfig) -> Result<(impl Builder, ImmutableIndexLayer)> {
    let builder = services::Http::default()
        // Endpoint.
        .endpoint(&cfg.endpoint_url);

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

    Ok((builder, immutable_layer))
}

/// init_memory_operator will init a opendal memory operator.
fn init_memory_operator() -> Result<impl Builder> {
    let builder = services::Memory::default();

    Ok(builder)
}

/// init_s3_operator will init a opendal s3 operator with input s3 config.
fn init_s3_operator(cfg: &StorageS3Config) -> Result<impl Builder> {
    let mut builder = services::S3::default()
        // Endpoint.
        .endpoint(&cfg.endpoint_url)
        // Bucket.
        .bucket(&cfg.bucket);

    // Region
    if !cfg.region.is_empty() {
        builder = builder.region(&cfg.region);
    } else if let Ok(region) = env::var("AWS_REGION") {
        // Try to load region from env if not set.
        builder = builder.region(&region);
    } else {
        // FIXME: we should return error here but keep those logic for compatibility.
        warn!(
            "Region is not specified for S3 storage, we will attempt to load it from profiles. If it is still not found, we will use the default region of `us-east-1`."
        );
        builder = builder.region("us-east-1");
    }

    // Always enable versioning support.
    builder = builder.enable_versioning(true);

    // Credential.
    builder = builder
        .access_key_id(&cfg.access_key_id)
        .secret_access_key(&cfg.secret_access_key)
        .session_token(&cfg.security_token)
        .role_arn(&cfg.role_arn)
        .external_id(&cfg.external_id)
        // It's safe to allow anonymous since opendal will perform the check first.
        .allow_anonymous()
        // Root.
        .root(&cfg.root);

    // Disable credential loader
    if cfg.disable_credential_loader {
        builder = builder.disable_config_load().disable_ec2_metadata();
    }

    // Enable virtual host style
    if cfg.enable_virtual_host_style {
        builder = builder.enable_virtual_host_style();
    }

    builder = builder.http_client(HttpClient::with(StorageHttpClient::default()));

    Ok(builder)
}

/// init_obs_operator will init a opendal obs operator with input obs config.
fn init_obs_operator(cfg: &StorageObsConfig) -> Result<impl Builder> {
    let builder = services::Obs::default()
        // Endpoint
        .endpoint(&cfg.endpoint_url)
        // Bucket
        .bucket(&cfg.bucket)
        // Root
        .root(&cfg.root)
        // Credential
        .access_key_id(&cfg.access_key_id)
        .secret_access_key(&cfg.secret_access_key)
        .http_client(HttpClient::with(StorageHttpClient::default()));

    Ok(builder)
}

/// init_oss_operator will init an opendal OSS operator with input oss config.
fn init_oss_operator(cfg: &StorageOssConfig) -> Result<impl Builder> {
    let builder = services::Oss::default()
        .endpoint(&cfg.endpoint_url)
        .presign_endpoint(&cfg.presign_endpoint_url)
        .access_key_id(&cfg.access_key_id)
        .access_key_secret(&cfg.access_key_secret)
        .bucket(&cfg.bucket)
        .root(&cfg.root)
        .server_side_encryption(&cfg.server_side_encryption)
        .server_side_encryption_key_id(&cfg.server_side_encryption_key_id)
        .http_client(HttpClient::with(StorageHttpClient::default()));

    Ok(builder)
}

/// init_moka_operator will init a moka operator.
fn init_moka_operator(v: &StorageMokaConfig) -> Result<impl Builder> {
    let builder = services::Moka::default()
        .max_capacity(v.max_capacity)
        .time_to_live(std::time::Duration::from_secs(v.time_to_live as u64))
        .time_to_idle(std::time::Duration::from_secs(v.time_to_idle as u64));

    Ok(builder)
}

/// init_webhdfs_operator will init a WebHDFS operator
fn init_webhdfs_operator(v: &StorageWebhdfsConfig) -> Result<impl Builder> {
    let mut builder = services::Webhdfs::default()
        .endpoint(&v.endpoint_url)
        .root(&v.root)
        .delegation(&v.delegation)
        .user_name(&v.user_name);

    if v.disable_list_batch {
        builder = builder.disable_list_batch();
    }

    Ok(builder)
}

/// init_cos_operator will init an opendal COS operator with input oss config.
fn init_cos_operator(cfg: &StorageCosConfig) -> Result<impl Builder> {
    let builder = services::Cos::default()
        .endpoint(&cfg.endpoint_url)
        .secret_id(&cfg.secret_id)
        .secret_key(&cfg.secret_key)
        .bucket(&cfg.bucket)
        .root(&cfg.root)
        .http_client(HttpClient::with(StorageHttpClient::default()));

    Ok(builder)
}

/// init_huggingface_operator will init an opendal operator with input config.
fn init_huggingface_operator(cfg: &StorageHuggingfaceConfig) -> Result<impl Builder> {
    let builder = services::Huggingface::default()
        .repo_type(&cfg.repo_type)
        .repo_id(&cfg.repo_id)
        .revision(&cfg.revision)
        .token(&cfg.token)
        .root(&cfg.root);

    Ok(builder)
}

pub struct DatabendRetryInterceptor;

impl RetryInterceptor for DatabendRetryInterceptor {
    fn intercept(&self, err: &opendal::Error, dur: Duration) {
        let labels = vec![("err", err.kind().to_string())];
        METRIC_OPENDAL_RETRIES_COUNT.get_or_create(&labels).inc();
        warn!(
            target: "opendal::layers::retry",
            "will retry after {:.2}s because: {:?}",
            dur.as_secs_f64(), err)
    }
}

/// DataOperator is the operator to access persist data services.
///
/// # Notes
///
/// All data accessed via this operator will be persisted.
#[derive(Clone, Debug)]
pub struct DataOperator {
    operator: Operator,
    spill_operator: Option<Operator>,
    params: StorageParams,
    spill_params: Option<StorageParams>,
}

impl DataOperator {
    /// Get the operator from PersistOperator
    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn spill_operator(&self) -> Operator {
        match &self.spill_operator {
            Some(op) => op.clone(),
            None => self.operator.clone(),
        }
    }

    pub fn spill_params(&self) -> Option<&StorageParams> {
        self.spill_params.as_ref()
    }

    pub fn params(&self) -> StorageParams {
        self.params.clone()
    }

    #[async_backtrace::framed]
    pub async fn init(
        conf: &StorageConfig,
        spill_params: Option<StorageParams>,
    ) -> databend_common_exception::Result<()> {
        GlobalInstance::set(Self::try_create(conf, spill_params).await?);

        Ok(())
    }

    /// Create a new data operator without check.
    pub fn try_new(
        conf: &StorageConfig,
        spill_params: Option<StorageParams>,
    ) -> databend_common_exception::Result<DataOperator> {
        let operator = init_operator(&conf.params)?;
        let spill_operator = spill_params.as_ref().map(init_operator).transpose()?;

        Ok(DataOperator {
            operator,
            params: conf.params.clone(),
            spill_operator,
            spill_params,
        })
    }

    #[async_backtrace::framed]
    pub async fn try_create(
        conf: &StorageConfig,
        spill_params: Option<StorageParams>,
    ) -> databend_common_exception::Result<DataOperator> {
        let operator = init_operator(&conf.params)?;
        check_operator(&operator, &conf.params).await?;

        let spill_operator = match &spill_params {
            Some(params) => {
                let op = init_operator(params)?;
                check_operator(&op, params).await?;
                Some(op)
            }
            None => None,
        };

        Ok(DataOperator {
            operator,
            params: conf.params.clone(),
            spill_operator,
            spill_params,
        })
    }

    /// Check license must be run after license manager setup.
    pub async fn check_license(&self) -> databend_common_exception::Result<()> {
        if self.params.need_encryption_feature() {
            get_storage_encryption_handler().check_license().await?;
        }
        Ok(())
    }

    pub fn instance() -> DataOperator {
        GlobalInstance::get()
    }
}

pub async fn check_operator(
    operator: &Operator,
    params: &StorageParams,
) -> databend_common_exception::Result<()> {
    // OpenDAL will send a real request to underlying storage to check whether it works or not.
    // If this check failed, it's highly possible that the users have configured it wrongly.
    //
    // Make sure the check is called inside GlobalIORuntime to prevent
    // IO hang on reuse connection.
    let op = operator.clone();

    GlobalIORuntime::instance()
        .spawn(async move {
            let res = op.stat("/").await;
            match res {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e),
            }
        })
        .await
        .expect("join must succeed")
        .map_err(|cause| {
            ErrorCode::StorageUnavailable(format!(
                "current configured storage is not available: config: {:?}, cause: {cause}",
                params
            ))
        })
}
