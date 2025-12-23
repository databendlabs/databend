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
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::anyhow;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::storage::S3StorageClass;
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
use databend_common_meta_app::storage::StorageNetworkParams;
use databend_common_meta_app::storage::StorageObsConfig;
use databend_common_meta_app::storage::StorageOssConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_app::storage::StorageWebhdfsConfig;
use databend_common_meta_app::storage::set_s3_storage_class;
use databend_enterprise_storage_encryption::get_storage_encryption_handler;
use log::warn;
use opendal::Builder;
use opendal::Operator;
use opendal::layers::AsyncBacktraceLayer;
use opendal::layers::ConcurrentLimitLayer;
use opendal::layers::FastraceLayer;
use opendal::layers::HttpClientLayer;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryInterceptor;
use opendal::layers::RetryLayer;
use opendal::layers::TimeoutLayer;
use opendal::raw::HttpClient;
use opendal::services;
use opendal_layer_immutable_index::ImmutableIndexLayer;

use crate::StorageConfig;
use crate::StorageHttpClient;
use crate::http_client::get_storage_http_client;
use crate::metrics_layer::METRICS_LAYER;
use crate::operator_cache::get_operator_cache;
use crate::runtime_layer::RuntimeLayer;

static METRIC_OPENDAL_RETRIES_COUNT: LazyLock<FamilyCounter<Vec<(&'static str, String)>>> =
    LazyLock::new(|| register_counter_family("opendal_retries_count"));

/// init_operator will init an opendal operator based on storage config.
pub fn init_operator(cfg: &StorageParams) -> Result<Operator> {
    let cache = get_operator_cache();
    cache
        .get_or_create(cfg)
        .map_err(|e| Error::other(anyhow!("Failed to get or create operator: {}", e)))
}

/// init_operator_uncached will init an opendal operator without caching.
/// This function creates a new operator every time it's called.
pub(crate) fn init_operator_uncached(cfg: &StorageParams) -> Result<Operator> {
    let op = match &cfg {
        StorageParams::Azblob(cfg) => {
            build_operator(init_azblob_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::Fs(cfg) => build_operator(init_fs_operator(cfg)?, None)?,
        StorageParams::Gcs(cfg) => {
            build_operator(init_gcs_operator(cfg)?, cfg.network_config.as_ref())?
        }
        #[cfg(feature = "storage-hdfs")]
        StorageParams::Hdfs(cfg) => {
            build_operator(init_hdfs_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::Http(cfg) => {
            let (builder, layer) = init_http_operator(cfg)?;
            build_operator(builder, cfg.network_config.as_ref())?.layer(layer)
        }
        StorageParams::Ipfs(cfg) => {
            build_operator(init_ipfs_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::Memory => build_operator(init_memory_operator()?, None)?,
        StorageParams::Moka(cfg) => build_operator(init_moka_operator(cfg)?, None)?,
        StorageParams::Obs(cfg) => {
            build_operator(init_obs_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::S3(cfg) => {
            build_operator(init_s3_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::Oss(cfg) => {
            build_operator(init_oss_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::Webhdfs(cfg) => {
            build_operator(init_webhdfs_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::Cos(cfg) => {
            build_operator(init_cos_operator(cfg)?, cfg.network_config.as_ref())?
        }
        StorageParams::Huggingface(cfg) => {
            build_operator(init_huggingface_operator(cfg)?, cfg.network_config.as_ref())?
        }
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
fn build_operator<B: Builder>(builder: B, cfg: Option<&StorageNetworkParams>) -> Result<Operator> {
    let ob = Operator::new(builder)?
        // Timeout layer is required to be the first layer so that internal
        // futures can be cancelled safely when the timeout is reached.
        .layer({
            let mut retry_timeout = match &cfg {
                None => 10,
                Some(v) if v.retry_timeout == 0 => 10,
                Some(v) => v.retry_timeout,
            };

            let mut retry_io_timeout = match &cfg {
                None => 60,
                Some(v) if v.retry_io_timeout == 0 => 60,
                Some(v) => v.retry_io_timeout,
            };

            if let Ok(v) = env::var("_DATABEND_INTERNAL_RETRY_TIMEOUT") {
                if let Ok(v) = v.parse::<u64>() {
                    retry_timeout = v;
                }
            }

            if let Ok(v) = env::var("_DATABEND_INTERNAL_RETRY_IO_TIMEOUT") {
                if let Ok(v) = v.parse::<u64>() {
                    retry_io_timeout = v;
                }
            }

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
        // NOTE
        //
        // Magic happens here. We will add a layer upon original
        // storage operator so that all underlying storage operations
        // will send to storage runtime.
        .layer(RuntimeLayer::new(GlobalIORuntime::instance()))
        .finish();

    // Make sure the http client has been updated.
    let ob = ob.layer(HttpClientLayer::new(HttpClient::with(get_http_client(cfg))));

    let mut op = ob
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

    let mut max_concurrent_request = cfg.as_ref().map(|x| x.max_concurrent_io_requests);

    if let Ok(permits) = env::var("_DATABEND_INTERNAL_MAX_CONCURRENT_IO_REQUEST") {
        if let Ok(permits) = permits.parse::<usize>() {
            max_concurrent_request = Some(permits);
        }
    }

    if let Some(permits) = max_concurrent_request {
        if permits != 0 {
            op = op.layer(ConcurrentLimitLayer::new(permits));
        }
    }

    Ok(op)
}

fn get_http_client(cfg: Option<&StorageNetworkParams>) -> StorageHttpClient {
    let mut pool_max_idle_per_host = usize::MAX;

    if let Some(cfg) = cfg {
        if cfg.pool_max_idle_per_host != 0 {
            pool_max_idle_per_host = cfg.pool_max_idle_per_host;
        }
    }

    if let Ok(v) = env::var("_DATABEND_INTERNAL_POOL_MAX_IDLE_PER_HOST") {
        if let Ok(v) = v.parse::<usize>() {
            pool_max_idle_per_host = v;
        }
    }

    // Connect timeout default to 30s.
    let mut connect_timeout = 30;
    if let Some(cfg) = cfg {
        if cfg.connect_timeout != 0 {
            connect_timeout = cfg.connect_timeout;
        }
    }

    if let Ok(v) = env::var("_DATABEND_INTERNAL_CONNECT_TIMEOUT") {
        if let Ok(v) = v.parse::<u64>() {
            connect_timeout = v;
        }
    }

    let mut keepalive = 0;

    if let Some(cfg) = cfg {
        if cfg.tcp_keepalive != 0 {
            keepalive = cfg.tcp_keepalive;
        }
    }

    if let Ok(v) = env::var("_DATABEND_INTERNAL_TCP_KEEPALIVE") {
        if let Ok(v) = v.parse::<u64>() {
            keepalive = v;
        }
    }

    get_storage_http_client(pool_max_idle_per_host, connect_timeout, keepalive)
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
        .account_key(&cfg.account_key);

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
        .credential(&cfg.credential);

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
        // Don't enable it otherwise we will get Permission in stat unknown files
        // .allow_anonymous()
        // Root.
        .root(&cfg.root);

    if cfg.storage_class != S3StorageClass::Standard {
        // Apply S3 storage class to the operator.
        // Note: Some S3-compatible storage systems (e.g., MinIO) may not support
        // AWS S3 storage classes and will fail during PutObject operations.
        builder = builder.default_storage_class(cfg.storage_class.to_string().as_ref())
    }

    // Disable credential loader
    if cfg.disable_credential_loader {
        builder = builder.disable_config_load().disable_ec2_metadata();
    }

    // Force anonymous (unsigned) requests only when credential loader is disabled and no explicit
    // credentials are provided. This is mainly for external stages to read public buckets safely
    // without accidentally using the tenant role from the environment (env/profile/IMDS/IRSA).
    //
    // Don't enable it when credential loader is allowed, otherwise it would bypass the default
    // credential chain that internal storage configurations may rely on.
    if cfg.disable_credential_loader
        && cfg.access_key_id.is_empty()
        && cfg.secret_access_key.is_empty()
        && cfg.security_token.is_empty()
        && cfg.role_arn.is_empty()
    {
        builder = builder.allow_anonymous();
    }

    // Enable virtual host style
    if cfg.enable_virtual_host_style {
        builder = builder.enable_virtual_host_style();
    }

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
        .secret_access_key(&cfg.secret_access_key);

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
        .server_side_encryption_key_id(&cfg.server_side_encryption_key_id);

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
        .root(&cfg.root);

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
    spill_operator: Operator,
    params: StorageParams,
    spill_params: Option<StorageParams>,
}

impl DataOperator {
    /// Get the operator from PersistOperator
    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn spill_operator(&self) -> Operator {
        self.spill_operator.clone()
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

    #[async_backtrace::framed]
    async fn try_create(
        conf: &StorageConfig,
        spill_params: Option<StorageParams>,
    ) -> databend_common_exception::Result<DataOperator> {
        let operator = init_operator(&conf.params)?;
        check_operator(&operator, &conf.params).await?;

        // Init spill operator
        let mut params = spill_params.as_ref().unwrap_or(&conf.params).clone();
        // Always use Standard storage class if spill to s3 object storage
        set_s3_storage_class(&mut params, S3StorageClass::Standard);
        let spill_operator = init_operator(&params)?;
        check_operator(&spill_operator, &params).await?;

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
            let res = op.stat("databend_storage_checker").await;
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
                "current configured storage is not valid: config: {:?}, cause: {cause}",
                params
            ))
        })
}

pub trait OperatorRegistry: Send + Sync {
    fn get_operator_path<'a>(&self, _location: &'a str) -> Result<(Operator, &'a str)>;
}

impl OperatorRegistry for Operator {
    fn get_operator_path<'a>(&self, location: &'a str) -> Result<(Operator, &'a str)> {
        Ok((self.clone(), location))
    }
}

impl OperatorRegistry for DataOperator {
    fn get_operator_path<'a>(&self, location: &'a str) -> Result<(Operator, &'a str)> {
        Ok((self.operator.clone(), location))
    }
}

impl OperatorRegistry for iceberg::io::FileIO {
    fn get_operator_path<'a>(&self, location: &'a str) -> Result<(Operator, &'a str)> {
        let file_io = self
            .new_input(location)
            .map_err(|err| std::io::Error::new(ErrorKind::Unsupported, err.message()))?;

        let pos = file_io.relative_path_pos();
        Ok((file_io.get_operator().clone(), &location[pos..]))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Scheme {
    Azblob,
    Gcs,
    Hdfs,
    Ipfs,
    S3,
    Oss,
    Obs,
    Cos,
    Http,
    Fs,
    Webhdfs,
    Huggingface,
    Custom(&'static str),
}

impl Scheme {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl From<Scheme> for &'static str {
    fn from(v: Scheme) -> Self {
        match v {
            Scheme::Azblob => "azblob",
            Scheme::Gcs => "gcs",
            Scheme::Hdfs => "hdfs",
            Scheme::Ipfs => "ipfs",
            Scheme::S3 => "s3",
            Scheme::Oss => "oss",
            Scheme::Obs => "obs",
            Scheme::Cos => "cos",
            Scheme::Http => "http",
            Scheme::Fs => "fs",
            Scheme::Webhdfs => "webhdfs",
            Scheme::Huggingface => "huggingface",
            Scheme::Custom(s) => s,
        }
    }
}

impl FromStr for Scheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_lowercase();
        match s.as_str() {
            "azblob" => Ok(Scheme::Azblob),
            "gcs" => Ok(Scheme::Gcs),
            "hdfs" => Ok(Scheme::Hdfs),
            "ipfs" => Ok(Scheme::Ipfs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "oss" => Ok(Scheme::Oss),
            "obs" => Ok(Scheme::Obs),
            "cos" => Ok(Scheme::Cos),
            "http" | "https" => Ok(Scheme::Http),
            "fs" => Ok(Scheme::Fs),
            "webhdfs" => Ok(Scheme::Webhdfs),
            "huggingface" | "hf" => Ok(Scheme::Huggingface),
            _ => Ok(Scheme::Custom(Box::leak(s.into_boxed_str()))),
        }
    }
}

impl std::fmt::Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}
