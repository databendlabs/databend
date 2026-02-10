// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Ok;
use anyhow::Result;
use anyhow::anyhow;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::session_type::SessionType;
use databend_common_config::CacheConfig;
use databend_common_config::Config;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_storage::init_operator;
use databend_common_users::UserApiProvider;
use databend_common_users::builtin::BuiltIn;
use databend_common_version::BUILD_INFO;
use databend_enterprise_query::license::RealLicenseManager;
use databend_meta::configs::MetaServiceConfig;
use databend_meta_cli_config::MetaConfig;
use databend_meta_client::ClientHandle;
use databend_meta_client::MetaGrpcClient;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::protobuf::ExportRequest;
use databend_query::sessions::BuildInfoRef;
use databend_query::sessions::SessionManager;
use futures::TryStream;
use futures::TryStreamExt;
use log::debug;
use opendal::Operator;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryLayer;

/// Load the configuration file of databend query.
///
/// The given input is the path to databend query's configuration file.
pub fn load_query_config(path: &str) -> Result<databend_common_config::InnerConfig> {
    let outer_config: Config = Config::load_with_config_file(path)?;
    let inner_config: InnerConfig = outer_config.try_into()?;
    Ok(inner_config)
}

/// Load the configuration file and return the operator for databend.
///
/// The given input is the path to databend's configuration file.
pub fn load_query_storage(cfg: &InnerConfig) -> Result<Operator> {
    let op = init_operator(&cfg.storage.params)?;
    debug!("databend storage loaded: {:?}", op.info());
    Ok(op)
}

/// Load the configuration file of databend meta.
///
/// The given input is the path to databend meta's configuration file.
pub fn load_meta_config(path: &str) -> Result<MetaServiceConfig> {
    let content = std::fs::read_to_string(path)?;
    let outer_config: databend_meta_cli_config::Config = toml::from_str(&content)?;
    let meta_config: MetaConfig = outer_config
        .try_into()
        .map_err(|msg: String| anyhow!("{msg}"))?;
    let service_config = meta_config.service;

    if !service_config.raft_config.raft_dir.starts_with("/") {
        return Err(anyhow!(
            "raft_dir of meta service must be an absolute path, but got: {:?}",
            service_config.raft_config.raft_dir
        ));
    }

    debug!("databend meta storage loaded: {:?}", service_config);
    Ok(service_config)
}

/// Init databend query instance so that we can read meta and check license
/// for it.
///
/// FIXME: I really don't like this pattern, but it's how databend work.
pub fn init_query(cfg: &InnerConfig) -> Result<()> {
    GlobalInstance::init_production();

    GlobalConfig::init(cfg, &BUILD_INFO)?;
    GlobalIORuntime::init(cfg.storage.num_cpus as usize)?;

    Ok(())
}

/// Verify databend query instance's license so that we can read meta and
/// check license for it.
///
/// We only need to call it while backup since we can't access metasrv while
/// restoring.
pub async fn verify_query_license(cfg: &InnerConfig, version: BuildInfoRef) -> Result<()> {
    RealLicenseManager::init(cfg.query.tenant_id.tenant_name().to_string())?;
    SessionManager::init(cfg)?;
    UserApiProvider::init(
        cfg.meta.to_meta_grpc_client_conf(),
        &CacheConfig::default(),
        BuiltIn::default(),
        &cfg.query.tenant_id,
        cfg.query.tenant_quota.clone(),
    )
    .await?;

    let session_manager = SessionManager::create(cfg);
    let session = session_manager.create_session(SessionType::Dummy).await?;
    let session = session_manager.register_session(session)?;
    let settings = session.get_settings();

    LicenseManagerSwitch::instance().check_enterprise_enabled(
        settings.get_enterprise_license(version),
        Feature::SystemManagement,
    )?;

    debug!("databend license check passed");
    Ok(())
}

/// Load the databend meta service client
///
/// This will load databend meta as a stream of bytes.
///
/// Its internal format looks like
///
/// ```text
/// {"xx": "yy"}\n
/// {"xx": "bb"}\n
/// ```
pub async fn load_databend_meta() -> Result<(
    Arc<ClientHandle<DatabendRuntime>>,
    impl TryStream<Ok = Bytes, Error = anyhow::Error>,
)> {
    let cfg = GlobalConfig::instance();
    let grpc_client_conf = cfg.meta.to_meta_grpc_client_conf();
    debug!("connect meta services on {:?}", grpc_client_conf.endpoints);

    let meta_client = MetaGrpcClient::try_new(&grpc_client_conf)?;
    let mut established_client = meta_client.make_established_client().await?;

    // Convert stream from meta chunks to bytes.
    let stream = established_client
        .export_v1(ExportRequest::default())
        .await?
        .into_inner()
        .map_ok(|v| {
            debug!("load databend meta data with {} entries", v.data.len());
            let mut bs = BytesMut::with_capacity(
                v.data.len() + v.data.iter().map(|v| v.len()).sum::<usize>(),
            );
            v.data.into_iter().for_each(|b| {
                bs.extend_from_slice(b.as_bytes());
                bs.put_u8(b'\n');
            });
            bs.freeze()
        })
        .map_err(|err| anyhow!("bandsave load databend meta data failed: {err:?}"));
    Ok((meta_client, stream))
}

/// Load epochfs storage from uri.
///
/// S3: `s3://bucket/path/to/root/?region=us-east-1&access_key_id=xxx&secret_access_key=xxx`
/// Fs: `fs://path/to/data`
pub async fn load_bendsave_storage(uri: &str) -> Result<Operator> {
    let uri = http::Uri::from_str(uri)?;
    let scheme = uri.scheme_str().unwrap_or_default();
    let name = uri.host().unwrap_or_default();
    let path = uri.path();
    let mut map: HashMap<String, String> =
        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .map(|(k, v)| (k.to_string(), v.to_lowercase()))
            .collect();

    let op = match scheme {
        "s3" => {
            if name.is_empty() {
                return Err(anyhow!(
                    "bendsave requires bucket but it's empty in uri: {}",
                    uri.to_string()
                ));
            }
            map.insert("bucket".to_string(), name.to_string());
            map.insert("root".to_string(), path.to_string());
            let op = Operator::from_iter::<opendal::services::S3>(map)?.finish();
            Ok(op)
        }
        "fs" => {
            map.insert("root".to_string(), format!("/{name}/{path}"));
            let op = Operator::from_iter::<opendal::services::Fs>(map)?.finish();
            Ok(op)
        }
        _ => Err(anyhow::anyhow!("Unsupported scheme: {}", scheme)),
    }?;

    let op = op
        .layer(RetryLayer::default().with_jitter())
        .layer(LoggingLayer::default());
    debug!("epoch storage loaded: {:?}", op.info());
    Ok(op)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use opendal::Scheme;

    use super::*;

    #[tokio::test]
    async fn test_load_meta_config() -> Result<()> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = Path::new(manifest_dir).join("tests/fixtures/databend_meta_config.toml");
        let cfg = load_meta_config(&file_path.to_string_lossy())?;

        assert_eq!(cfg.raft_config.raft_dir, "/tmp/.databend/meta1");
        assert_eq!(cfg.raft_config.id, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_load_epochfs_storage() -> Result<()> {
        let op = load_bendsave_storage("s3://bendsave/tmp?region=us-east-1").await?;
        assert_eq!(op.info().scheme(), Scheme::S3);
        assert_eq!(op.info().name(), "bendsave");
        assert_eq!(op.info().root(), "/tmp/");

        let op = load_bendsave_storage("fs://opt").await?;
        assert_eq!(op.info().scheme(), Scheme::Fs);
        assert_eq!(op.info().root(), "/opt");
        Ok(())
    }
}
