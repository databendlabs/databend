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

use anyhow::anyhow;
use anyhow::Result;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_config::Config;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_types::protobuf::ExportRequest;
use databend_common_storage::init_operator;
use futures::TryStream;
use futures::TryStreamExt;
use log::debug;
use opendal::layers::LoggingLayer;
use opendal::layers::RetryLayer;
use opendal::Operator;

/// Load the configuration file and return the operator for databend.
///
/// The given input is the path to databend's configuration file.
pub fn load_query_storage(path: &str) -> Result<Operator> {
    GlobalInstance::init_production();

    let outer_config: Config = Config::load_with_config_file(path)?;
    let inner_config: InnerConfig = outer_config.try_into()?;

    // FIXME: I really don't like this pattern, but it's how databend work.
    GlobalConfig::init(&inner_config)?;
    GlobalIORuntime::init(inner_config.storage.num_cpus as usize)?;

    let op = init_operator(&inner_config.storage.params)?;
    debug!("databend storage loaded: {:?}", op.info());
    Ok(op)
}

/// Load the configuration file of databend meta.
///
/// The given input is the path to databend meta's configuration file.
pub fn load_meta_config(path: &str) -> Result<databend_meta::configs::Config> {
    let content = std::fs::read_to_string(path)?;
    let outer_config: databend_meta::configs::outer_v0::Config = toml::from_str(&content)?;
    let inner_config: databend_meta::configs::Config = outer_config.into();

    if !inner_config.raft_config.raft_dir.starts_with("/") {
        return Err(anyhow!(
            "raft_dir of meta service must be an absolute path, but got: {:?}",
            inner_config.raft_config.raft_dir
        ));
    }

    debug!("databend meta storage loaded: {:?}", inner_config);
    Ok(inner_config)
}

/// Load the databend meta service client
///
/// This will load databend meta as a stream of bytes.
///
/// It's internal format looks like
///
/// ```text
/// {"xx": "yy"}\n
/// {"xx": "bb"}\n
/// ```
pub async fn load_databend_meta() -> Result<(
    Arc<ClientHandle>,
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
pub async fn load_epochfs_storage(uri: &str) -> Result<Operator> {
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

    use databend_common_base::base::tokio;
    use opendal::Scheme;

    use super::*;

    #[tokio::test]
    async fn test_load_query_storage() -> Result<()> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = Path::new(manifest_dir).join("tests/fixtures/databend_query_config.toml");
        let op = load_query_storage(&file_path.to_string_lossy())?;

        assert_eq!(op.info().scheme(), Scheme::Fs);
        assert_eq!(op.info().root(), "/tmp/bendsave");
        Ok(())
    }

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
        let op = load_epochfs_storage("s3://bendsave/tmp?region=us-east-1").await?;
        assert_eq!(op.info().scheme(), Scheme::S3);
        assert_eq!(op.info().name(), "bendsave");
        assert_eq!(op.info().root(), "/tmp/");

        let op = load_epochfs_storage("fs://tmp").await?;
        assert_eq!(op.info().scheme(), Scheme::Fs);
        assert_eq!(op.info().root(), "/tmp");
        Ok(())
    }
}
